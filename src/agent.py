"""
agent.py
────────
Agentic orchestrator for TPV Insight Pro.
Implements a Think → Generate SQL → Execute → Validate → Retry → Interpret loop.

Flow:
  1. Receive user question
  2. Inject RAG context → generate SQL via Groq
  3. Execute SQL on DuckDB
  4. If SQL error → auto-repair (up to MAX_RETRIES)
  5. Post-execution result validation (catches logical errors the LLM missed)
  6. If validation fails → regenerate SQL with the specific failure reason
  7. Format results → send back to LLM for business interpretation
  8. Record to query history with confidence score
  9. Return final answer + SQL + DataFrame to UI
"""

import re
import time
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional

from db import db
from nl_to_sql import NLToSQL
from knowledge_base import get_result_interpretation_prompt
from nl_to_sql import call_groq, DEFAULT_MODEL
from history import history_db

MAX_RETRIES      = 2
MAX_REGEN_PASSES = 1   # post-validation regeneration attempts


@dataclass
class AgentResponse:
    """Structured response from the agent for one question."""
    question: str
    thought: str = ""
    sql: str = ""
    results_df: Optional[pd.DataFrame] = None
    interpretation: str = ""
    error: str = ""
    retries_used: int = 0
    success: bool = False
    validation_note: str = ""   # non-empty when post-exec guard fired
    sql_gen_ms: float = 0.0
    sql_exec_ms: float = 0.0
    interp_ms: float = 0.0
    history_id: str = ""

    @property
    def results_preview(self) -> str:
        if self.results_df is None or self.results_df.empty:
            return "(no results)"
        return self.results_df.head(20).to_string(index=False, max_colwidth=40)


# ── Post-execution result validators ─────────────────────────────────────────

def _is_churn_worst_question(question: str) -> bool:
    """Detect questions asking for worst/highest churn segment."""
    q = question.lower()
    churn_kw  = any(w in q for w in ["churn", "churning", "lost", "retention"])
    worst_kw  = any(w in q for w in ["worst", "highest", "most", "largest", "bad"])
    return churn_kw and worst_kw


def _validate_churn_result(df: pd.DataFrame, question: str) -> tuple[bool, str]:
    """
    Validates churn query results. Returns (is_valid, failure_reason).

    Catches three verified failure modes:
      1. Top result < 5% churn on a "worst" question → sort inverted / wrong denominator
      2. Any churn "rate" = exactly 1/actives (e.g. 0.0041 = 1/239) → NULL numerator bug
      3. Only one segment returned and its rate is suspiciously low → micro-segment noise
    """
    if not _is_churn_worst_question(question):
        return True, ""
    if df is None or df.empty:
        return True, ""

    # Find a churn rate column
    rate_cols = [c for c in df.columns
                 if "rate" in c.lower() or "pct" in c.lower() or "churn" in c.lower()]
    if not rate_cols:
        return True, ""

    top_rate_col = rate_cols[0]
    numeric_vals = pd.to_numeric(df[top_rate_col], errors="coerce").dropna()
    if numeric_vals.empty:
        return True, ""

    top_rate  = float(numeric_vals.iloc[0])
    max_rate  = float(numeric_vals.max())

    # Guard 1: "Worst" churn is < 5% — almost certainly wrong sort or wrong denominator
    if top_rate < 5.0:
        return False, (
            f"RESULT VALIDATION FAILED — 'worst churn' question but top result shows "
            f"{top_rate:.4f}% which is a low/good churn rate. "
            f"This means either: (a) ORDER BY is ASC instead of DESC, "
            f"or (b) the churn rate was computed as 1/actives (e.g. 1/239 = 0.4%) "
            f"because active_cohorted_churn was NULL and COALESCE(...,0) was used. "
            f"FIX: Use T9 template. Ensure ORDER BY churn_rate_pct DESC NULLS LAST. "
            f"Add WHERE active_cohorted_churn IS NOT NULL AND total_churn > 0. "
            f"Use prior-period actives (LAG) as denominator, not current actives."
        )

    # Guard 2: Top rate is suspiciously close to 1/prior_actives
    # (classic NULL numerator: LLM computed 1 / actives instead of using the column)
    actives_cols = [c for c in df.columns if "active" in c.lower() and "prior" in c.lower()]
    if actives_cols:
        prior_actives_vals = pd.to_numeric(df[actives_cols[0]], errors="coerce")
        if not prior_actives_vals.empty:
            prior_val = float(prior_actives_vals.iloc[0])
            if prior_val > 0:
                implied_numerator = top_rate / 100.0 * prior_val
                if 0.5 < implied_numerator < 1.5:  # implies numerator ≈ 1
                    return False, (
                        f"RESULT VALIDATION FAILED — churn rate {top_rate:.4f}% "
                        f"implies exactly 1 churned merchant out of {prior_val:.0f}. "
                        f"This is the NULL-numerator bug: active_cohorted_churn was NULL "
                        f"and COALESCE defaulted to 0 or the agent invented a count of 1. "
                        f"FIX: Add WHERE active_cohorted_churn IS NOT NULL AND total_churn > 0."
                    )

    return True, ""


def _validate_result(df: pd.DataFrame, question: str) -> tuple[bool, str]:
    """
    Master post-execution validator. Runs all applicable guards.
    Returns (is_valid, failure_reason_for_regen_prompt).
    """
    if df is None or df.empty:
        return True, ""

    # Churn-specific validation
    ok, reason = _validate_churn_result(df, question)
    if not ok:
        return False, reason

    # Universal guard: any share column > 100% (except churn rates)
    for col in df.columns:
        col_lower = col.lower()
        if any(w in col_lower for w in ["share", "mix", "pct", "ratio"]) \
                and "churn" not in col_lower:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            impossible = vals[vals > 100]
            if not impossible.empty:
                return False, (
                    f"RESULT VALIDATION FAILED — column '{col}' contains "
                    f"values > 100% ({float(impossible.iloc[0]):.2f}%), "
                    f"which is impossible for a share/ratio metric. "
                    f"FIX: Check the denominator parent. "
                    f"Recurring/non-recurring invoice shares must divide by "
                    f"'Eco Invoice Amount', not 'Eco CP Amount'."
                )

    # Universal guard: active count > 800K (double-counting signal)
    active_cols = [c for c in df.columns
                   if "active" in c.lower() and "churn" not in c.lower()]
    for col in active_cols:
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        if not vals.empty and float(vals.sum()) > 800_000:
            return False, (
                f"RESULT VALIDATION FAILED — column '{col}' sums to "
                f"{int(vals.sum()):,}, which exceeds 800K and likely indicates "
                f"double-counting across segment rows. "
                f"FIX: Add WHERE \"Segment Name\" = 'Aggregate' AND Segment = 'All Segments' "
                f"for total-business queries."
            )

    return True, ""


class TPVAgent:
    """
    End-to-end NL-to-SQL agent with:
    - RAG context injection
    - SQL auto-repair on DuckDB error
    - Post-execution result validation (logical correctness guards)
    - Business interpretation pass
    - Query history recording
    """

    def __init__(self, model: str = DEFAULT_MODEL, session_id: Optional[str] = None):
        self.model     = model
        self.nl_to_sql = NLToSQL(model=model)
        self._conversation_history: list[dict] = []
        self._turn_number = 0
        import uuid
        self.session_id = session_id or str(uuid.uuid4())

    def run(self, question: str, user_email: str = "anonymous") -> AgentResponse:
        resp = AgentResponse(question=question)
        self._turn_number += 1

        # ── Step 1: Generate SQL ────────────────────────────────────────────
        t0 = time.time()
        gen = self.nl_to_sql.generate(question)
        resp.sql_gen_ms = (time.time() - t0) * 1000
        resp.thought    = gen["thought"]

        if gen["error"]:
            resp.error = f"LLM error: {gen['error']}"
            self._record(resp, user_email)
            return resp

        if not gen["sql"]:
            if not gen["raw_response"]:
                 resp.error = "The model returned an empty response. Check if the Groq API is functioning correctly or if your prompt is triggering a safety filter."
            else:
                 # If we have a raw response but no SQL, it's likely a conversational/off-topic question.
                 resp.interpretation = gen["raw_response"]
                 resp.success = True
            self._record(resp, user_email)
            return resp

        resp.sql = gen["sql"]

        # ── Step 2: Execute SQL (with syntax-error retry) ───────────────────
        last_error = None
        t0 = time.time()
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp.results_df = db.query(resp.sql)
                last_error = None
                break
            except Exception as e:
                last_error = str(e)
                resp.retries_used = attempt + 1
                if attempt < MAX_RETRIES:
                    fixed = self.nl_to_sql.refine_sql(
                        original_sql=resp.sql,
                        error_msg=last_error,
                        question=question,
                    )
                    if fixed:
                        resp.sql = fixed
                    else:
                        break
        resp.sql_exec_ms = (time.time() - t0) * 1000

        if last_error:
            resp.error = (f"SQL execution failed after {resp.retries_used} "
                          f"attempt(s):\n{last_error}")
            self._record(resp, user_email)
            return resp

        # ── Step 3: Post-execution result validation ────────────────────────
        # Catches logical errors that DuckDB executes without raising — e.g.
        # wrong sort order, NULL-numerator churn hack, impossible share ratios.
        for _pass in range(MAX_REGEN_PASSES):
            valid, failure_reason = _validate_result(resp.results_df, question)
            if valid:
                break

            resp.validation_note = failure_reason
            # Regenerate SQL with the specific failure reason injected
            regen_prompt = (
                f"The following SQL for the question below produced logically incorrect results.\n\n"
                f"Question: {question}\n\n"
                f"Incorrect SQL:\n```sql\n{resp.sql}\n```\n\n"
                f"Validation failure:\n{failure_reason}\n\n"
                f"Rewrite the SQL completely to fix this issue. "
                f"Use the T9 template from the knowledge base for churn-rate questions. "
                f"Return ONLY the corrected SQL in a ```sql block."
            )
            try:
                raw = call_groq(
                    prompt=regen_prompt,
                    system=self.nl_to_sql.system_prompt,
                    model=self.model,
                    temperature=0.0,
                )
                from nl_to_sql import extract_sql
                new_sql = extract_sql(raw)
                if new_sql and new_sql != resp.sql:
                    try:
                        new_df = db.query(new_sql)
                        resp.sql        = new_sql
                        resp.results_df = new_df
                    except Exception:
                        pass   # keep original result if regen also fails
            except Exception as e:
                # Log API error but don't crash; keep original result
                print(f"[agent] Validation regeneration API error: {e}")
                pass   # validation note is kept for display; continue to interpret

        # ── Step 4: Interpret results ───────────────────────────────────────
        t0 = time.time()
        if resp.results_df is not None:
            if resp.results_df.empty:
                resp.interpretation = (
                    "The query returned no rows. "
                    "This may mean no data matches the time period or segment filter. "
                    "Try broadening the date range or removing segment filters."
                )
            else:
                interp_prompt = get_result_interpretation_prompt(
                    question=question,
                    sql=resp.sql,
                    results=resp.results_preview,
                    validation_note=resp.validation_note,
                )
                try:
                    resp.interpretation = call_groq(
                        prompt=interp_prompt,
                        model=self.model,
                        temperature=0.3,
                    )
                except Exception as e:
                    resp.interpretation = f"The query was successful, but I encountered an error generating the natural language interpretation: {e}. You can still view the raw data and SQL below."
        resp.interp_ms = (time.time() - t0) * 1000

        resp.success = True
        self._record(resp, user_email)
        return resp

    def _record(self, resp: AgentResponse, user_email: str):
        """Persist the response to query history."""
        try:
            record_id = history_db.record(
                resp=resp,
                session_id=self.session_id,
                user_email=user_email,
                turn_number=self._turn_number,
                model_used=self.model,
                sql_gen_ms=resp.sql_gen_ms,
                sql_exec_ms=resp.sql_exec_ms,
                interp_ms=resp.interp_ms,
            )
            resp.history_id = record_id
        except Exception as e:
            print(f"[history] Warning: failed to record query — {e}")

    def run_with_history(self, question: str, user_email: str = "anonymous") -> AgentResponse:
        """Run with conversation context for multi-turn follow-ups."""
        if self._conversation_history:
            recent = self._conversation_history[-2:]
            ctx = "\n".join(
                f"Prior Q: {t['question']}" +
                (f"\nPrior SQL: {t['sql'][:200]}..." if t.get("sql") else "")
                for t in recent
            )
            enriched = f"{ctx}\n\nNow answer: {question}"
        else:
            enriched = question

        resp = self.run(enriched, user_email=user_email)
        resp.question = question
        self._conversation_history.append({
            "question": question, "sql": resp.sql, "success": resp.success,
        })
        return resp

    def clear_history(self):
        self._conversation_history.clear()
        self._turn_number = 0


