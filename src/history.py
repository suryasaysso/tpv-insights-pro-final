"""
history.py
──────────
Query history tracker for TPV Insight Pro.
Persists every query to a local SQLite database with:
  - Confidence scoring (composite of 4 signals from AgentResponse)
  - Full SQL + results metadata
  - Governance flag tracking
  - Latency breakdown
  - User feedback capture

Database: data/query_history.db  (auto-created on first run)
"""

import re
import json
import uuid
import sqlite3
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "query_history.db"

# ── Confidence scoring ────────────────────────────────────────────────────────

GOVERNANCE_PATTERNS = [
    (r'PARTITION\s+BY\s+"Segment Name"(?!\s*,\s*Segment)',
     "granularity_trap"),
    (r'(?i)SUM\s*\(\s*"?Eco Actives',
     "possible_double_count"),
    (r'"Eco (?:Non-)?Recurring Invoice Amount[^"]*"\s*/\s*"Eco CP Amount',
     "wrong_parent_denominator"),
    (r'(?i)AVG\s*\(\s*"?Avg (?:Paid )?Invoice',
     "unweighted_avg_of_avgs"),
]


def _detect_governance_flags(sql: str) -> list[str]:
    flags = []
    for pattern, label in GOVERNANCE_PATTERNS:
        if re.search(pattern, sql, re.IGNORECASE | re.DOTALL):
            flags.append(label)
    return flags


def _detect_narrative_flags(text: str) -> list[str]:
    flags = []
    if re.search(r'\d{1,3}(?:,\d{3}){2,}%', text):
        flags.append("phantom_growth_rate")
    share_hits = re.findall(
        r'(\d+(?:\.\d+)?)\s*%.*?(?:share|ratio|mix|portion|proportion)',
        text, re.IGNORECASE
    )
    if any(float(s) > 100 for s in share_hits):
        flags.append("impossible_share_ratio")
    large = re.findall(r'\b(\d[\d,]+)\b', text)
    for n in large:
        try:
            if 800_000 < int(n.replace(",", "")) < 100_000_000:
                flags.append("possible_double_count_in_narrative")
                break
        except ValueError:
            pass
    return flags


def _sql_complexity(sql: str) -> str:
    """Rough complexity bucket based on SQL structure."""
    if not sql:
        return "none"
    score = 0
    score += len(re.findall(r'\bWITH\b', sql, re.I))        # CTEs
    score += len(re.findall(r'\bJOIN\b', sql, re.I))         # joins
    score += len(re.findall(r'\bOVER\s*\(', sql, re.I)) * 2  # window fns
    score += len(re.findall(r'\bCASE\b', sql, re.I))
    if score == 0:
        return "simple"
    if score <= 3:
        return "moderate"
    return "complex"


def _question_category(question: str) -> str:
    """Infer question type for grouping in analytics."""
    q = question.lower()
    if any(w in q for w in ["which", "who", "what segment", "worst", "best", "fastest"]):
        return "ranking"
    if any(w in q for w in ["trend", "over time", "evolved", "last 12", "month over month", "week over week"]):
        return "trend"
    if any(w in q for w in ["compare", "vs", "versus", "difference", "gap"]):
        return "comparison"
    if any(w in q for w in ["scorecard", "health", "matrix", "playbook", "recommend"]):
        return "insight"
    if any(w in q for w in ["how much", "total", "how many", "what was"]):
        return "retrieval"
    return "other"


@dataclass
class ConfidenceBreakdown:
    sql_success: float        # 0 or 1 — did SQL run?
    sql_clean: float          # 1=no retries, 0.5=repaired, 0=failed
    governance: float         # 1=no flags, 0.5=1 flag, 0=2+ flags
    result_quality: float     # rows returned, no all-null columns
    narrative_safety: float   # no phantom rates or impossible ratios
    composite: float          # weighted final score

    def to_dict(self) -> dict:
        return asdict(self)


def compute_confidence(
    resp,                         # AgentResponse
    sql_gen_ms: float = 0,
    sql_exec_ms: float = 0,
) -> ConfidenceBreakdown:
    """
    Composite confidence score from 5 observable signals.

    Weights chosen to reflect what matters most in a payments analytics context:
      SQL success    25%  — a failed query is useless
      SQL clean      20%  — retries indicate fragility
      Governance     30%  — the hardest bugs to catch (partition, parent, double-count)
      Result quality 15%  — empty results or all-NaN columns reduce trust
      Narrative safe 10%  — catches hallucinated numbers in the interpretation
    """
    # 1. SQL success
    sql_success = 1.0 if resp.success and not resp.error else 0.0

    # 2. SQL cleanliness
    if not resp.success:
        sql_clean = 0.0
    elif resp.retries_used == 0:
        sql_clean = 1.0
    elif resp.retries_used == 1:
        sql_clean = 0.6
    else:
        sql_clean = 0.3

    # 3. Governance
    gov_flags = _detect_governance_flags(resp.sql or "")
    if len(gov_flags) == 0:
        governance = 1.0
    elif len(gov_flags) == 1:
        governance = 0.5
    else:
        governance = 0.0

    # 4. Result quality
    if resp.results_df is None or resp.results_df.empty:
        result_quality = 0.3   # penalise but not zero — could be valid empty result
    else:
        df = resp.results_df
        rows = len(df)
        # penalise if >80% of numeric columns are null
        num_cols = df.select_dtypes("number").columns
        null_frac = df[num_cols].isna().mean().mean() if len(num_cols) else 0
        if null_frac > 0.8:
            result_quality = 0.4
        elif rows == 0:
            result_quality = 0.3
        else:
            result_quality = 1.0

    # 5. Narrative safety
    narr_flags = _detect_narrative_flags(resp.interpretation or "")
    narrative_safety = 1.0 if not narr_flags else 0.0

    # Weighted composite
    composite = (
        sql_success    * 0.25 +
        sql_clean      * 0.20 +
        governance     * 0.30 +
        result_quality * 0.15 +
        narrative_safety * 0.10
    )

    return ConfidenceBreakdown(
        sql_success=sql_success,
        sql_clean=sql_clean,
        governance=governance,
        result_quality=result_quality,
        narrative_safety=narrative_safety,
        composite=round(composite, 3),
    )


# ── Database setup ────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS query_history (
    -- Identity
    id                  TEXT PRIMARY KEY,        -- UUID
    session_id          TEXT NOT NULL,           -- groups a conversation session
    user_email          TEXT,                    -- links to a specific user account
    turn_number         INTEGER NOT NULL,        -- position within session (1-based)
    created_at          TEXT NOT NULL,           -- ISO 8601 UTC timestamp

    -- Question
    question            TEXT NOT NULL,           -- raw user question
    question_hash       TEXT NOT NULL,           -- SHA-256 for dedup detection
    question_category   TEXT,                    -- retrieval/trend/comparison/ranking/insight

    -- SQL
    sql_generated       TEXT,                    -- final SQL (after repairs)
    sql_complexity      TEXT,                    -- simple/moderate/complex/none
    had_repair          INTEGER NOT NULL,        -- 1 if auto-repair was triggered
    retries_used        INTEGER NOT NULL,        -- number of repair attempts
    tables_referenced   TEXT,                    -- JSON array of table names
    governance_flags    TEXT,                    -- JSON array of violated rules

    -- Results
    rows_returned       INTEGER,                 -- result row count
    cols_returned       INTEGER,                 -- result column count
    result_has_nulls    INTEGER,                 -- 1 if any nulls in result
    result_preview      TEXT,                    -- first 3 rows as JSON

    -- Confidence
    confidence_score    REAL NOT NULL,           -- composite 0.0–1.0
    conf_sql_success    REAL,                    -- signal breakdown
    conf_sql_clean      REAL,
    conf_governance     REAL,
    conf_result_quality REAL,
    conf_narrative_safe REAL,

    -- Narrative flags
    narrative_flags     TEXT,                    -- JSON array of detected issues

    -- Interpretation
    interpretation      TEXT,                    -- LLM narrative answer
    thought             TEXT,                    -- LLM analytical reasoning

    -- Timing (milliseconds)
    sql_gen_ms          REAL,
    sql_exec_ms         REAL,
    interp_ms           REAL,
    total_ms            REAL,

    -- Model
    model_used          TEXT,                    -- e.g. llama-3.3-70b-versatile
    success             INTEGER NOT NULL,        -- 1=success, 0=failure
    error_message       TEXT,                    -- error if failed

    -- User feedback (updated post-hoc via record_feedback())
    user_thumbs         INTEGER,                 -- 1=up, -1=down, NULL=no rating
    user_note           TEXT,                    -- optional free-text correction
    feedback_at         TEXT                     -- ISO 8601 timestamp of feedback
);

CREATE INDEX IF NOT EXISTS idx_session   ON query_history(session_id);
CREATE INDEX IF NOT EXISTS idx_user      ON query_history(user_email);
CREATE INDEX IF NOT EXISTS idx_created   ON query_history(created_at);
CREATE INDEX IF NOT EXISTS idx_category  ON query_history(question_category);
CREATE INDEX IF NOT EXISTS idx_confidence ON query_history(confidence_score);
"""


class QueryHistoryDB:
    """Persistent SQLite store for all TPV Insight Pro queries."""

    def __init__(self, db_path: Path = DB_PATH):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as con:
            con.executescript(CREATE_TABLE_SQL)

    def record(
        self,
        resp,                            # AgentResponse
        session_id: str,
        user_email: str,
        turn_number: int,
        model_used: str,
        sql_gen_ms: float = 0,
        sql_exec_ms: float = 0,
        interp_ms: float = 0,
    ) -> str:
        """
        Persist one AgentResponse to history.
        Returns the new record ID.
        """
        conf = compute_confidence(resp, sql_gen_ms, sql_exec_ms)
        gov_flags = _detect_governance_flags(resp.sql or "")
        narr_flags = _detect_narrative_flags(resp.interpretation or "")

        # Result metadata
        rows, cols, has_nulls, preview = None, None, None, None
        if resp.results_df is not None and not resp.results_df.empty:
            df = resp.results_df
            rows = len(df)
            cols = len(df.columns)
            has_nulls = int(df.isna().any().any())
            preview = df.head(3).to_json(orient="records", date_format="iso")

        record_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        row = {
            "id":                 record_id,
            "session_id":         session_id,
            "user_email":         user_email,
            "turn_number":        turn_number,
            "created_at":         now,
            "question":           resp.question,
            "question_hash":      hashlib.sha256(resp.question.encode()).hexdigest()[:16],
            "question_category":  _question_category(resp.question),
            "sql_generated":      resp.sql or None,
            "sql_complexity":     _sql_complexity(resp.sql or ""),
            "had_repair":         int(resp.retries_used > 0),
            "retries_used":       resp.retries_used,
            "tables_referenced":  json.dumps(["payments"]) if resp.sql else None,
            "governance_flags":   json.dumps(gov_flags),
            "rows_returned":      rows,
            "cols_returned":      cols,
            "result_has_nulls":   has_nulls,
            "result_preview":     preview,
            "confidence_score":   conf.composite,
            "conf_sql_success":   conf.sql_success,
            "conf_sql_clean":     conf.sql_clean,
            "conf_governance":    conf.governance,
            "conf_result_quality":conf.result_quality,
            "conf_narrative_safe":conf.narrative_safety,
            "narrative_flags":    json.dumps(narr_flags),
            "interpretation":     resp.interpretation or None,
            "thought":            resp.thought or None,
            "sql_gen_ms":         round(sql_gen_ms, 1),
            "sql_exec_ms":        round(sql_exec_ms, 1),
            "interp_ms":          round(interp_ms, 1),
            "total_ms":           round(sql_gen_ms + sql_exec_ms + interp_ms, 1),
            "model_used":         model_used,
            "success":            int(resp.success),
            "error_message":      resp.error or None,
            "user_thumbs":        None,
            "user_note":          None,
            "feedback_at":        None,
        }

        cols_str = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                f"INSERT INTO query_history ({cols_str}) VALUES ({placeholders})",
                list(row.values())
            )
        return record_id

    def record_feedback(self, record_id: str, thumbs: int, note: str = ""):
        """Update a record with user feedback. thumbs: 1=up, -1=down."""
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                """UPDATE query_history
                   SET user_thumbs=?, user_note=?, feedback_at=?
                   WHERE id=?""",
                (thumbs, note or None,
                 datetime.now(timezone.utc).isoformat(), record_id)
            )

    def get_history(
        self,
        session_id: Optional[str] = None,
        user_email: Optional[str] = None,
        limit: int = 100,
        min_confidence: float = 0.0,
        category: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch history with optional filters."""
        where_clauses = ["1=1"]
        params = []
        if session_id:
            where_clauses.append("session_id = ?")
            params.append(session_id)
        if user_email:
            where_clauses.append("user_email = ?")
            params.append(user_email)
        if min_confidence > 0:
            where_clauses.append("confidence_score >= ?")
            params.append(min_confidence)
        if category:
            where_clauses.append("question_category = ?")
            params.append(category)

        sql = f"""
            SELECT id, created_at, session_id, user_email, turn_number, question,
                   question_category, sql_complexity, had_repair, retries_used,
                   rows_returned, confidence_score, conf_governance,
                   narrative_flags, governance_flags,
                   model_used, success, user_thumbs, total_ms
            FROM query_history
            WHERE {' AND '.join(where_clauses)}
            ORDER BY created_at DESC
            LIMIT ?
        """
        params.append(limit)
        with sqlite3.connect(self.db_path) as con:
            return pd.read_sql_query(sql, con, params=params)

    def get_record(self, record_id: str) -> Optional[dict]:
        """Fetch a single full record by ID."""
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            row = con.execute(
                "SELECT * FROM query_history WHERE id = ?", (record_id,)
            ).fetchone()
            return dict(row) if row else None

    def summary_stats(self) -> dict:
        """Aggregate stats for the dashboard sidebar."""
        with sqlite3.connect(self.db_path) as con:
            row = con.execute("""
                SELECT
                    COUNT(*)                                AS total_queries,
                    ROUND(AVG(confidence_score) * 100, 1)  AS avg_confidence_pct,
                    SUM(success)                            AS successful,
                    SUM(had_repair)                         AS repaired,
                    SUM(CASE WHEN user_thumbs = 1  THEN 1 ELSE 0 END) AS thumbs_up,
                    SUM(CASE WHEN user_thumbs = -1 THEN 1 ELSE 0 END) AS thumbs_down,
                    ROUND(AVG(total_ms), 0)                AS avg_latency_ms,
                    COUNT(DISTINCT session_id)              AS total_sessions
                FROM query_history
            """).fetchone()
            return dict(zip(
                ["total_queries", "avg_confidence_pct", "successful", "repaired",
                 "thumbs_up", "thumbs_down", "avg_latency_ms", "total_sessions"],
                row
            )) if row else {}

    def export_csv(self, path: Optional[Path] = None) -> Path:
        """Export full history to CSV."""
        out = path or (DB_PATH.parent / "query_history_export.csv")
        df = pd.read_sql_query(
            "SELECT * FROM query_history ORDER BY created_at DESC",
            sqlite3.connect(self.db_path)
        )
        df.to_csv(out, index=False)
        return out


# Module-level singleton
history_db = QueryHistoryDB()
