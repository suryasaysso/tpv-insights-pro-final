"""
scripts/eval.py
───────────────
Automated evaluation runner for TPV Insight Pro.
Scores each of the 15 business questions across 4 dimensions.

Run from project root:
    python scripts/eval.py
    python scripts/eval.py gpt-oss-120b   # specify model
"""

# ── Path setup — MUST be first, before any local imports ─────────────────────
import os, sys
_SCRIPTS = os.path.dirname(os.path.abspath(__file__))   # .../scripts/
_SRC     = os.path.join(_SCRIPTS, "..", "src")          # .../src/
sys.path.insert(0, _SCRIPTS)   # ground_truth.py lives here
sys.path.insert(0, _SRC)       # agent, db, knowledge_base live here
# ─────────────────────────────────────────────────────────────────────────────

import re
import json
import time
from pathlib import Path

from agent import TPVAgent
from ground_truth import GROUND_TRUTH

# ── Governance red-flag patterns ──────────────────────────────────────────────
GOVERNANCE_RED_FLAGS = [
    (
        r'PARTITION\s+BY\s+"Segment Name"(?!\s*,\s*Segment)',
        "Granularity Trap: PARTITION BY 'Segment Name' without Segment → phantom growth rates",
    ),
    (
        r'(?i)SUM\s*\(\s*"?Eco Actives',
        "Double-count risk: SUM(Eco Actives) without Aggregate filter",
    ),
    (
        r'"Eco (?:Non-)?Recurring Invoice Amount[^"]*"\s*/\s*"Eco CP Amount',
        "Wrong parent: Invoice sub-metric divided by CP Amount → impossible >100% ratio",
    ),
    (
        r'(?i)AVG\s*\(\s*"?Avg (?:Paid )?Invoice',
        "Unweighted average of averages: use SUM(avg*actives)/SUM(actives) instead",
    ),
]

# ── Scoring helpers ───────────────────────────────────────────────────────────

def score_sql_correctness(resp) -> tuple[float, str]:
    """Did the SQL execute? Were retries needed?"""
    if not resp.success or resp.error:
        return 0.0, f"FAIL — {str(resp.error)[:100]}"
    if resp.retries_used > 0:
        return 0.5, f"PASS after {resp.retries_used} auto-repair(s)"
    return 1.0, "PASS — clean first attempt"


def score_governance(sql: str) -> tuple[float, list[str]]:
    """Check SQL against known governance anti-patterns."""
    if not sql:
        return 0.0, ["No SQL generated"]
    violations = []
    for pattern, label in GOVERNANCE_RED_FLAGS:
        if re.search(pattern, sql, re.IGNORECASE | re.DOTALL):
            violations.append(label)
    if not violations:
        return 1.0, []
    if len(violations) == 1:
        return 0.5, violations
    return 0.0, violations


def score_numerical(resp, gt: dict) -> tuple[float | None, str]:
    """Compare first numeric result to ground truth within tolerance."""
    verified = gt.get("verified_answer")
    if verified is None:
        return None, "No numeric ground truth — manual review required"
    if resp.results_df is None or resp.results_df.empty:
        return 0.0, "No results returned"

    tolerance = gt.get("tolerance_pct", 5) / 100
    numeric_cols = resp.results_df.select_dtypes("number").columns.tolist()
    if not numeric_cols:
        return 0.0, "No numeric columns in result"

    actual = float(resp.results_df[numeric_cols[0]].iloc[0])

    if isinstance(verified, (int, float)):
        diff_pct = abs(actual - float(verified)) / max(abs(float(verified)), 1)
        if diff_pct <= tolerance:
            return 1.0, f"✓ {actual:,.0f} ≈ {verified:,.0f}  (Δ {diff_pct*100:.1f}%)"
        if diff_pct < 0.50:
            return 0.5, f"~ {actual:,.0f} vs {verified:,.0f}  (Δ {diff_pct*100:.1f}%)"
        return 0.0, f"✗ {actual:,.0f} vs {verified:,.0f}  (Δ {diff_pct*100:.1f}%)"

    if isinstance(verified, dict):
        # Multi-value ground truth — check if result df contains expected values
        hits = 0
        notes = []
        for seg, expected in verified.items():
            match_rows = resp.results_df[
                resp.results_df.apply(lambda r: seg in str(r.values), axis=1)
            ]
            if not match_rows.empty:
                num_vals = match_rows.select_dtypes("number")
                if not num_vals.empty:
                    closest = float(num_vals.iloc[0, 0])
                    diff = abs(closest - expected) / max(abs(expected), 1)
                    if diff <= tolerance:
                        hits += 1
                        notes.append(f"✓ {seg}: {closest:.2f} ≈ {expected:.2f}")
                    else:
                        notes.append(f"✗ {seg}: {closest:.2f} vs {expected:.2f} (Δ {diff*100:.1f}%)")
        score = hits / len(verified)
        return round(score, 2), " | ".join(notes)

    return None, "Complex ground truth — manual review"


def score_interpretation(resp) -> tuple[float, str]:
    """Auto-flag obvious hallucination patterns in the narrative."""
    if not resp.interpretation:
        return 0.0, "No interpretation generated"

    text = resp.interpretation

    # Phantom growth rate — only flag rates in the millions (e.g. "65,574,525%")
    # Normal business growth rates of 100-999% are valid
    if re.search(r'\d{1,3}(?:,\d{3}){2,}%', text):
        return 0.0, "Phantom growth rate (millions%) in narrative — granularity trap artifact"

    # Impossible SHARE metrics > 100% — but NOT churn rates (which can legitimately exceed 100%)
    # Only flag when the context word suggests it should be a share/ratio (not a rate/change)
    share_contexts = re.findall(
        r'(\d+(?:\.\d+)?)\s*%.*?(?:share|ratio|mix|portion|proportion)',
        text, re.IGNORECASE
    )
    impossible_shares = [s for s in share_contexts if float(s) > 100]
    if impossible_shares:
        return 0.0, f"Impossible share > 100% in narrative: {impossible_shares}"

    # Suspiciously large active count (double-count signal) — only integers, skip dollar amounts
    large_nums = re.findall(r'\b(\d[\d,]+)\b', text)
    for n in large_nums:
        clean = n.replace(",", "")
        if len(clean) <= 10:
            try:
                val = int(clean)
                if 800_000 < val < 100_000_000:   # plausible double-count range
                    return 0.5, f"Suspiciously large number ({val:,}) — possible double-count"
            except ValueError:
                pass

    return 1.0, "OK"


def _run_with_rate_limit_retry(agent, question: str, max_retries: int = 3) -> tuple:
    """
    Run agent with exponential backoff on 429 rate limit errors.
    Returns (resp, elapsed_seconds).
    """
    import time as _time
    delay = 30   # start with 30s wait — Groq free tier resets quickly

    for attempt in range(max_retries):
        t0   = _time.time()
        resp = agent.run(question)
        elapsed = _time.time() - t0

        # Not a rate limit error — return immediately
        if not resp.error or "429" not in str(resp.error):
            return resp, elapsed

        # Rate limited — wait and retry
        if attempt < max_retries - 1:
            wait = delay * (2 ** attempt)   # 30s, 60s, 120s
            print(f"  ⏳ Rate limited. Waiting {wait}s before retry {attempt+2}/{max_retries}...")
            _time.sleep(wait)

    return resp, elapsed   # return last attempt even if still failing


# ── Main eval loop ────────────────────────────────────────────────────────────

def run_eval(model: str = "llama-3.3-70b-versatile"):
    agent   = TPVAgent(model=model)
    results = []

    WEIGHTS_WITH_GT    = {"sql": 0.25, "numerical": 0.35, "governance": 0.25, "interpretation": 0.15}
    WEIGHTS_WITHOUT_GT = {"sql": 0.35, "governance": 0.35, "interpretation": 0.30}

    print(f"\n{'='*72}")
    print(f"  TPV Insight Pro — Evaluation Suite")
    print(f"  Model : {model}")
    print(f"  Questions: {len(GROUND_TRUTH)}")
    print(f"  Note: 8s delay between questions to avoid rate limits")
    print(f"  Estimated runtime: ~{len(GROUND_TRUTH) * 45 // 60}–{len(GROUND_TRUTH) * 60 // 60} minutes")
    print(f"{'='*72}\n")

    for i, gt in enumerate(GROUND_TRUTH):
        qid = gt["id"]
        q   = gt["question"]
        print(f"▶ {qid} ({i+1}/{len(GROUND_TRUTH)}): {q[:65]}{'...' if len(q) > 65 else ''}")

        # Rate limit guard — pause between questions
        if i > 0:
            time.sleep(8)

        resp, elapsed = _run_with_rate_limit_retry(agent, q)

        sql_score,  sql_note  = score_sql_correctness(resp)
        gov_score,  gov_notes = score_governance(resp.sql or "")
        num_score,  num_note  = score_numerical(resp, gt)
        itp_score,  itp_note  = score_interpretation(resp)

        has_gt = num_score is not None
        w      = WEIGHTS_WITH_GT if has_gt else WEIGHTS_WITHOUT_GT

        if has_gt:
            total = (sql_score * w["sql"] + num_score * w["numerical"] +
                     gov_score * w["governance"] + itp_score * w["interpretation"])
        else:
            total = (sql_score * w["sql"] + gov_score * w["governance"] +
                     itp_score * w["interpretation"])

        grade = "A" if total >= 0.85 else "B" if total >= 0.70 else "C" if total >= 0.55 else "F"

        print(f"  [{grade}] {total:.0%}  SQL:{sql_score}  GOV:{gov_score}  "
              f"NUM:{num_score if num_score is not None else '—'}  "
              f"INTERP:{itp_score}  ({elapsed:.1f}s)")
        print(f"  SQL  : {sql_note}")
        print(f"  NUM  : {num_note}")
        if gov_notes:
            for v in gov_notes:
                print(f"  ⚠ GOV: {v}")
        if itp_note != "OK":
            print(f"  ⚠ ITP: {itp_note}")
        print()

        results.append({
            "id":             qid,
            "question":       q,
            "grade":          grade,
            "total_score":    round(total, 3),
            "sql_score":      sql_score,
            "gov_score":      gov_score,
            "num_score":      num_score,
            "itp_score":      itp_score,
            "sql_note":       sql_note,
            "num_note":       num_note,
            "gov_violations": gov_notes,
            "itp_note":       itp_note,
            "elapsed_s":      round(elapsed, 1),
            "sql_generated":  (resp.sql or "")[:500],
            "retries_used":   resp.retries_used,
        })

    if not results:
        print("⚠ No questions in GROUND_TRUTH. Populate scripts/ground_truth.py first.")
        return []

    # ── Summary ───────────────────────────────────────────────────────────────
    avg   = sum(r["total_score"] for r in results) / len(results)
    by_grade = {g: sum(1 for r in results if r["grade"] == g) for g in "ABCF"}
    gov_fails = sum(1 for r in results if r["gov_violations"])

    print(f"\n{'='*72}")
    print(f"  FINAL SCORE : {avg:.1%}  ({avg*10:.1f} / 10)")
    print(f"  Grades      : A={by_grade['A']}  B={by_grade['B']}  C={by_grade['C']}  F={by_grade['F']}")
    print(f"  Gov failures: {gov_fails} / {len(results)}")
    print(f"{'='*72}\n")

    # Save JSON report
    out_path = Path(_SCRIPTS) / "eval_results.json"
    out_path.write_text(json.dumps(results, indent=2, default=str))
    print(f"Full results saved → {out_path}\n")

    return results


if __name__ == "__main__":
    model = sys.argv[1] if len(sys.argv) > 1 else "llama-3.3-70b-versatile"
    run_eval(model=model)
