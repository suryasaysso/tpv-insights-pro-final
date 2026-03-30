"""
scripts/test_queries.py
───────────────────────
CLI test runner for all 15 business questions from the Payments Performance Dashboard.
Run from project root: python scripts/test_queries.py

Outputs a pass/fail report and saves results to scripts/test_results.txt
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agent import TPVAgent
from nl_to_sql import check_groq_running

BUSINESS_QUESTIONS = [
    # ── Can the model find and retrieve the right data? ──────────────────
    (1,  "How did our total ecosystem payment volume perform this past week compared "
         "to the same week last month?"),
    (2,  "What was our total active customer base last month, and how does that compare "
         "to the same month in previous years?"),
    (3,  "Which customer segment had the worst churn rate last month, and how does it "
         "compare to the same period last year?"),
    (4,  "Were there any weeks in the last quarter where we saw a noticeable spike in "
         "customers lapsing? Who was most affected?"),
    (5,  "On average, how much are our card-only customers paying per invoice compared "
         "to customers who use a mix of payment methods?"),

    # ── Can the model analyze, compare, and spot patterns? ───────────────
    (6,  "Looking at weekly data over the last quarter, which segments are driving new "
         "customer additions — and is the mix shifting toward higher-value or lower-value customers?"),
    (7,  "Month over month, which segment is growing its connected payment volume the "
         "fastest — and is that growth coming from recurring invoices or one-time payments?"),
    (8,  "How has the average invoice size evolved over the last 12 months? Are our "
         "merchants moving upmarket, or are smaller merchants starting to dominate the mix?"),
    (9,  "On a weekly basis, are we winning back lapsed customers fast enough to offset "
         "the ones we're losing? How does the resurrection-to-lapse ratio look across segments?"),
    (10, "Are our high-value customers churning at a different rate than non-high-value "
         "ones? Has that gap been widening or narrowing over the last six months?"),

    # ── Can the model derive insights and recommend actions? ─────────────
    (11, "Break down our churn over FY24 through FY26 into active vs passive components "
         "on a monthly basis. Is one type improving while the other is getting worse?"),
    (12, "Across our tenure-based cohorts, are the new customers we acquired in FY25 "
         "ramping their payment volume faster or slower than FY24 cohorts at the same stage?"),
    (13, "If you built a health scorecard for each segment today — factoring in volume "
         "per active user, repeat usage rates, invoice sizes, and churn — which segments "
         "are thriving and which ones need immediate attention?"),
    (14, "Looking at weekly activity patterns across our payment-type segments, which "
         "payment mix shows the earliest warning signs before a customer disengages?"),
    (15, "Looking at our full segment portfolio on a monthly basis, where does each "
         "segment sit on a growth-vs-retention matrix?"),
]


def run_tests(model: str = "gpt-oss-120b"):
    """Run all 15 business questions and print a pass/fail report."""
    if not check_groq_running():
        print("❌ Groq API unreachable. Check your GROQ_API_KEY and network connection.")
        sys.exit(1)

    print(f"\n{'='*70}")
    print(f"  TPV Insight Pro — Business Question Test Suite")
    print(f"  Model: {model}")
    print(f"{'='*70}\n")

    agent = TPVAgent(model=model)
    results = []

    for num, question in BUSINESS_QUESTIONS:
        print(f"Q{num:02d}: {question[:80]}{'...' if len(question) > 80 else ''}")
        start = time.time()

        resp = agent.run(question)
        elapsed = time.time() - start

        status = "✅ PASS" if resp.success else "❌ FAIL"
        sql_status = "SQL✓" if resp.sql else "NO SQL"
        rows = len(resp.results_df) if resp.results_df is not None else 0

        print(f"     {status} | {sql_status} | {rows} rows | {elapsed:.1f}s")

        if resp.error:
            print(f"     ERROR: {resp.error[:100]}")

        if resp.sql:
            print(f"     SQL preview: {resp.sql[:100].strip()}...")

        print()

        results.append({
            "question_num": num,
            "question": question,
            "success": resp.success,
            "has_sql": bool(resp.sql),
            "rows_returned": rows,
            "elapsed_s": round(elapsed, 2),
            "error": resp.error or "",
            "sql": resp.sql or "",
            "interpretation_preview": (resp.interpretation or "")[:200],
        })

    # Summary
    passed = sum(1 for r in results if r["success"])
    total  = len(results)
    avg_t  = sum(r["elapsed_s"] for r in results) / total

    print(f"\n{'='*70}")
    print(f"  Results: {passed}/{total} passed | Avg time: {avg_t:.1f}s per question")
    print(f"{'='*70}\n")

    # Write detailed results
    output_path = Path(__file__).parent / "test_results.txt"
    with open(output_path, "w") as f:
        f.write(f"TPV Insight Pro — Test Results\nModel: {model}\n{'='*70}\n\n")
        for r in results:
            f.write(f"Q{r['question_num']:02d}: {'PASS' if r['success'] else 'FAIL'}\n")
            f.write(f"Question: {r['question']}\n")
            f.write(f"SQL:\n{r['sql']}\n")
            f.write(f"Rows: {r['rows_returned']} | Time: {r['elapsed_s']}s\n")
            f.write(f"Interpretation: {r['interpretation_preview']}\n")
            if r["error"]:
                f.write(f"Error: {r['error']}\n")
            f.write(f"\n{'-'*70}\n\n")

    print(f"Full results saved to: {output_path}")
    return results


if __name__ == "__main__":
    model = sys.argv[1] if len(sys.argv) > 1 else "gpt-oss-120b"
    run_tests(model=model)
