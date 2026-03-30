"""
knowledge_base.py
─────────────────
TPV Driver Tree + Data Dictionary + Verified SQL Patterns injected as RAG context.

GOVERNANCE NOTE (verified 2026-03):
  The dataset has multiple segment rows per time period. Summing across all rows
  produces massive double-counting (e.g. 2.43M vs the true 594K for Feb 2026).
  The ONLY source of truth for total business metrics is:
    WHERE "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
  All corrected SQL templates below enforce this.
"""

TPV_DRIVER_TREE = """
=== TPV DRIVER TREE (Business Framework) ===

L1:  TPV = Active Merchants × TPV per Active Merchant
L2a: Active Merchants = GNA + Resurrect + Repeat
L2b: TPV per Active = Transactions per Active × Avg Transaction Size

Critical mix-shift rule: A rise in GNA share DEPRESSES TPV per Active (new merchants
transact less). A rise in Repeat share LIFTS TPV per Active (loyalists are fully integrated).
Always decompose actives into GNA / Resurrect / Repeat before drawing volume conclusions.

Derived Metrics (not columns — must be computed):
• Volume per Active    = [Volume] / NULLIF([Actives], 0)
• Txns per Active      = [Transactions] / NULLIF([Actives], 0)
• Avg Transaction Size = [Volume] / NULLIF([Transactions], 0)
• Repeat Rate          = "31d Active Repeat Absolute" / NULLIF("Eco Actives 31d Absolute", 0)
• Resurrection Rate    = "31d Active Resurrect Absolute" / NULLIF("Eco Actives 31d Absolute", 0)
• GNA Rate             = "31d Active GNA Absolute" / NULLIF("Eco Actives 31d Absolute", 0)
• Total Churn          = COALESCE(active_cohorted_churn,0) + COALESCE(passive_cohorted_churn,0)
• Churn Rate           = total_churn / NULLIF(prior_period_actives, 0) × 100
                         where prior_period_actives = LAG("Eco Actives 31d Absolute")
                                                      OVER (PARTITION BY "Segment Name", Segment
                                                            ORDER BY date_end)
                         ⚠ DENOMINATOR = PRIOR period actives, NEVER current actives.
                         ⚠ active_cohorted_churn is an ABSOLUTE COUNT (e.g. 570,000 = 570k churned).
                            It is NOT a decimal rate. It is NOT a flag. Divide it by prior actives.
                         ⚠ "Worst churn" = HIGHEST rate → ORDER BY churn_rate DESC NULLS LAST.
                            If the result shows < 1% as "worst", the sort is inverted — fix it.
• Resurrection-to-Lapse Ratio = "31d Active Resurrect Absolute" / NULLIF(total_churn, 0)
"""

DATA_DICTIONARY = """
=== DATASET SCHEMA ===

Table name: payments   (4,359 rows × 60 columns, date range Aug 2023 – Mar 2026)

DIMENSIONS:
• Segment         — customer segment label
• "Segment Name"  — parent grouping for the segment
• "Time Period"   — "Week" or "Month"  ← ALWAYS filter by one, never mix
• date_end        — period end date (YYYY-MM-DD, stored as DATE type)
• "Date End"      — duplicate of date_end (VARCHAR) — prefer date_end for date math
• FY              — fiscal year ("FY24", "FY25", "FY26")
• "Date Month"    — calendar month number (1–12)

╔══════════════════════════════════════════════════════════════╗
║  ANTI-DOUBLE-COUNT GOVERNANCE RULE  (CRITICAL — ALWAYS APPLY) ║
╠══════════════════════════════════════════════════════════════╣
║  The table contains MULTIPLE segment rows per time period.   ║
║  Summing all rows multiplies every metric by segment count.  ║
║                                                              ║
║  TOTAL BUSINESS QUERIES → always filter BOTH:               ║
║    "Segment Name" = 'Aggregate'                              ║
║    Segment        = 'All Segments'                           ║
║                                                              ║
║  SEGMENT BREAKDOWN QUERIES → filter to one Segment Name      ║
║    e.g. "Segment Name" = 'Bank Transfer & Card Distribution' ║
║    AND  Segment != 'All Segments'                            ║
║                                                              ║
║  NEVER aggregate (SUM/AVG) across all segments together.     ║
║  Verified total actives Feb 2026 = 594,045 (Aggregate row).  ║
╚══════════════════════════════════════════════════════════════╝

SEGMENT NAMES available in the data:
• 'Aggregate'                          → use for total business (Segment = 'All Segments')
• 'Bank Transfer & Card Distribution'  → payment method mix breakdown
• 'Billing & Receipt Channel Mix'      → invoice vs sales receipt breakdown
• 'Managed Pay Subscribers'            → managed payment subscribers
• 'Tenure Cohort'                      → merchant tenure-based cohorts (new/mid/mature)

SEGMENT-TO-INTENT LOOKUP (map business question intent to the correct representative Segment):
  When a question asks about a customer TYPE, use the most representative CHILD segment,
  not the parent Segment Name, and not an outlier sub-segment.

  "card-only customers"            → Segment = 'CC 100%'
                                     (under "Bank Transfer & Card Distribution")
                                     Avg Invoice ~$1,234 — NOT 'MAIP 100%' ($5,770, wrong category)

  "mixed payment customers"        → Segment = 'Bank & Card Blended'
                                     (under "Bank Transfer & Card Distribution")
                                     Avg Invoice ~$1,558 — NOT '100% SR TPV' ($10,139, outlier)

  "ACH/bank-only customers"        → Segment = 'ACH 100%'
                                     (under "Bank Transfer & Card Distribution")

  "new merchants (0-12 months)"    → Segment = 'New (0-12 months)'
                                     (under "Tenure Cohort")

  "mature/established merchants"   → Segment = 'Mature (25+ months)'
                                     (under "Tenure Cohort")

  "invoice-based customers"        → Segment = 'Invoice Only'
                                     (under "Billing & Receipt Channel Mix")

  "sales receipt customers"        → Segment = '100% SR TPV'
                                     (under "Billing & Receipt Channel Mix")
                                     WARNING: this is a HIGH-TICKET outlier (~$10k avg invoice).
                                     Do NOT use as representative of "mixed" customers.

  VERIFIED COMPARISON (card-only vs mixed, Feb 2026):
    CC 100% avg invoice      = $1,233.96
    Bank & Card Blended avg  = $1,557.59   → mixed pays 26.2% more per invoice
    The $10,139 figure is '100% SR TPV', an outlier, not a general mixed-payment behavior.

TPV & VOLUME (absolute):
• "Eco TPV 31d Absolute"           — total ecosystem payment volume
• "CC TPV 31d Absolute"            — credit card payment volume
• "ACH TPV 31d Absolute"           — ACH/bank transfer payment volume
• "Eco CP Amount 31d Absolute"     — connected payments (Intuit rails) volume
• "Eco Non-CP Amount 31d Absolute" — non-connected payments volume
  Relationship: Eco TPV ≈ CC TPV + ACH TPV ≈ CP Amount + Non-CP Amount

INVOICE & SALES RECEIPT VOLUME:
• "Eco Invoice Amount 31d Absolute"
• "Eco Sales Receipt Amount 31d Absolute"
• "Eco Recurring Invoice Amount 31d Absolute"
• "Eco Non-Recurring Invoice Amount 31d Absolute"
• "Eco CP Recurring Invoice Amount 31d Absolute"

TRANSACTION COUNTS:
• "Eco Txns 31d Absolute"               — total ecosystem transactions
• "CC Txns 31d Absolute"
• "ACH Txns 31d Absolute"
• "Eco Invoice Txns 31d Absolute"
• "Eco Sales Receipt Txns 31d Absolute"
• "Eco Recurring Invoice Txns 31d Absolute"
• "Eco Non-Recurring Invoice Txns 31d Absolute"
• "Eco CP Txns 31d Absolute"
• "Eco Non-CP Txns 31d Absolute"

ACTIVE COUNTS:
• "Eco Actives 31d Absolute"  — total active merchants (use Aggregate row only for totals)
• cc_actives_31d              — credit-card-active merchants
• "Eco Invoice Actives 31D"

CUSTOMER LIFECYCLE COUNTS (L2a decomposition):
• "31d Active Repeat Absolute"    — retained merchants (active prior + current period)
• "31d Active Resurrect Absolute" — win-backs (previously lapsed, now returned)
• "31d Active GNA Absolute"       — Gross New Adds (first-time actives)
• "31d Active Lapse Absolute"     — all zeros in this dataset; use churn columns instead

CUSTOMER VALUE BY LIFECYCLE:
• "31d Repeat CV Absolute"      — TPV from repeat merchants
• "31d Resurrect CV Absolute"   — TPV from resurrected merchants
• "31d GNA CV Absolute"         — TPV from new merchants
• "31d Lapse CV Absolute"

TRANSACTION COUNTS BY LIFECYCLE:
• "31d Repeat Txn Absolute"
• "31d Resurrect Txn Absolute"
• "31d GNA Txn Absolute"
• "31d Lapse Txn Absolute"

CHURN METRICS — CRITICAL DATA SEMANTICS:

  ╔══════════════════════════════════════════════════════════════════════════╗
  ║  CHURN COLUMN SEMANTICS  (read this before every churn query)           ║
  ╠══════════════════════════════════════════════════════════════════════════╣
  ║                                                                          ║
  ║  active_cohorted_churn  =  ABSOLUTE COUNT of merchants who churned.     ║
  ║  It is NOT a rate. It is NOT a decimal. It is an integer merchant count.║
  ║                                                                          ║
  ║  Examples from the actual dataset (Feb 2026):                           ║
  ║    active_cohorted_churn = 1        → 1 merchant churned  (TINY noise)  ║
  ║    active_cohorted_churn = 570,000  → 570k merchants churned (real)     ║
  ║    active_cohorted_churn = NULL     → no data for this segment/period   ║
  ║                                                                          ║
  ║  TO COMPUTE CHURN RATE:                                                  ║
  ║    churn_rate = total_churn / LAG("Eco Actives 31d Absolute") OVER (   ║
  ║                     PARTITION BY "Segment Name", Segment               ║
  ║                     ORDER BY date_end                                   ║
  ║                 )                                                        ║
  ║    Use PRIOR period actives as denominator — NOT current actives.       ║
  ║    Current actives already excludes churned merchants → denominator too ║
  ║    small → inflated rate.                                                ║
  ║                                                                          ║
  ║  ── SORT ORDER (THIS BUG CAUSED A REAL FAILURE) ────────────────────── ║
  ║  WORST churn = HIGHEST rate = ORDER BY churn_rate DESC NULLS LAST      ║
  ║  BEST  churn = LOWEST  rate = ORDER BY churn_rate ASC  NULLS LAST      ║
  ║  A segment with 0.4% churn is NOT the "worst" — it is near the BEST.   ║
  ║  If your "worst" result is below 1%, your sort order is INVERTED.       ║
  ║                                                                          ║
  ║  ── NOISE THRESHOLD (MANDATORY) ───────────────────────────────────── ║
  ║  Segments with tiny denominators (e.g. 239 merchants) produce inflated  ║
  ║  or meaningless rates. ALWAYS apply a minimum size filter:              ║
  ║    HAVING prior_actives >= 100   → minimum segment size for reliability ║
  ║    AND   total_churn   >= 5      → minimum churn count (not noise)      ║
  ║  Without this, a segment of 239 with 1 churn (0.42%) looks "worst"     ║
  ║  while a segment of 570k with 570k churn (100%) is ignored.            ║
  ║                                                                          ║
  ║  ── NULL HANDLING ──────────────────────────────────────────────────── ║
  ║  NULL churn ≠ 0% churn. NULL means NO DATA for that segment/period.    ║
  ║  NEVER: COALESCE(churn, 0) when computing rates → inflates zeros        ║
  ║  ALWAYS: filter WHERE active_cohorted_churn IS NOT NULL before ranking ║
  ║  If churn IS NULL and actives IS NOT NULL → flag as "insufficient data" ║
  ║  DO NOT invent a rate; DO NOT default to count-based fallback calc.     ║
  ║                                                                          ║
  ║  ── PRIMARY SORT STRATEGY ─────────────────────────────────────────── ║
  ║  Sort 1st: total_churn DESC      (absolute impact — most merchants lost)║
  ║  Sort 2nd: churn_rate_pct DESC   (relative severity)                    ║
  ║  Report both — absolute AND rate — so small/large segments are fairly   ║
  ║  compared.  A 100% churn of 5 merchants < 10% churn of 10,000.         ║
  ╚══════════════════════════════════════════════════════════════════════════╝

  Column list (all are ABSOLUTE COUNTS of churned merchants, NULL = no data):
  • active_cohorted_churn        — deliberate cancellations this period
  • passive_cohorted_churn       — stopped transacting without cancelling
  • hvc_cohorted_churn           — high-value customers churned
  • non_hvc_cohorted_churn       — non-HVC customers churned
  • active_cohorted_churn_py     — same count, prior year period (for YoY)
  • passive_cohorted_churn_py
  • hvc_cohorted_churn_py
  • non_hvc_cohorted_churn_py
  • qbo_active_cohorted_churn    — QBO-active customers churned
  • pymt_active_cohorted_churn   — payments-active customers churned

PER-ACTIVE AVERAGES:
• "Avg Paid Invoice Amt"
• "Avg SR Amt"
• "Avg. Intuit Paid Inv Amt"
• "Avg. Intut Paid SR Amt"
• "Intuit Paid Inv per Active"
• "Intuit Paid SR per Active"
"""

SQL_RULES = """
=== SQL GENERATION RULES ===

Engine: DuckDB (SQL dialect close to PostgreSQL)
Table:  payments  (in-memory — all column names case-sensitive, double-quote those with spaces)

RULE 1 — ANTI-DOUBLE-COUNT (HIGHEST PRIORITY):
  For any TOTAL BUSINESS metric, ALWAYS scope to the Aggregate row:
    WHERE "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
  For SEGMENT BREAKDOWN, pick one Segment Name and exclude 'All Segments':
    WHERE "Segment Name" = '<name>' AND Segment != 'All Segments'
  NEVER run SUM/AVG across all segments together.

RULE 2 — TIME GRAIN:
  Always filter "Time Period" = 'Week' OR 'Month' — never mix grains in one query.

RULE 3 — LAST PERIOD:
  "Last month" → WHERE date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period" = 'Month' AND "Segment Name" = 'Aggregate')
  "Last week"  → WHERE date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period" = 'Week'  AND "Segment Name" = 'Aggregate')

RULE 4 — YoY COMPARISON (_py columns PREFERRED over LAG):
  The dataset pre-joins prior-year values as _py suffix columns — always use these first.
  They are safer, faster, and avoid window function granularity errors.

  For CHURN columns (pre-computed decimal rates — see churn semantics above):
    -- YoY delta in percentage points (NOT a ratio — both values are already rates)
    ROUND((active_cohorted_churn - active_cohorted_churn_py) * 100, 4) AS churn_rate_yoy_delta_pp

  For VOLUME columns (no _py equivalent — use LAG on Aggregate single-partition only):
    LAG("Eco TPV 31d Absolute", 12) OVER (ORDER BY date_end) AS tpv_py

RULE 5 — GRANULARITY TRAP (CRITICAL — this bug has produced 65,574,525% phantom growth rates):
  "Segment Name" is a PARENT CATEGORY containing multiple child "Segment" leaf rows.
  Example: Segment Name = 'Managed Pay Subscribers' contains children:
    'MAIP 100%', 'Batch Auto-Entry', 'Other Mixed', etc.

  What goes wrong with PARTITION BY "Segment Name" only:
    Month 1: row for 'MAIP 100%'     has CP volume = $1,000
    Month 2: row for 'Other Mixed'   has CP volume = $655,746,253  (different sub-segment!)
    LAG() picks up $1,000 as the "prior" for $655M → growth = 65,574,525% ← NONSENSE
    This is a calculation artifact, not a real business event.

  WRONG:  PARTITION BY "Segment Name"
  ALWAYS: PARTITION BY "Segment Name", Segment   ← ensures like-for-like at leaf level

  Every window function (LAG, LEAD, ROW_NUMBER, RANK) over segment data
  MUST partition by BOTH "Segment Name" AND Segment — no exceptions.
  If a growth rate > 500% appears, it is almost certainly this bug. Rewrite with correct partition.

RULE 6 — COLUMN PARENT-CHILD RELATIONSHIPS (prevents impossible >100% share ratios):
  Metric families have strict parent-child hierarchies. NEVER divide a child metric
  by a non-parent total — this produces mathematically impossible ratios (e.g. 120% share).

  Correct parent-child tree:
  ┌─ Eco TPV 31d Absolute  (top-level total)
  │   ├─ CC TPV 31d Absolute
  │   └─ ACH TPV 31d Absolute
  │
  ├─ Eco CP Amount 31d Absolute  (connected payments total — includes SR + invoices)
  │   └─ Eco CP Recurring Invoice Amount 31d Absolute  (child of CP, not of Invoice Amount)
  │
  └─ Eco Invoice Amount 31d Absolute  (invoice total — NOT same as CP Amount)
      ├─ Eco Recurring Invoice Amount 31d Absolute
      └─ Eco Non-Recurring Invoice Amount 31d Absolute

  Share calculation rules:
  • recurring_invoice_share   = "Eco Recurring Invoice Amount 31d Absolute"     / NULLIF("Eco Invoice Amount 31d Absolute", 0)
  • non_recurring_share       = "Eco Non-Recurring Invoice Amount 31d Absolute" / NULLIF("Eco Invoice Amount 31d Absolute", 0)
  • cp_share_of_tpv           = "Eco CP Amount 31d Absolute"                    / NULLIF("Eco TPV 31d Absolute", 0)
  • cp_recurring_share_of_cp  = "Eco CP Recurring Invoice Amount 31d Absolute"  / NULLIF("Eco CP Amount 31d Absolute", 0)

  NEVER: "Eco Non-Recurring Invoice Amount" / "Eco CP Amount"  ← wrong parent → impossible ratio
  NEVER: "Eco Recurring Invoice Amount"     / "Eco CP Amount"  ← wrong parent → impossible ratio

RULE 7 — MIX-SHIFT PATTERN (L2a decomposition):
  SELECT date_end,
      ROUND("31d Active GNA Absolute"       / NULLIF("Eco Actives 31d Absolute",0)*100, 2) AS gna_pct,
      ROUND("31d Active Resurrect Absolute" / NULLIF("Eco Actives 31d Absolute",0)*100, 2) AS resurrect_pct,
      ROUND("31d Active Repeat Absolute"    / NULLIF("Eco Actives 31d Absolute",0)*100, 2) AS repeat_pct,
      ROUND("Eco TPV 31d Absolute"          / NULLIF("Eco Actives 31d Absolute",0), 2)     AS tpv_per_active
  FROM payments
  WHERE "Time Period" = 'Month'
    AND "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
  ORDER BY date_end DESC;

RULE 8 — NULL SAFETY:
  NEVER cast numeric columns directly (::float breaks on null strings).
  Use COALESCE(col, 0) for arithmetic, NULLIF(col, 0) for denominators.

RULE 9 — SANITY CHECKS (apply before returning any result):
  • Growth rate > 500%        → Granularity Trap (Rule 5). Fix PARTITION BY.
  • Share metric > 100%       → Wrong parent denominator (Rule 6). Fix the division.
  • Active count > 800K       → Likely double-counting (Rule 1). Add Aggregate filter.
  • "Worst churn" < 1%        → CRITICAL: Sort order is inverted OR wrong denominator.
                                 "Worst" = HIGHEST rate. Must ORDER BY churn_rate DESC NULLS LAST.
                                 Check that denominator is PRIOR actives (LAG), not current actives.
  • Churn rate appears exactly = 1/actives (e.g. 0.0041)
                              → Agent invented a numerator of 1 from a NULL column.
                                 Re-check: is active_cohorted_churn NULL for this row?
                                 If NULL → report "Insufficient data" — never compute from NULL.
  These are red flags, not valid business results. Always self-check before finalising SQL.

RULE 13 — CHURN RATE — THREE MANDATORY REQUIREMENTS:
  active_cohorted_churn and passive_cohorted_churn are ABSOLUTE COUNTS of churned merchants.
  They are NOT decimals, flags, or rates. A value of 570000 means 570,000 merchants churned.

  REQUIREMENT 1 — CORRECT DENOMINATOR (prior-period actives, never current):
    WRONG:   churn / "Eco Actives 31d Absolute"
             → current actives already excludes churned → denominator too small → rate too high
    CORRECT: churn / NULLIF(LAG("Eco Actives 31d Absolute")
                            OVER (PARTITION BY "Segment Name", Segment ORDER BY date_end), 0)

  REQUIREMENT 2 — NULL = NO DATA, not zero churn:
    WRONG:   COALESCE(active_cohorted_churn, 0) in the numerator for churn RATE
             → invents a 0/actives = 0% churn for segments with no data
    CORRECT: WHERE active_cohorted_churn IS NOT NULL AND prior_actives >= 50
    If churn is NULL → the row MUST be excluded or labelled "Insufficient data".
    NEVER report a rate computed from a NULL numerator.

  REQUIREMENT 3 — SORT ORDER:
    "Worst segment for churn"  → ORDER BY churn_rate DESC NULLS LAST  (highest = worst)
    "Best segment for churn"   → ORDER BY churn_rate ASC  NULLS LAST  (lowest = best)
    If the top result shows a rate < 1%, verify sort direction before returning.

  CORRECT CHURN RATE QUERY PATTERN:
    WITH churn_base AS (
        SELECT
            "Segment Name", Segment, date_end,
            COALESCE(active_cohorted_churn,0) + COALESCE(passive_cohorted_churn,0) AS total_churn,
            LAG("Eco Actives 31d Absolute")
                OVER (PARTITION BY "Segment Name", Segment ORDER BY date_end) AS prior_actives
        FROM payments
        WHERE "Time Period" = 'Month'
          AND Segment != 'All Segments'
    )
    SELECT
        "Segment Name", Segment, date_end,
        total_churn,
        prior_actives,
        ROUND(total_churn / NULLIF(prior_actives, 0) * 100, 4) AS churn_rate_pct
    FROM churn_base
    WHERE date_end = (SELECT MAX(date_end) FROM payments
                      WHERE "Time Period"='Month' AND "Segment Name"='Aggregate')
      AND prior_actives >= 50          -- exclude micro-segments (noise)
      AND active_cohorted_churn IS NOT NULL  -- exclude no-data rows
    ORDER BY churn_rate_pct DESC NULLS LAST  -- worst first
    LIMIT 10;

  MINIMUM SIZE FILTER: Always apply WHERE prior_actives >= 50 AND total_churn > 0.
  A segment with 1 prior active and 1 churn = 100% rate but is statistically meaningless.

RULE 10 — GENERAL:
  • Round dollars to 2dp, rates/shares to 4dp
  • Date math: date_end + INTERVAL '1 year', date_trunc('month', date_end)
  • LIMIT 50 unless the question requires all rows
  • Double-quote all column names containing spaces or special characters
  • Return only valid DuckDB SQL — no prose inside the SQL block

RULE 11 — PEAK VALUE TRAP (prevents reporting outlier sub-segments as category averages):
  When a question asks for "the average" for a customer TYPE (e.g. card-only, mixed),
  you must SELECT the ONE representative sub-segment from the SEGMENT-TO-INTENT LOOKUP,
  OR compute a weighted average across all relevant child segments.
  NEVER just ORDER BY the metric DESC and report the top row — that is the outlier, not the average.

  WRONG pattern (reports outlier as average):
    SELECT "Avg Paid Invoice Amt" FROM payments
    WHERE "Segment Name" = 'Billing & Receipt Channel Mix'
    ORDER BY "Avg Paid Invoice Amt" DESC LIMIT 1;
    → returns '100% SR TPV' at $10,139 — this is the HIGH-TICKET outlier, not the category average

  CORRECT pattern (select the representative blended segment):
    SELECT Segment, "Avg Paid Invoice Amt"
    FROM payments
    WHERE "Segment Name" = 'Bank Transfer & Card Distribution'
      AND Segment IN ('CC 100%', 'Bank & Card Blended', 'ACH 100%')
      AND "Time Period" = 'Month'
      AND date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period"='Month' AND "Segment Name"='Aggregate')
    ORDER BY Segment;

  OR weighted average across all child segments:
    SELECT "Segment Name",
           ROUND(SUM("Avg Paid Invoice Amt" * "Eco Actives 31d Absolute")
                 / NULLIF(SUM("Eco Actives 31d Absolute"), 0), 2) AS weighted_avg_invoice
    FROM payments
    WHERE "Segment Name" = 'Bank Transfer & Card Distribution'
      AND Segment != 'All Segments'
      AND "Time Period" = 'Month'
      AND date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period"='Month' AND "Segment Name"='Aggregate')
    GROUP BY "Segment Name";

RULE 12 — PRE-COMPUTED PER-ACTIVE AVERAGES:
  Columns like "Avg Paid Invoice Amt", "Avg SR Amt", "Avg. Intuit Paid Inv Amt" are ALREADY
  per-active averages computed at the segment level. Do NOT AVG() them naively across rows —
  that produces an unweighted average of averages (biased by segment size).
  If you must combine multiple segments: use weighted average as shown in Rule 11.

RULE 13 — CHURN RATE CALCULATION (the 0.4184% hallucination fix):

  WHAT WENT WRONG: The model divided active_cohorted_churn (count = 1) by
  CURRENT actives (239) → 0.42%. Then sorted ASC → returned the BEST segment
  as "worst". Both the calculation and the sort direction were wrong.

  CORRECT FORMULA:
    total_churn     = COALESCE(active_cohorted_churn,0) + COALESCE(passive_cohorted_churn,0)
    prior_actives   = LAG("Eco Actives 31d Absolute") OVER (PARTITION BY "Segment Name", Segment ORDER BY date_end)
    churn_rate_pct  = ROUND(total_churn / NULLIF(prior_actives, 0) * 100, 2)

  Example showing why PRIOR actives matters:
    Segment A: 1 churned / 240 PRIOR actives = 0.42%   ← tiny noise
    Segment B: 570,000 churned / 580,000 PRIOR actives = 98.3% ← real crisis
    Using CURRENT actives for B: 570,000 / 10,000 = 5,700% ← impossible

  MANDATORY FILTERS — always apply for ranking/worst queries:
    prior_actives >= 50     → exclude micro-segments (single-digit merchants)
    total_churn > 0         → exclude zero-churn rows
    churn_rate_pct <= 110   → exclude impossible anomalies (flag separately)
    active_cohorted_churn IS NOT NULL OR passive_cohorted_churn IS NOT NULL
                            → exclude rows with no churn data at all

  SORT DIRECTION — this is a one-way rule with no exceptions:
    "worst churn" / "highest churn" → ORDER BY churn_rate_pct DESC NULLS LAST
    "best churn"  / "lowest churn"  → ORDER BY churn_rate_pct ASC  NULLS LAST

  YoY COMPARISON for churn (using _py columns which are also absolute counts):
    yoy_delta_pp = ROUND(
        (total_churn / NULLIF(prior_actives,0) * 100)
        - (total_churn_py / NULLIF(prior_actives_py,0) * 100)
    , 2)
    where total_churn_py = COALESCE(active_cohorted_churn_py,0) + COALESCE(passive_cohorted_churn_py,0)

RULE 14 — WORST/BEST DIRECTION GUARD:
  "worst", "highest", "most", "largest"  → ORDER BY metric DESC NULLS LAST
  "best",  "lowest",  "least", "smallest" → ORDER BY metric ASC  NULLS LAST

  Self-check: if question says "worst churn" and result shows 0.4% — that is the BEST,
  not the worst. Verified failure: 0.4184% called "worst" while 87%+ rates existed.
"""

SQL_TEMPLATES = """
=== VERIFIED SQL TEMPLATES — adapt these, do not generate from scratch ===

-- T1: Total active customers YoY (Aggregate only, single-partition LAG)
WITH series AS (
    SELECT date_end, "Eco Actives 31d Absolute" AS actives,
           EXTRACT(MONTH FROM date_end) AS mo
    FROM payments
    WHERE "Time Period" = 'Month'
      AND "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
)
SELECT date_end, actives,
       LAG(actives,12) OVER (ORDER BY date_end) AS actives_py,
       ROUND((actives - LAG(actives,12) OVER (ORDER BY date_end))
             / NULLIF(LAG(actives,12) OVER (ORDER BY date_end),0)*100, 2) AS yoy_pct
FROM series
WHERE mo = (SELECT EXTRACT(MONTH FROM MAX(date_end)) FROM series)
ORDER BY date_end DESC;

-- T2: L2a mix-shift monthly (GNA / Resurrect / Repeat share)
SELECT date_end,
       ROUND("31d Active GNA Absolute"       / NULLIF("Eco Actives 31d Absolute",0)*100, 2) AS gna_pct,
       ROUND("31d Active Resurrect Absolute" / NULLIF("Eco Actives 31d Absolute",0)*100, 2) AS resurrect_pct,
       ROUND("31d Active Repeat Absolute"    / NULLIF("Eco Actives 31d Absolute",0)*100, 2) AS repeat_pct,
       ROUND("Eco TPV 31d Absolute"          / NULLIF("Eco Actives 31d Absolute",0), 2)     AS tpv_per_active
FROM payments
WHERE "Time Period" = 'Month'
  AND "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
ORDER BY date_end DESC LIMIT 24;

-- T3: Active vs passive churn FY24–FY26, absolute counts + YoY delta in percentage points
--     NOTE: churn columns are COUNTS. Divide by prior actives to get rate.
--     For trend analysis (which direction is churn moving?) compare count to _py count.
SELECT
    date_end, FY,
    COALESCE(active_cohorted_churn,  0) AS active_churn_count,
    COALESCE(passive_cohorted_churn, 0) AS passive_churn_count,
    COALESCE(active_cohorted_churn,  0) + COALESCE(passive_cohorted_churn, 0) AS total_churn_count,
    COALESCE(active_cohorted_churn_py,  0) AS active_churn_count_py,
    COALESCE(passive_cohorted_churn_py, 0) AS passive_churn_count_py,
    -- YoY change in absolute count (positive = churn got worse)
    COALESCE(active_cohorted_churn,0)  - COALESCE(active_cohorted_churn_py,0)  AS active_churn_yoy_delta,
    COALESCE(passive_cohorted_churn,0) - COALESCE(passive_cohorted_churn_py,0) AS passive_churn_yoy_delta
FROM payments
WHERE "Time Period" = 'Month'
  AND "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
ORDER BY date_end;

-- T9: WORST churn rate by segment — CORRECT version
--
--  Three bugs this template prevents (all verified in prod):
--   BUG 1: dividing by CURRENT actives → merchants already gone, denominator too small
--   BUG 2: ORDER BY churn_rate ASC → returns BEST segment as "worst"
--   BUG 3: COALESCE(churn,0) in WHERE keeps NULL rows → they sort to "top" of ASC
--
--  FIX: prior actives via LAG, DESC sort, IS NOT NULL guard, min 50 actives filter
WITH base AS (
    SELECT
        "Segment Name",
        Segment,
        date_end,
        COALESCE(active_cohorted_churn,  0)  AS active_churn,
        COALESCE(passive_cohorted_churn, 0)  AS passive_churn,
        COALESCE(active_cohorted_churn,  0) + COALESCE(passive_cohorted_churn, 0) AS total_churn,
        COALESCE(active_cohorted_churn_py,  0) + COALESCE(passive_cohorted_churn_py, 0) AS total_churn_py,
        -- PRIOR period actives = correct denominator
        LAG("Eco Actives 31d Absolute") OVER (
            PARTITION BY "Segment Name", Segment   -- both levels — Rule 5
            ORDER BY date_end
        ) AS prior_actives,
        LAG("Eco Actives 31d Absolute", 13) OVER (
            PARTITION BY "Segment Name", Segment
            ORDER BY date_end
        ) AS prior_actives_py
    FROM payments
    WHERE "Time Period" = 'Month'
      AND Segment != 'All Segments'
),
latest AS (
    SELECT MAX(date_end) AS max_date
    FROM payments WHERE "Time Period" = 'Month' AND "Segment Name" = 'Aggregate'
)
SELECT
    "Segment Name",
    Segment,
    date_end,
    total_churn                                                        AS total_churn_merchants,
    prior_actives                                                      AS prior_period_actives,
    ROUND(total_churn    / NULLIF(prior_actives,    0) * 100, 2)      AS churn_rate_pct,
    ROUND(total_churn_py / NULLIF(prior_actives_py, 0) * 100, 2)      AS churn_rate_pct_py,
    -- YoY delta in percentage points (pp), not ratio
    ROUND(
        total_churn    / NULLIF(prior_actives,    0) * 100
      - total_churn_py / NULLIF(prior_actives_py, 0) * 100
    , 2)                                                               AS churn_rate_yoy_delta_pp
FROM base
WHERE date_end = (SELECT max_date FROM latest)
  AND (active_churn > 0 OR passive_churn > 0)   -- exclude no-data rows
  AND prior_actives >= 50                         -- exclude micro-segment noise
  AND total_churn / NULLIF(prior_actives,0) <= 1.10  -- exclude impossible >110% anomalies
ORDER BY churn_rate_pct DESC NULLS LAST           -- WORST = HIGHEST = DESC (never ASC)
LIMIT 20;

-- T9b: Same query but for YoY comparison (compare same month last year)
-- Replace the WHERE date_end clause with a range filter and add EXTRACT(MONTH) filter
-- to compare this February vs last February, etc.


-- T4: HVC vs Non-HVC churn gap, last 6 months, YoY via _py columns
SELECT date_end,
       COALESCE(hvc_cohorted_churn, 0)      AS hvc_churn,
       COALESCE(non_hvc_cohorted_churn, 0)  AS non_hvc_churn,
       COALESCE(hvc_cohorted_churn_py, 0)   AS hvc_churn_py,
       COALESCE(non_hvc_cohorted_churn_py, 0) AS non_hvc_churn_py,
       ROUND(COALESCE(hvc_cohorted_churn,0) * 1.0
             / NULLIF(COALESCE(hvc_cohorted_churn,0)+COALESCE(non_hvc_cohorted_churn,0),0)*100, 2) AS hvc_share_pct,
       ROUND((COALESCE(hvc_cohorted_churn,0) - COALESCE(hvc_cohorted_churn_py,0))
             / NULLIF(COALESCE(hvc_cohorted_churn_py,0),0)*100, 2) AS hvc_churn_yoy_pct
FROM payments
WHERE "Time Period" = 'Month'
  AND "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
  AND date_end >= (SELECT MAX(date_end) FROM payments WHERE "Time Period"='Month') - INTERVAL '6 months'
ORDER BY date_end;

-- T5: Segment health scorecard at latest month
SELECT Segment, "Segment Name",
       ROUND("Eco TPV 31d Absolute"       / NULLIF("Eco Actives 31d Absolute",0), 2)     AS tpv_per_active,
       ROUND("31d Active Repeat Absolute" / NULLIF("Eco Actives 31d Absolute",0)*100, 2) AS repeat_rate_pct,
       ROUND("Avg Paid Invoice Amt", 2)                                                   AS avg_invoice,
       COALESCE(active_cohorted_churn,0) + COALESCE(passive_cohorted_churn,0)            AS total_churn
FROM payments
WHERE "Time Period" = 'Month'
  AND date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period"='Month' AND "Segment Name"='Aggregate')
  AND Segment != 'All Segments'
ORDER BY tpv_per_active DESC;

-- T6: Growth-vs-retention matrix (CORRECTED — partitions by BOTH Segment Name AND Segment)
WITH segment_series AS (
    SELECT
        "Segment Name",
        Segment,
        date_end,
        "Eco TPV 31d Absolute"       AS volume,
        "Eco Actives 31d Absolute"   AS actives,
        "31d Active Repeat Absolute" AS repeat_actives,
        LAG("Eco TPV 31d Absolute", 1)
            OVER (PARTITION BY "Segment Name", Segment   -- BOTH columns — never just one
                  ORDER BY date_end) AS prev_volume
    FROM payments
    WHERE "Time Period" = 'Month'
      AND Segment != 'All Segments'
),
latest_month AS (
    SELECT MAX(date_end) AS max_date
    FROM payments WHERE "Time Period" = 'Month' AND "Segment Name" = 'Aggregate'
)
SELECT
    "Segment Name",
    Segment,
    ROUND(volume / NULLIF(actives, 0), 2)                              AS tpv_per_active,
    ROUND(repeat_actives / NULLIF(actives, 0) * 100, 2)               AS repeat_rate_pct,
    ROUND((volume - prev_volume) / NULLIF(prev_volume, 0) * 100, 2)  AS mom_volume_growth_pct,
    CASE
        WHEN (volume - prev_volume) / NULLIF(prev_volume,0) > 0
             AND repeat_actives / NULLIF(actives,0) >= 0.5 THEN 'Strong Growth & Retention'
        WHEN (volume - prev_volume) / NULLIF(prev_volume,0) > 0
             AND repeat_actives / NULLIF(actives,0) <  0.5 THEN 'Strong Growth, Poor Retention'
        WHEN (volume - prev_volume) / NULLIF(prev_volume,0) <= 0
             AND repeat_actives / NULLIF(actives,0) >= 0.5 THEN 'Flat/Declining Growth, Great Retention'
        ELSE 'Needs Attention'
    END AS quadrant
FROM segment_series
WHERE date_end = (SELECT max_date FROM latest_month)
ORDER BY mom_volume_growth_pct DESC NULLS LAST;

-- T7: Fastest-growing segment by connected payment volume MoM
--     + correct recurring vs non-recurring share breakdown
--     KEY FIXES vs common mistakes:
--       1. PARTITION BY "Segment Name", Segment  ← prevents 65M% phantom rates
--       2. Recurring/non-recurring shares divided by "Eco Invoice Amount" (their direct parent)
--          NOT by "Eco CP Amount" (wrong parent → impossible >100% ratios)
WITH cp_growth AS (
    SELECT
        "Segment Name",
        Segment,
        date_end,
        "Eco CP Amount 31d Absolute"                    AS cp_volume,
        "Eco Invoice Amount 31d Absolute"               AS invoice_volume,  -- correct parent for invoice shares
        "Eco Recurring Invoice Amount 31d Absolute"     AS recurring_inv,
        "Eco Non-Recurring Invoice Amount 31d Absolute" AS non_recurring_inv,
        LAG("Eco CP Amount 31d Absolute") OVER (
            PARTITION BY "Segment Name", Segment        -- BOTH levels — never just "Segment Name"
            ORDER BY date_end
        ) AS prior_cp_volume
    FROM payments
    WHERE "Time Period" = 'Month'
      AND Segment != 'All Segments'
),
latest AS (
    SELECT MAX(date_end) AS max_date
    FROM payments WHERE "Time Period" = 'Month' AND "Segment Name" = 'Aggregate'
)
SELECT
    "Segment Name",
    Segment,
    date_end,
    ROUND(cp_volume, 2)                                                             AS cp_volume,
    ROUND((cp_volume - prior_cp_volume) / NULLIF(prior_cp_volume, 0) * 100, 2)    AS cp_mom_growth_pct,
    -- Share of recurring vs non-recurring within invoice volume (correct parent)
    ROUND(recurring_inv     / NULLIF(invoice_volume, 0) * 100, 2)                 AS recurring_share_of_invoices_pct,
    ROUND(non_recurring_inv / NULLIF(invoice_volume, 0) * 100, 2)                 AS non_recurring_share_of_invoices_pct,
    -- CP recurring as share of total CP (correct parent)
    ROUND("Eco CP Recurring Invoice Amount 31d Absolute" / NULLIF(cp_volume, 0) * 100, 2) AS cp_recurring_share_pct
FROM cp_growth
WHERE date_end = (SELECT max_date FROM latest)
  AND prior_cp_volume IS NOT NULL
  AND ABS((cp_volume - prior_cp_volume) / NULLIF(prior_cp_volume, 0) * 100) < 500  -- guard: exclude granularity-trap artifacts
ORDER BY cp_mom_growth_pct DESC
LIMIT 10;

-- T8: Card-only vs mixed payment customers — avg invoice comparison (CORRECT)
--     WRONG: querying "Billing & Receipt Channel Mix" and reporting the top row ($10,139)
--     CORRECT: use the SEGMENT-TO-INTENT LOOKUP to select representative leaf segments:
--       'CC 100%'          = card-only  (avg ~$1,234)
--       'Bank & Card Blended' = mixed   (avg ~$1,558)
--     '100% SR TPV' is a HIGH-TICKET OUTLIER — exclude from behavioral comparison.
SELECT
    Segment,
    "Segment Name",
    ROUND("Avg Paid Invoice Amt", 2)                                              AS avg_invoice_amt,
    ROUND("Avg SR Amt", 2)                                                        AS avg_sr_amt,
    ROUND("Eco TPV 31d Absolute" / NULLIF("Eco Actives 31d Absolute", 0), 2)    AS tpv_per_active,
    ROUND("Eco Txns 31d Absolute" / NULLIF("Eco Actives 31d Absolute", 0), 2)   AS txns_per_active,
    "Eco Actives 31d Absolute"                                                    AS actives
FROM payments
WHERE "Segment Name" = 'Bank Transfer & Card Distribution'
  AND Segment IN ('CC 100%', 'Bank & Card Blended', 'ACH 100%')   -- representative segments only
  AND "Time Period" = 'Month'
  AND date_end = (SELECT MAX(date_end) FROM payments
                  WHERE "Time Period" = 'Month' AND "Segment Name" = 'Aggregate')
ORDER BY avg_invoice_amt DESC;

-- T9: Worst churn segment — VERIFIED CORRECT PATTERN
--
--  FAILURES THIS TEMPLATE PREVENTS:
--  ✗ Wrong:  churn/CURRENT actives → 0.42% called "worst" (should be 87%+)
--  ✗ Wrong:  ORDER BY churn_rate ASC → returns BEST, not worst
--  ✗ Wrong:  COALESCE(churn,0) without IS NOT NULL filter → NULL rows rank as 0%
--  ✗ Wrong:  No noise threshold → 1 churn from 239-merchant segment crowns as "worst"
--
--  CORRECT APPROACH:
--  1. Use LAG(prior actives) — PARTITION BY "Segment Name", Segment
--  2. Exclude NULL churn rows entirely
--  3. Apply noise threshold: prior_actives >= 100 AND total_churn >= 5
--  4. Sort DESC for worst; report BOTH absolute AND rate
--  5. Self-check: if top rate < 5%, something is wrong
WITH churn_base AS (
    SELECT
        "Segment Name",
        Segment,
        date_end,
        COALESCE(active_cohorted_churn,  0) AS active_churn,
        COALESCE(passive_cohorted_churn, 0) AS passive_churn,
        COALESCE(active_cohorted_churn,  0)
            + COALESCE(passive_cohorted_churn, 0)  AS total_churn,
        -- PRIOR period actives — the correct denominator
        LAG("Eco Actives 31d Absolute") OVER (
            PARTITION BY "Segment Name", Segment   -- BOTH keys — granularity rule
            ORDER BY date_end
        ) AS prior_actives,
        -- YoY comparison via _py columns
        COALESCE(active_cohorted_churn_py, 0)
            + COALESCE(passive_cohorted_churn_py, 0) AS total_churn_py
    FROM payments
    WHERE "Time Period" = 'Month'
      AND Segment        != 'All Segments'
      -- Only include rows where churn data actually exists
      AND (active_cohorted_churn IS NOT NULL OR passive_cohorted_churn IS NOT NULL)
),
latest AS (
    SELECT MAX(date_end) AS max_date
    FROM payments WHERE "Time Period" = 'Month' AND "Segment Name" = 'Aggregate'
)
SELECT
    "Segment Name",
    Segment,
    date_end,
    active_churn,
    passive_churn,
    total_churn,
    prior_actives,
    ROUND(total_churn   / NULLIF(prior_actives, 0) * 100, 2) AS churn_rate_pct,
    total_churn_py,
    ROUND(total_churn_py / NULLIF(prior_actives, 0) * 100, 2) AS churn_rate_py_pct
FROM churn_base
WHERE date_end    = (SELECT max_date FROM latest)
  AND prior_actives >= 100   -- noise threshold: exclude micro-segments
  AND total_churn   >= 5     -- noise threshold: exclude single-digit churn
ORDER BY
    total_churn    DESC NULLS LAST,   -- 1st: absolute impact (most merchants lost)
    churn_rate_pct DESC NULLS LAST    -- 2nd: relative severity
LIMIT 15;
-- NOTE: If top result shows churn_rate < 5%, verify sort direction and NULL filtering.
-- Real large-segment churn rates in this dataset are typically 10–100%.
"""


def get_system_prompt(question: str = "") -> str:
    """Returns the full RAG-enriched system prompt for the LLM."""
    return f"""You are TPV Insight Pro, a senior payments analytics expert and SQL engineer.
You have deep knowledge of the payments business framework and strict data governance rules below.
Your job: convert natural language questions into precise, governance-compliant DuckDB SQL queries.

{TPV_DRIVER_TREE}

{DATA_DICTIONARY}

{SQL_RULES}

{SQL_TEMPLATES}

=== YOUR RESPONSE FORMAT ===

1. THOUGHT: Briefly explain your approach and which governance rules apply (1-3 sentences).

2. SQL:
```sql
-- your DuckDB SQL here — always enforce RULE 1 (anti-double-count)
```

3. Leave INTERPRETATION blank — it will be filled after query execution.

CRITICAL RULES SUMMARY:
• Rule 1:  Never aggregate across all segments → always use Aggregate anchor for totals
• Rule 5:  Always PARTITION BY "Segment Name", Segment (both) → prevents phantom growth rates
• Rule 6:  Only divide child metrics by their direct parent → prevents >100% share ratios
• Rule 9:  Self-check: growth >500% or share >100% = bug, not a business result
• Rule 11: Never report the top-ordered row as an average → use intent-to-segment lookup or weighted avg
• Rule 12: Never AVG() pre-computed per-active columns naively → use weighted average by actives
"""


def get_result_interpretation_prompt(question: str, sql: str,
                                      results: str,
                                      validation_note: str = "") -> str:
    """Prompt to interpret query results in business context."""
    validation_block = ""
    if validation_note:
        validation_block = f"""
⚠ POST-EXECUTION VALIDATION NOTE (read before interpreting):
{validation_note}
The SQL was automatically regenerated to fix the above issue.
The results below reflect the corrected query.
"""

    return f"""You are TPV Insight Pro, a senior payments analytics expert.
You executed a governance-compliant SQL query. Interpret the results accurately.

Original Question: {question}

SQL Executed:
{sql}
{validation_block}
Query Results:
{results}

{TPV_DRIVER_TREE}

INTERPRETATION REQUIREMENTS:
- Lead with the direct, specific answer (segment name + exact number)
- For churn questions: verify the "worst" rate is genuinely the HIGHEST in the results.
  If the top churn rate is < 5%, flag it explicitly as suspicious — real worst-case churn
  in this dataset runs 80–100% for major segments.
- Quote absolute churn COUNTS alongside rates (e.g. "87% churn rate, 392,000 merchants")
- Decompose via the TPV driver tree (L1 → L2a GNA/Resurrect/Repeat mix → L2b per-active)
- Call out mix-shift signals where relevant
- Flag governance anomalies (small denominators < 50, unclassified segments, data gaps)
- Suggest 1-2 precise follow-up questions the data can answer
"""

