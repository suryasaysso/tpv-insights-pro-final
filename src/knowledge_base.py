"""
knowledge_base.py — Optimized for Token Efficiency
──────────────────────────────────────────────────
Streamlined TPV Driver Tree + Data Dictionary + Core SQL Patterns.
Reduced verbosity to fit Groq free-tier TPM limits (8k-12k tokens).
"""

TPV_DRIVER_TREE = """
=== TPV DRIVER TREE ===
L1: TPV = Actives * TPV/Active
L2a: Actives = GNA + Resurrect + Repeat
L2b: TPV/Active = Txns/Active * Avg Txn Size

Derived Metrics:
• Volume/Active = [Volume] / NULLIF([Actives], 0)
• Txns/Active = [Transactions] / NULLIF([Actives], 0)
• Avg Txn Size = [Volume] / NULLIF([Transactions], 0)
• Churn Rate = (active_cohorted_churn + passive_cohorted_churn) / NULLIF(prior_actives, 0) * 100
  where prior_actives = LAG("Eco Actives 31d Absolute") OVER (PARTITION BY "Segment Name", Segment ORDER BY date_end)
"""

DATA_DICTIONARY = """
=== DATASET SCHEMA (Table: payments) ===
Dimensions: Segment, "Segment Name", "Time Period" (Week/Month), date_end (DATE), FY.

GOVERNANCE:
- TOTAL BIZ: WHERE "Segment Name" = 'Aggregate' AND Segment = 'All Segments'
- SEGMENTS: WHERE "Segment Name" = '...' AND Segment != 'All Segments'

Key Columns:
- TPV: "Eco TPV 31d Absolute", "CC TPV 31d Absolute", "ACH TPV 31d Absolute"
- CP: "Eco CP Amount 31d Absolute", "Eco CP Recurring Invoice Amount 31d Absolute"
- Invoice: "Eco Invoice Amount 31d Absolute", "Eco Recurring Invoice Amount 31d Absolute"
- Txns: "Eco Txns 31d Absolute", "Eco Invoice Txns 31d Absolute"
- Actives: "Eco Actives 31d Absolute", "cc_actives_31d"
- Lifecycle: "31d Active Repeat/Resurrect/GNA Absolute"
- Churn (Counts): active_cohorted_churn, passive_cohorted_churn (NULL = No data)

Representative Segments:
- 'CC 100%' (card-only), 'Bank & Card Blended' (mixed), 'ACH 100%' (bank-only)
- 'New (0-12 months)', 'Mature (25+ months)' (tenure)
"""

SQL_RULES = """
=== SQL RULES ===
1. Grain: Filter "Time Period" = 'Week' OR 'Month'. Never mix.
2. Last Period: date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period" = '...')
3. Window Fns: ALWAYS PARTITION BY "Segment Name", Segment.
4. Churn: Numerator = Counts. Denominator = LAG(actives). Worst = ORDER BY rate DESC.
5. Parents: Divide sub-metrics ONLY by their direct parent (e.g. Recurring / Invoice Total).
6. Averages: Never naively AVG() pre-computed columns; use weighted average by actives.
"""

SQL_TEMPLATES = """
=== CORE SQL PATTERNS ===

-- T1: Trend with YoY (Aggregate)
SELECT date_end, "Eco TPV 31d Absolute" as tpv,
       LAG(tpv, 12) OVER (ORDER BY date_end) as tpv_py
FROM payments WHERE "Time Period" = 'Month' AND "Segment Name" = 'Aggregate' AND Segment = 'All Segments';

-- T2: Churn Ranking (Worst)
WITH base AS (
    SELECT "Segment Name", Segment, date_end,
           (COALESCE(active_cohorted_churn,0) + COALESCE(passive_cohorted_churn,0)) as total_churn,
           LAG("Eco Actives 31d Absolute") OVER (PARTITION BY "Segment Name", Segment ORDER BY date_end) as prior_actives
    FROM payments WHERE "Time Period" = 'Month' AND Segment != 'All Segments'
)
SELECT *, ROUND(total_churn / NULLIF(prior_actives, 0) * 100, 2) as churn_rate_pct
FROM base WHERE date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period"='Month')
  AND prior_actives >= 50 AND total_churn > 0
ORDER BY churn_rate_pct DESC LIMIT 10;

-- T3: Segment Comparison (Weighted Avg)
SELECT "Segment Name",
       ROUND(SUM("Avg Paid Invoice Amt" * "Eco Actives 31d Absolute") / NULLIF(SUM("Eco Actives 31d Absolute"), 0), 2) as weighted_avg_invoice
FROM payments WHERE "Time Period" = 'Month' AND Segment != 'All Segments'
  AND date_end = (SELECT MAX(date_end) FROM payments WHERE "Time Period"='Month')
GROUP BY 1;
"""

def get_system_prompt(question: str = "") -> str:
    return f"""You are TPV Insight Pro, a payments SQL expert.
{TPV_DRIVER_TREE}
{DATA_DICTIONARY}
{SQL_RULES}
{SQL_TEMPLATES}

FORMAT:
1. THOUGHT: analytical approach (1-2 sentences).
2. SQL: ```sql ... ```
3. INTERPRETATION: Leave blank.
"""

def get_result_interpretation_prompt(question: str, sql: str, results: str, validation_note: str = "") -> str:
    return f"""Interpret these payments analytics results.
Question: {question}
SQL: {sql}
Results: {results}
{validation_note}
{TPV_DRIVER_TREE}
Lead with the direct answer. Use absolute counts and rates. Call out mix-shifts.
"""
