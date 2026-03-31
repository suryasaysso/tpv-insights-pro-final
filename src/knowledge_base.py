"""
Optimized Knowledge Base for TPV Insight Pro
Token-efficient version (~60% reduction)
Focus: correctness > verbosity
"""

# =========================
# CORE BUSINESS FRAMEWORK
# =========================
TPV_DRIVER_TREE = """
TPV = Actives × TPV per Active

Actives = GNA + Resurrect + Repeat
TPV per Active = Txns per Active × Avg Txn Size

Key Insight:
- More GNA → lowers TPV/Active
- More Repeat → increases TPV/Active
"""

# =========================
# CRITICAL GOVERNANCE RULES
# =========================
CORE_RULES = """
1. ANTI-DOUBLE COUNT:
   ALWAYS use:
   "Segment Name"='Aggregate' AND Segment='All Segments' for totals

2. TIME GRAIN:
   Filter "Time Period" = 'Month' OR 'Week' (never mix)

3. LAST PERIOD:
   date_end = MAX(date_end) for that grain

4. PARTITION RULE:
   ALWAYS:
   PARTITION BY "Segment Name", Segment
   (never just one)

5. CHURN:
   total_churn = active_cohorted_churn + passive_cohorted_churn

   churn_rate = total_churn / LAG(actives)

   RULES:
   - denominator = PRIOR actives
   - exclude NULL churn
   - filter prior_actives >= 50

6. SHARE RULE:
   Only divide child by correct parent
   Example:
   recurring / invoice (✔)
   recurring / CP (✘)

7. PRE-COMPUTED METRICS:
   NEVER AVG("Avg Paid Invoice Amt")
   → use weighted avg if needed

8. DATE MATH:
   ALWAYS use INTERVAL. Example: date_end - INTERVAL '12 months'

9. IDENTIFIERS:
   Never use hyphens in aliases (e.g., use churn_rate, NOT churn-rate)
"""

# =========================
# SEGMENT MAPPING (CRITICAL)
# =========================
SEGMENT_LOOKUP = """
card-only        → 'CC 100%'
mixed            → 'Bank & Card Blended'
bank-only        → 'ACH 100%'
new merchants    → 'New (0-12 months)'
mature           → 'Mature (25+ months)'
invoice          → 'Invoice Only'
sales receipt    → '100% SR TPV' (outlier)
"""

# =========================
# ESSENTIAL COLUMN MAP
# =========================
COLUMN_MAP = """
Actives: "Eco Actives 31d Absolute"
TPV:     "Eco TPV 31d Absolute"
Txns:    "Eco Txns 31d Absolute"

Churn:
- active_cohorted_churn
- passive_cohorted_churn

Lifecycle:
- "31d Active GNA Absolute"
- "31d Active Resurrect Absolute"
- "31d Active Repeat Absolute"
"""

# =========================
# MINIMAL SQL TEMPLATES
# =========================
SQL_TEMPLATES = """
-- Latest Month
SELECT MAX(date_end)
FROM payments
WHERE "Time Period"='Month'
  AND "Segment Name"='Aggregate';

-- TPV per Active
SELECT
    date_end,
    "Eco TPV 31d Absolute" / NULLIF("Eco Actives 31d Absolute",0) AS tpv_per_active
FROM payments
WHERE "Segment Name"='Aggregate'
  AND Segment='All Segments'
  AND "Time Period"='Month';

-- Churn Rate (Correct)
WITH base AS (
  SELECT
    "Segment Name",
    Segment,
    date_end,
    COALESCE(active_cohorted_churn,0)+COALESCE(passive_cohorted_churn,0) AS churn,
    LAG("Eco Actives 31d Absolute")
      OVER (PARTITION BY "Segment Name", Segment ORDER BY date_end) AS prior_actives
  FROM payments
  WHERE "Time Period"='Month'
    AND Segment!='All Segments'
)
SELECT *,
       churn / NULLIF(prior_actives,0) AS churn_rate
FROM base
WHERE prior_actives >= 50;
"""

# =========================
# SYSTEM PROMPT
# =========================
def get_system_prompt(question: str = "") -> str:
    return f"""
You are a payments analytics expert generating DuckDB SQL.

GOAL: Convert question → correct SQL

=== RULES (MANDATORY) ===
{CORE_RULES}

=== BUSINESS LOGIC ===
{TPV_DRIVER_TREE}

=== SEGMENT MAPPING ===
{SEGMENT_LOOKUP}

=== COLUMNS ===
{COLUMN_MAP}

=== SQL PATTERNS ===
{SQL_TEMPLATES}

=== OUTPUT FORMAT ===
THOUGHT:
(1-2 lines reasoning)

SQL:
```sql
-- query
CRITICAL:
Always apply Aggregate filter for totals
Always use correct partitioning
If churn: use PRIOR actives
If result >500% growth or >100% share → FIX query
"""

# =========================
# INTERPRETATION PROMPT
# =========================
def get_result_interpretation_prompt(question: str, sql: str, results: str, validation_note: str = "") -> str:
   return f"""
You are a payments analytics expert.
Question: {question}
SQL:
{sql}
Results:
{results}
{validation_note}
Instructions:
Give direct answer first
Include numbers + segment names
For churn: include BOTH count and %
Highlight key driver (GNA / Repeat / etc)
Flag anomalies if unrealistic
Suggest 1 follow-up question
"""
