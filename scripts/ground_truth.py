"""
ground_truth.py
───────────────
Verified ground-truth register for all 15 business questions.
Numbers confirmed against the raw dataset (Aggregate row, Feb 2026 unless stated).

verified_answer:
  - float/int  → single numeric result to compare against
  - dict       → multiple named values to compare
  - None       → directional/qualitative — scored by manual review

tolerance_pct: acceptable % deviation from verified_answer (default 5%)
required_sql_filters: strings that MUST appear in the generated SQL
forbidden_sql_patterns: regex patterns that must NOT appear (governance checks)
required_segments: child Segment values that must be selected (not Segment Name)
forbidden_segments: segments that would indicate the Peak Value Trap
"""

GROUND_TRUTH = [

    # ── RETRIEVAL QUESTIONS ────────────────────────────────────────────────────

    {
        "id": "Q1",
        "question": (
            "How did our total ecosystem payment volume perform this past week "
            "compared to the same week last month?"
        ),
        "verified_answer": None,          # directional — trend comparison
        "tolerance_pct": 5,
        "required_sql_filters": [
            "Time Period",
            "Week",
            "Segment Name",
            "Aggregate",
        ],
        "forbidden_sql_patterns": [
            r'PARTITION BY\s+"Segment Name"(?!\s*,\s*Segment)',
        ],
        "notes": "Must use Week grain. Must anchor to Aggregate. Compare latest week vs prior week.",
    },

    {
        "id": "Q2",
        "question": (
            "What was our total active customer base last month, and how does "
            "that compare to the same month in previous years?"
        ),
        "verified_answer": 594045,        # Feb 2026, Aggregate row
        "tolerance_pct": 1,               # tight — exact row read, no calculation
        "required_sql_filters": [
            "Segment Name",
            "Aggregate",
            "All Segments",
            "Time Period",
            "Month",
        ],
        "forbidden_sql_patterns": [
            r'SUM\(.*[Aa]ctive.*\)',       # summing actives = double-count
            r'PARTITION BY\s+"Segment Name"(?!\s*,\s*Segment)',
        ],
        "notes": (
            "Verified: Feb 2026 = 594,045. "
            "Common failure: summing all segment rows → ~2.43M (4x overcounting). "
            "YoY: Feb 2025 = 530,475 (+12.0% YoY), Feb 2024 = 450,422 (+17.8% prior year)."
        ),
    },

    {
        "id": "Q3",
        "question": (
            "Which customer segment had the worst churn rate last month, "
            "and how does it compare to the same period last year?"
        ),
        "verified_answer": None,          # segment identity varies — directional
        "tolerance_pct": 5,
        "required_sql_filters": ["Time Period", "Month"],
        "required_columns": [
            "active_cohorted_churn",
            "passive_cohorted_churn",
        ],
        "forbidden_sql_patterns": [
            r'PARTITION BY\s+"Segment Name"(?!\s*,\s*Segment)',
        ],
        "notes": (
            "Must compute total_churn = active + passive cohorted churn. "
            "Must use _py columns for YoY, not self-join. "
            "Must exclude 'All Segments' row when ranking segments."
        ),
    },

    {
        "id": "Q4",
        "question": (
            "Were there any weeks in the last quarter where we saw a noticeable "
            "spike in customers lapsing? Who was most affected?"
        ),
        "verified_answer": None,          # pattern detection — no single number
        "tolerance_pct": 10,
        "required_sql_filters": ["Time Period", "Week"],
        "required_columns": [
            "active_cohorted_churn",
            "passive_cohorted_churn",
        ],
        "notes": (
            "'31d Active Lapse Absolute' is ALL ZEROS — must use churn columns instead. "
            "Last quarter = last ~13 weekly rows. "
            "Spike detection: compare each week vs trailing average."
        ),
    },

    {
        "id": "Q5",
        "question": (
            "On average, how much are our card-only customers paying per invoice "
            "compared to customers who use a mix of payment methods?"
        ),
        "verified_answer": {
            "CC 100%":            1233.96,
            "Bank & Card Blended": 1557.59,
        },
        "tolerance_pct": 5,
        "required_sql_filters": [
            "Bank Transfer & Card Distribution",
            "Time Period",
            "Month",
        ],
        "required_segments": ["CC 100%", "Bank & Card Blended"],
        "forbidden_segments": [
            "100% SR TPV",    # high-ticket outlier (~$10,139) — not representative of mixed
            "MAIP 100%",      # managed-pay sub-segment — not card-only
        ],
        "required_columns": ["Avg Paid Invoice Amt"],
        "notes": (
            "Correct answer: CC 100% = $1,233.96, Bank & Card Blended = $1,557.59. "
            "Mixed pays 26.2% more per invoice. "
            "Common failure: selecting '100% SR TPV' ($10,139) as 'mixed' — that is an outlier. "
            "Common failure: selecting 'MAIP 100%' ($5,770) as 'card-only' — wrong category."
        ),
    },

    # ── ANALYSIS QUESTIONS ─────────────────────────────────────────────────────

    {
        "id": "Q6",
        "question": (
            "Looking at weekly data over the last quarter, which segments are "
            "driving new customer additions — and is the mix shifting toward "
            "higher-value or lower-value customers?"
        ),
        "verified_answer": None,          # trend analysis — directional
        "tolerance_pct": 10,
        "required_sql_filters": ["Time Period", "Week"],
        "required_columns": [
            "31d Active GNA Absolute",
            "Eco Actives 31d Absolute",
        ],
        "forbidden_sql_patterns": [
            r'PARTITION BY\s+"Segment Name"(?!\s*,\s*Segment)',
        ],
        "notes": (
            "GNA Rate = '31d Active GNA Absolute' / 'Eco Actives 31d Absolute'. "
            "Higher-value = higher TPV per Active. "
            "Must partition by BOTH Segment Name AND Segment for window functions."
        ),
    },

    {
        "id": "Q7",
        "question": (
            "Month over month, which segment is growing its connected payment "
            "volume the fastest — and is that growth coming from recurring "
            "invoices or one-time payments?"
        ),
        "verified_answer": None,          # directional — segment identity varies
        "tolerance_pct": 10,
        "required_sql_filters": ["Time Period", "Month"],
        "required_columns": [
            "Eco CP Amount 31d Absolute",
            "Eco Recurring Invoice Amount 31d Absolute",
            "Eco Non-Recurring Invoice Amount 31d Absolute",
            "Eco Invoice Amount 31d Absolute",
        ],
        "forbidden_sql_patterns": [
            r'PARTITION BY\s+"Segment Name"(?!\s*,\s*Segment)',                      # granularity trap
            r'"Eco (?:Non-)?Recurring Invoice.*?/.*?"Eco CP Amount',                 # wrong parent
        ],
        "notes": (
            "CRITICAL: must PARTITION BY Segment Name, Segment — not just Segment Name. "
            "Recurring/non-recurring shares must divide by 'Eco Invoice Amount' (correct parent), "
            "NOT 'Eco CP Amount' (wrong parent → >100% ratio). "
            "Any growth rate >500% = granularity trap artifact, not a real result. "
            "Verified real growth rates for Managed Pay sub-segments: 16–20% MoM range."
        ),
    },

    {
        "id": "Q8",
        "question": (
            "How has the average invoice size evolved over the last 12 months? "
            "Are our merchants moving upmarket, or are smaller merchants "
            "starting to dominate the mix?"
        ),
        "verified_answer": None,          # trend — directional
        "tolerance_pct": 5,
        "required_sql_filters": [
            "Time Period", "Month",
            "Aggregate", "All Segments",
        ],
        "required_columns": ["Avg Paid Invoice Amt"],
        "notes": (
            "Use Aggregate row for overall trend. "
            "12-month window = last 12 monthly rows by date_end. "
            "Upmarket = rising avg invoice. Mix-shift down = rising GNA of smaller merchants."
        ),
    },

    {
        "id": "Q9",
        "question": (
            "On a weekly basis, are we winning back lapsed customers fast enough "
            "to offset the ones we're losing? How does the resurrection-to-lapse "
            "ratio look across segments?"
        ),
        "verified_answer": None,          # ratio trend — directional
        "tolerance_pct": 5,
        "required_sql_filters": ["Time Period", "Week"],
        "required_columns": [
            "31d Active Resurrect Absolute",
            "active_cohorted_churn",
            "passive_cohorted_churn",
        ],
        "notes": (
            "'31d Active Lapse Absolute' = all zeros — must use churn columns for lapse proxy. "
            "Resurrection-to-Lapse = '31d Active Resurrect' / (active_churn + passive_churn). "
            "Ratio > 1 = winning back more than losing."
        ),
    },

    {
        "id": "Q10",
        "question": (
            "Are our high-value customers churning at a different rate than "
            "non-high-value ones? Has that gap been widening or narrowing "
            "over the last six months?"
        ),
        "verified_answer": None,          # gap trend — directional
        "tolerance_pct": 5,
        "required_sql_filters": [
            "Time Period", "Month",
            "Aggregate", "All Segments",
        ],
        "required_columns": [
            "hvc_cohorted_churn",
            "non_hvc_cohorted_churn",
            "hvc_cohorted_churn_py",
            "non_hvc_cohorted_churn_py",
        ],
        "notes": (
            "6-month window: date_end >= MAX(date_end) - INTERVAL '6 months'. "
            "Use _py columns for YoY. "
            "HVC share = hvc_churn / (hvc_churn + non_hvc_churn)."
        ),
    },

    # ── INSIGHT QUESTIONS ──────────────────────────────────────────────────────

    {
        "id": "Q11",
        "question": (
            "Break down our churn over FY24 through FY26 into active vs passive "
            "components on a monthly basis. Is one type improving while the other "
            "is getting worse?"
        ),
        "verified_answer": None,          # multi-year trend — directional
        "tolerance_pct": 5,
        "required_sql_filters": [
            "Time Period", "Month",
            "Aggregate", "All Segments",
        ],
        "required_columns": [
            "active_cohorted_churn",
            "passive_cohorted_churn",
            "active_cohorted_churn_py",
            "passive_cohorted_churn_py",
            "FY",
        ],
        "notes": (
            "Must cover FY24, FY25, FY26. "
            "YoY delta: use _py columns, not self-join. "
            "Active churn = deliberate cancellation. Passive = stopped transacting."
        ),
    },

    {
        "id": "Q12",
        "question": (
            "Across our tenure-based cohorts, are the new customers we acquired "
            "in FY25 ramping their payment volume faster or slower than FY24 "
            "cohorts at the same stage?"
        ),
        "verified_answer": None,          # cohort comparison — directional
        "tolerance_pct": 10,
        "required_sql_filters": [
            "Time Period", "Month",
            "Tenure Cohort",
        ],
        "required_columns": [
            "Eco TPV 31d Absolute",
            "Eco Actives 31d Absolute",
            "FY",
        ],
        "notes": (
            "Segment Name = 'Tenure Cohort'. "
            "Compare TPV per Active at equivalent months-since-acquisition. "
            "FY25 cohort = merchants who became active in FY25."
        ),
    },

    {
        "id": "Q13",
        "question": (
            "If you built a health scorecard for each segment today — factoring "
            "in volume per active user, repeat usage rates, invoice sizes, and "
            "churn — which segments are thriving and which ones need immediate "
            "attention? What would you recommend for the weakest ones?"
        ),
        "verified_answer": None,          # composite scorecard — qualitative
        "tolerance_pct": 10,
        "required_columns": [
            "Eco TPV 31d Absolute",
            "Eco Actives 31d Absolute",
            "31d Active Repeat Absolute",
            "Avg Paid Invoice Amt",
            "active_cohorted_churn",
            "passive_cohorted_churn",
        ],
        "forbidden_segments": ["All Segments"],
        "notes": (
            "Should return one row per child Segment. "
            "Must NOT include 'All Segments' row (double-count). "
            "Scoring: TPV/Active, Repeat Rate, Avg Invoice, Total Churn. "
            "Weak = low repeat rate + high churn + low TPV per active."
        ),
    },

    {
        "id": "Q14",
        "question": (
            "Looking at weekly activity patterns across our payment-type segments, "
            "which payment mix shows the earliest warning signs before a customer "
            "disengages? How many weeks of declining activity typically precede "
            "a churn event?"
        ),
        "verified_answer": None,          # pattern detection — qualitative
        "tolerance_pct": 10,
        "required_sql_filters": ["Time Period", "Week"],
        "required_columns": [
            "Eco TPV 31d Absolute",
            "Eco Txns 31d Absolute",
            "active_cohorted_churn",
            "passive_cohorted_churn",
        ],
        "forbidden_sql_patterns": [
            r'PARTITION BY\s+"Segment Name"(?!\s*,\s*Segment)',
        ],
        "notes": (
            "Leading indicator = declining Txns per Active before churn spike. "
            "Must PARTITION BY Segment Name, Segment for LAG calculations. "
            "Look for 2–4 week lag between volume decline and churn event."
        ),
    },

    {
        "id": "Q15",
        "question": (
            "Looking at our full segment portfolio on a monthly basis, where does "
            "each segment sit on a growth-vs-retention matrix? Which ones have "
            "strong volume growth but poor repeat rates, and which have great "
            "retention but flat growth — and what's the right playbook for each quadrant?"
        ),
        "verified_answer": None,          # matrix classification — qualitative
        "tolerance_pct": 10,
        "required_sql_filters": ["Time Period", "Month"],
        "required_columns": [
            "Eco TPV 31d Absolute",
            "31d Active Repeat Absolute",
            "Eco Actives 31d Absolute",
        ],
        "forbidden_sql_patterns": [
            r'PARTITION BY\s+"Segment Name"(?!\s*,\s*Segment)',  # granularity trap
        ],
        "forbidden_segments": ["All Segments"],
        "notes": (
            "Quadrants: Strong Growth+Retention / Strong Growth Poor Retention / "
            "Flat Growth Great Retention / Needs Attention. "
            "Growth = MoM TPV change. Retention = Repeat Rate >= 50%. "
            "Must PARTITION BY Segment Name, Segment for LAG."
        ),
    },
]
