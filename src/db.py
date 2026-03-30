"""
db.py
─────
DuckDB engine that loads payments.csv once and serves all queries.
DuckDB reads the CSV directly — no ETL, no database file needed.
"""

import os
import duckdb
import pandas as pd
from pathlib import Path

# Resolve data path relative to this file
DATA_PATH = Path(__file__).parent.parent / "data" / "payments.csv"


class PaymentsDB:
    """
    Singleton DuckDB connection with the payments table pre-loaded.
    Thread-safe enough for Streamlit's single-user model.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.con = duckdb.connect(database=":memory:")
        self._load_data()
        self._initialized = True

    def _load_data(self):
        """Load CSV into DuckDB as a persistent in-memory view."""
        if not DATA_PATH.exists():
            raise FileNotFoundError(
                f"payments.csv not found at {DATA_PATH}\n"
                "Place your CSV at: tpv-insight-pro/data/payments.csv"
            )
        # Load CSV — treat literal 'null' strings as NULL, infer numeric types
        self.con.execute(f"""
            CREATE OR REPLACE TABLE payments AS
            SELECT * FROM read_csv_auto(
                '{DATA_PATH}',
                ALL_VARCHAR=FALSE,
                NORMALIZE_NAMES=FALSE,
                nullstr=['null', 'NULL', 'None', 'NA', 'N/A', '']
            )
        """)
        # Quick sanity check
        count = self.con.execute("SELECT COUNT(*) FROM payments").fetchone()[0]
        print(f"[DB] Loaded {count:,} rows from {DATA_PATH.name}")

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute SQL and return a DataFrame.
        Raises an exception on SQL errors (caught upstream by agent).
        """
        return self.con.execute(sql).df()

    def schema_info(self) -> str:
        """Return column names and types as a formatted string."""
        result = self.con.execute("DESCRIBE payments").df()
        lines = [f"  {row['column_name']} ({row['column_type']})" for _, row in result.iterrows()]
        return "Table: payments\n" + "\n".join(lines)

    def sample(self, n: int = 3) -> pd.DataFrame:
        """Return n sample rows for debugging."""
        return self.con.execute(f"SELECT * FROM payments LIMIT {n}").df()

    def get_segments(self) -> list:
        """Return distinct segment values."""
        return self.con.execute(
            "SELECT DISTINCT Segment FROM payments ORDER BY Segment"
        ).df()["Segment"].tolist()

    def get_date_range(self) -> dict:
        """Return min/max dates and available fiscal years."""
        row = self.con.execute("""
            SELECT
                MIN(date_end)::VARCHAR AS min_date,
                MAX(date_end)::VARCHAR AS max_date,
                LIST(DISTINCT FY ORDER BY FY) AS fiscal_years
            FROM payments
        """).fetchone()
        return {"min_date": row[0], "max_date": row[1], "fiscal_years": row[2]}


# Module-level singleton
db = PaymentsDB()
