"""
scheduler.py
────────────
Scheduled Reports module for TPV Insight Pro.

Features:
  - Create, pause, resume, delete scheduled reports
  - Schedules: hourly, daily, weekly, monthly
  - Runs due reports in a background thread on page load
  - Results stored in SQLite

Backend: SQLite at data/tpv_app.db
"""

import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent.parent / "data" / "tpv_app.db"

SCHEDULE_LABELS = {
    "hourly":  "Every Hour",
    "daily":   "Every Day",
    "weekly":  "Every Week",
    "monthly": "Every Month",
}

DAY_OPTIONS = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday"
]


# ── Database setup ────────────────────────────────────────────────────────────

def init_scheduler_db():
    """Create scheduled_reports table if it doesn't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_reports (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email      TEXT NOT NULL,
            name            TEXT NOT NULL,
            prompt          TEXT NOT NULL,
            schedule_type   TEXT NOT NULL,
            schedule_time   TEXT DEFAULT '08:00',
            schedule_day    TEXT DEFAULT 'Monday',
            status          TEXT DEFAULT 'active',
            next_run        TEXT NOT NULL,
            last_run        TEXT,
            last_result     TEXT,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


# ── Next-run calculator ───────────────────────────────────────────────────────

def _next_run(schedule_type: str, schedule_time: str,
              schedule_day: str) -> str:
    """Calculate the next run timestamp from now."""
    now   = datetime.now()
    parts = schedule_time.split(":") if ":" in schedule_time else ["8", "0"]
    hour, minute = int(parts[0]), int(parts[1])

    if schedule_type == "hourly":
        t = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    elif schedule_type == "daily":
        t = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if t <= now:
            t += timedelta(days=1)

    elif schedule_type == "weekly":
        day_map = {d.lower(): i for i, d in enumerate(
            ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"])}
        target = day_map.get(schedule_day.lower(), 0)
        ahead  = target - now.weekday()
        if ahead < 0 or (ahead == 0 and now.hour >= hour):
            ahead += 7
        t = (now + timedelta(days=ahead)).replace(
            hour=hour, minute=minute, second=0, microsecond=0)

    elif schedule_type == "monthly":
        try:
            day = min(int(schedule_day), 28)
        except ValueError:
            day = 1
        t = now.replace(day=day, hour=hour, minute=minute, second=0, microsecond=0)
        if t <= now:
            m = now.month + 1 if now.month < 12 else 1
            y = now.year if now.month < 12 else now.year + 1
            t = t.replace(year=y, month=m)
    else:
        t = now + timedelta(days=1)

    return t.strftime("%Y-%m-%d %H:%M:%S")


# ── CRUD operations ───────────────────────────────────────────────────────────

def create_report(user_email: str, name: str, prompt: str,
                  schedule_type: str, schedule_time: str = "08:00",
                  schedule_day: str = "Monday") -> tuple[bool, str]:
    """Create a new scheduled report. Returns (success, message)."""
    init_scheduler_db()
    if not name.strip():
        return False, "Report name cannot be empty."
    if not prompt.strip():
        return False, "Prompt cannot be empty."

    try:
        nr = _next_run(schedule_type, schedule_time, schedule_day)
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            INSERT INTO scheduled_reports
            (user_email, name, prompt, schedule_type, schedule_time, schedule_day, next_run)
            VALUES (?,?,?,?,?,?,?)
        """, (user_email, name.strip(), prompt.strip(),
              schedule_type, schedule_time, schedule_day, nr))
        conn.commit()
        conn.close()
        return True, f"✅ **{name}** scheduled. Next run: **{nr}**"
    except Exception as e:
        return False, f"Error: {e}"


def get_user_reports(user_email: str) -> list[dict]:
    """Return all scheduled reports for a user, newest first."""
    init_scheduler_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT * FROM scheduled_reports
        WHERE user_email = ?
        ORDER BY created_at DESC
    """, (user_email,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def pause_report(report_id: int, user_email: str) -> tuple[bool, str]:
    """Pause an active report."""
    init_scheduler_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "UPDATE scheduled_reports SET status='paused' WHERE id=? AND user_email=?",
        (report_id, user_email)
    )
    ok = c.rowcount > 0
    conn.commit()
    conn.close()
    return (True, "Report paused.") if ok else (False, "Report not found.")


def resume_report(report_id: int, user_email: str) -> tuple[bool, str]:
    """Resume a paused report and recalculate next_run."""
    init_scheduler_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM scheduled_reports WHERE id=? AND user_email=?",
              (report_id, user_email))
    row = dict(c.fetchone() or {})
    if not row:
        conn.close()
        return False, "Report not found."
    nr = _next_run(row["schedule_type"], row["schedule_time"], row["schedule_day"])
    c.execute(
        "UPDATE scheduled_reports SET status='active', next_run=? WHERE id=?",
        (nr, report_id)
    )
    conn.commit()
    conn.close()
    return True, f"Report resumed. Next run: **{nr}**"


def delete_report(report_id: int, user_email: str) -> tuple[bool, str]:
    """Permanently delete a report."""
    init_scheduler_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "DELETE FROM scheduled_reports WHERE id=? AND user_email=?",
        (report_id, user_email)
    )
    ok = c.rowcount > 0
    conn.commit()
    conn.close()
    return (True, "Report deleted.") if ok else (False, "Report not found.")


def run_now(report_id: int, user_email: str, agent) -> tuple[bool, str]:
    """Manually trigger a report immediately."""
    init_scheduler_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM scheduled_reports WHERE id=? AND user_email=?",
              (report_id, user_email))
    row = dict(c.fetchone() or {})
    conn.close()
    if not row:
        return False, "Report not found."
    try:
        resp   = agent.run(row["prompt"])
        result = resp.interpretation or resp.error or "No result generated."
        _save_result(report_id, result, row["schedule_type"],
                     row["schedule_time"], row["schedule_day"])
        return True, result
    except Exception as e:
        return False, f"Error running report: {e}"


# ── Background execution ──────────────────────────────────────────────────────

def _due_reports() -> list[dict]:
    """Return active reports whose next_run has passed."""
    init_scheduler_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT * FROM scheduled_reports
        WHERE status = 'active' AND next_run <= ?
        ORDER BY next_run ASC
    """, (now,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def _save_result(report_id: int, result: str,
                 schedule_type: str, schedule_time: str, schedule_day: str):
    nr = _next_run(schedule_type, schedule_time, schedule_day)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        UPDATE scheduled_reports
        SET last_run = datetime('now'), last_result = ?, next_run = ?
        WHERE id = ?
    """, (result[:3000], nr, report_id))
    conn.commit()
    conn.close()


def check_and_run_due(agent):
    """
    Called on every page load. Runs any due reports in a background thread.
    Returns the number of reports triggered.
    """
    due = _due_reports()
    if not due:
        return 0

    def _worker():
        for r in due:
            try:
                resp   = agent.run(r["prompt"])
                result = resp.interpretation or resp.error or "No result."
            except Exception as e:
                result = f"Error: {e}"
            _save_result(r["id"], result, r["schedule_type"],
                         r["schedule_time"], r["schedule_day"])

    threading.Thread(target=_worker, daemon=True).start()
    return len(due)
