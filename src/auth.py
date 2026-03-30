"""
auth.py
───────
Authentication module for TPV Insight Pro.

Features:
  - Email + password login with bcrypt hashing
  - Google SSO via OAuth2
  - Login logs stored in SQLite (data/tpv_app.db)
  - User management (create, verify, SSO upsert)

Backend: SQLite at data/tpv_app.db
"""

import os
import sqlite3
import hashlib
import secrets
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── bcrypt (preferred) with SHA-256 fallback ──────────────────────────────────
try:
    import bcrypt
    _USE_BCRYPT = True
except ImportError:
    _USE_BCRYPT = False

# ── Google OAuth ──────────────────────────────────────────────────────────────
try:
    from requests_oauthlib import OAuth2Session
    _GOOGLE_SSO_AVAILABLE = True
except ImportError:
    _GOOGLE_SSO_AVAILABLE = False

# ── Paths & Google config ─────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent.parent / "data" / "tpv_app.db"

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.getenv("APP_BASE_URL", "http://localhost:8501") + "/"
GOOGLE_AUTH_URL      = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL     = "https://accounts.google.com/o/oauth2/token"
GOOGLE_USERINFO_URL  = "https://www.googleapis.com/oauth2/v1/userinfo"
GOOGLE_SCOPE         = ["openid", "email", "profile"]

GOOGLE_SSO_CONFIGURED = bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET
                              and _GOOGLE_SSO_AVAILABLE)


# ── Database setup ────────────────────────────────────────────────────────────

def init_db():
    """Create all auth tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            email         TEXT UNIQUE NOT NULL,
            name          TEXT NOT NULL,
            password_hash TEXT,
            auth_method   TEXT DEFAULT 'email',
            google_id     TEXT,
            avatar_url    TEXT,
            created_at    TEXT DEFAULT (datetime('now')),
            last_login    TEXT,
            is_active     INTEGER DEFAULT 1
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS login_logs (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            email          TEXT NOT NULL,
            method         TEXT NOT NULL,
            success        INTEGER NOT NULL,
            ip_address     TEXT,
            user_agent     TEXT,
            failure_reason TEXT,
            timestamp      TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    conn.close()


# ── Password helpers ──────────────────────────────────────────────────────────

def _hash_password(password: str) -> str:
    if _USE_BCRYPT:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    salt = secrets.token_hex(16)
    hashed = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"sha256:{salt}:{hashed}"


def _verify_password(password: str, stored: str) -> bool:
    if _USE_BCRYPT and not stored.startswith("sha256:"):
        try:
            return bcrypt.checkpw(password.encode(), stored.encode())
        except Exception:
            return False
    if stored.startswith("sha256:"):
        _, salt, hashed = stored.split(":", 2)
        return hashlib.sha256((salt + password).encode()).hexdigest() == hashed
    return False


# ── User operations ───────────────────────────────────────────────────────────

def create_user(email: str, password: str, name: str) -> tuple[bool, str]:
    """Register a new email/password user. Returns (success, message)."""
    init_db()
    if len(password) < 6:
        return False, "Password must be at least 6 characters."
    if "@" not in email or "." not in email:
        return False, "Please enter a valid email address."
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO users (email, name, password_hash, auth_method) VALUES (?,?,?,'email')",
            (email.lower().strip(), name.strip(), _hash_password(password))
        )
        conn.commit()
        conn.close()
        return True, "Account created! You can now sign in."
    except sqlite3.IntegrityError:
        return False, "An account with this email already exists."
    except Exception as e:
        return False, f"Error: {e}"


def verify_login(email: str, password: str) -> tuple[bool, Optional[dict], str]:
    """
    Verify email/password. Returns (success, user_dict | None, message).
    """
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, email, name, password_hash, is_active "
        "FROM users WHERE email = ? AND auth_method = 'email'",
        (email.lower().strip(),)
    )
    row = c.fetchone()

    if not row:
        conn.close()
        log_login(email, "email_password", False, failure_reason="User not found")
        return False, None, "Invalid email or password."

    uid, uemail, name, pw_hash, active = row

    if not active:
        conn.close()
        log_login(email, "email_password", False, failure_reason="Disabled")
        return False, None, "Account disabled. Contact support."

    if not _verify_password(password, pw_hash):
        conn.close()
        log_login(email, "email_password", False, failure_reason="Wrong password")
        return False, None, "Invalid email or password."

    conn.execute("UPDATE users SET last_login = datetime('now') WHERE id = ?", (uid,))
    conn.commit()
    conn.close()
    log_login(uemail, "email_password", True)
    return True, {"id": uid, "email": uemail, "name": name, "method": "email"}, "Welcome back!"


def upsert_google_user(google_id: str, email: str, name: str,
                       avatar_url: str = None) -> dict:
    """Create or update a Google SSO user. Returns user dict."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE email = ?", (email.lower(),))
    existing = c.fetchone()

    if existing:
        c.execute(
            "UPDATE users SET google_id=?, name=?, avatar_url=?, "
            "last_login=datetime('now'), auth_method='google' WHERE email=?",
            (google_id, name, avatar_url, email.lower())
        )
    else:
        c.execute(
            "INSERT INTO users (email, name, google_id, avatar_url, auth_method) "
            "VALUES (?,?,?,?,'google')",
            (email.lower(), name, google_id, avatar_url)
        )

    conn.commit()
    c.execute("SELECT id, email, name, avatar_url FROM users WHERE email=?", (email.lower(),))
    row = c.fetchone()
    conn.close()

    log_login(email, "google_sso", True)
    return {"id": row[0], "email": row[1], "name": row[2],
            "avatar_url": row[3], "method": "google"}


# ── Login log ─────────────────────────────────────────────────────────────────

def log_login(email: str, method: str, success: bool,
              ip_address: str = None, user_agent: str = None,
              failure_reason: str = None):
    """Record every login attempt."""
    init_db()
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO login_logs "
            "(email, method, success, ip_address, user_agent, failure_reason) "
            "VALUES (?,?,?,?,?,?)",
            (email, method, 1 if success else 0,
             ip_address, user_agent, failure_reason)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_login_logs(limit: int = 200) -> list[dict]:
    """Return recent login log rows as dicts."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT email, method, success, ip_address, timestamp, failure_reason
        FROM login_logs ORDER BY timestamp DESC LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


# ── Google OAuth ──────────────────────────────────────────────────────────────

def get_google_auth_url(state_token: str) -> Optional[str]:
    """Build the Google OAuth authorization URL."""
    if not GOOGLE_SSO_CONFIGURED:
        return None
    try:
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"
        oauth = OAuth2Session(
            GOOGLE_CLIENT_ID,
            redirect_uri=GOOGLE_REDIRECT_URI,
            scope=GOOGLE_SCOPE,
            state=state_token,
        )
        url, _ = oauth.authorization_url(
            GOOGLE_AUTH_URL, access_type="offline", prompt="select_account"
        )
        return url
    except Exception:
        return None


def handle_google_callback(code: str, state: str) -> tuple[bool, Optional[dict], str]:
    """Exchange OAuth code for user info. Returns (success, user | None, msg)."""
    if not GOOGLE_SSO_CONFIGURED:
        return False, None, "Google SSO not configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET."
    try:
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"
        oauth = OAuth2Session(
            GOOGLE_CLIENT_ID,
            redirect_uri=GOOGLE_REDIRECT_URI,
            scope=GOOGLE_SCOPE,
            state=state,
        )
        oauth.fetch_token(
            GOOGLE_TOKEN_URL, code=code, client_secret=GOOGLE_CLIENT_SECRET
        )
        info = oauth.get(GOOGLE_USERINFO_URL).json()
        user = upsert_google_user(
            google_id=info.get("id", ""),
            email=info.get("email", ""),
            name=info.get("name", info.get("email", "User")),
            avatar_url=info.get("picture"),
        )
        return True, user, "Google login successful."
    except Exception as e:
        log_login("unknown", "google_sso", False, failure_reason=str(e))
        return False, None, f"Google login failed: {e}"
