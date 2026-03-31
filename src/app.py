"""
app.py — TPV Insight Pro
Run: streamlit run src/app.py
"""

# Load .env BEFORE any other imports
import sys, secrets, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="TPV Insight Pro", page_icon="💳",
    layout="wide", initial_sidebar_state="expanded",
)

from auth import (
    verify_login, create_user, handle_google_callback,
    get_google_auth_url, GOOGLE_SSO_CONFIGURED,
    GOOGLE_REDIRECT_URI, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
)
from scheduler import (
    create_report, get_user_reports, pause_report,
    resume_report, delete_report, run_now,
    check_and_run_due, SCHEDULE_LABELS, DAY_OPTIONS,
)
from nl_to_sql import (
    check_groq_running, PRIMARY_MODEL, FALLBACK_MODEL,
    get_active_model, is_using_fallback,
)
from history import history_db

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600&family=DM+Mono&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.app-header {
    background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 60%, #0f3460 100%);
    padding: 1.25rem 1.75rem; border-radius: 14px;
    margin-bottom: 1.5rem; color: white;
    display: flex; align-items: center; justify-content: space-between;
}
.app-header h1 { font-size: 1.6rem; margin: 0; letter-spacing: -0.5px; }
.app-header p  { color: #94a3b8; margin: 0.15rem 0 0; font-size: 0.85rem; }
.badge { display: inline-block; padding: 2px 9px; border-radius: 999px;
         font-size: 0.72rem; font-weight: 600; }
.badge-green  { background:#d1fae5; color:#065f46; }
.badge-yellow { background:#fef3c7; color:#92400e; }
.badge-red    { background:#fee2e2; color:#991b1b; }
.badge-blue   { background:#dbeafe; color:#1e40af; }
.model-row { display:flex; align-items:center; gap:8px; padding:6px 10px;
             border-radius:8px; margin-bottom:4px; font-size:0.8rem; font-weight:500; }
.model-active  { background:#ecfdf5; color:#065f46; border:1px solid #6ee7b7; }
.model-standby { background:#f9fafb; color:#6b7280; border:1px solid #e5e7eb; }
.model-offline { background:#fef2f2; color:#991b1b; border:1px solid #fca5a5; }
.model-dot { width:7px; height:7px; border-radius:50%; flex-shrink:0; }
.dot-green { background:#10b981; }
.dot-grey  { background:#9ca3af; }
.dot-red   { background:#ef4444; }
.thought-block { background:#f5f3ff; border-left:3px solid #8b5cf6;
                 border-radius:0 6px 6px 0; padding:.5rem .75rem;
                 color:#5b21b6; font-style:italic; font-size:.88rem; }
.report-card { border:1px solid #e5e7eb; border-radius:12px;
               padding:1rem 1.25rem; margin-bottom:.75rem; background:#fafafa; }
.report-name { font-weight:600; font-size:.95rem; }
.report-meta { font-size:.78rem; color:#6b7280; margin-top:2px; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
for k, v in {"user":None,"messages":[],"agent":None,
             "oauth_state":None,"login_error":"",
             "login_toast":None}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Google OAuth callback ─────────────────────────────────────────────────────
params = st.query_params
if "code" in params and st.session_state.user is None:
    with st.spinner("Completing Google sign-in…"):
        ok, user, msg = handle_google_callback(
            params.get("code",""), params.get("state", st.session_state.oauth_state or ""))
    st.query_params.clear()
    if ok:
        st.session_state.user = user
        st.session_state.login_toast = f"Welcome, {user['name']}! 👋"
        st.rerun()
    else:
        st.session_state.login_error = msg
        st.rerun()

# ════════════════════════════════════════════════════════════════════════════
# LOGIN PAGE
# ════════════════════════════════════════════════════════════════════════════
if st.session_state.user is None:
    st.markdown("""
        <div style="max-width:440px;margin:3rem auto;padding:0 1rem">
        <div style="text-align:center;margin-bottom:2rem">
            <h1 style="font-size:2.2rem;margin:0;letter-spacing:-1px">💳 TPV Insight Pro</h1>
            <p style="color:#6b7280;margin:.4rem 0 0">Payments Analytics Agent · Powered by Groq</p>
        </div>
    """, unsafe_allow_html=True)

    if st.session_state.login_error:
        st.error(st.session_state.login_error)
        st.session_state.login_error = ""

    tab_in, tab_reg = st.tabs(["Sign In", "Create Account"])

    with tab_in:
        with st.form("login_form"):
            email = st.text_input("Email", placeholder="you@example.com")
            pwd   = st.text_input("Password", type="password")
            ok_btn = st.form_submit_button("Sign In", use_container_width=True, type="primary")
        if ok_btn:
            if not email or not pwd:
                st.error("Enter email and password.")
            else:
                ok, user, msg = verify_login(email, pwd)
                if ok:
                    st.session_state.user = user
                    st.session_state.login_toast = f"Welcome back, {user['name']}! 👋"
                    st.rerun()
                else:
                    st.error(msg)

        st.markdown('<div style="display:flex;align-items:center;gap:12px;color:#9ca3af;font-size:.82rem;margin:1.25rem 0"><div style="flex:1;height:1px;background:#e5e7eb"></div>or<div style="flex:1;height:1px;background:#e5e7eb"></div></div>', unsafe_allow_html=True)

        if GOOGLE_SSO_CONFIGURED:
            if st.button("🔵  Sign in with Google", use_container_width=True):
                state = secrets.token_urlsafe(16)
                st.session_state.oauth_state = state
                url = get_google_auth_url(state)
                if url:
                    st.markdown(f'<meta http-equiv="refresh" content="0;url={url}">', unsafe_allow_html=True)
                    st.info("Redirecting to Google…")
                else:
                    st.error("Failed to generate Google Auth URL. Check configuration.")
        else:
            st.button("🔵  Sign in with Google", disabled=True, use_container_width=True)
            st.caption("Google SSO not configured. To enable, add **GOOGLE_CLIENT_ID** and **GOOGLE_CLIENT_SECRET** to your Streamlit Secrets.")
            with st.expander("🛠 Troubleshooting SSO"):
                st.info(f"""
                - **Redirect URI:** `{GOOGLE_REDIRECT_URI}` (must match Google Cloud Console)
                - **Client ID set:** `{'✅ Yes' if GOOGLE_CLIENT_ID else '❌ No'}`
                - **Client Secret set:** `{'✅ Yes' if GOOGLE_CLIENT_SECRET else '❌ No'}`
                """)

    with tab_reg:
        with st.form("reg_form", clear_on_submit=True):
            rn = st.text_input("Full Name")
            re = st.text_input("Email")
            rp = st.text_input("Password", type="password")
            rp2= st.text_input("Confirm Password", type="password")
            rs = st.form_submit_button("Create Account", use_container_width=True, type="primary")
        if rs:
            if rp != rp2:
                st.error("Passwords do not match.")
            elif not rn or not re:
                st.error("All fields required.")
            else:
                ok, msg = create_user(re, rp, rn)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)

    st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

# ════════════════════════════════════════════════════════════════════════════
# MAIN APP
# ════════════════════════════════════════════════════════════════════════════
user = st.session_state.user

if st.session_state.login_toast:
    st.toast(st.session_state.login_toast, icon="✅")
    st.session_state.login_toast = None

if st.session_state.agent is None:
    from agent import TPVAgent
    st.session_state.agent = TPVAgent(model=PRIMARY_MODEL)
agent = st.session_state.agent

triggered = check_and_run_due(agent)

from db import db

groq_ok  = check_groq_running()
using_fb = is_using_fallback()
active_m = get_active_model()

# Header with avatar support
avatar_html = ""
if user.get("avatar_url"):
    avatar_html = f'<img src="{user["avatar_url"]}" style="width:32px;height:32px;border-radius:50%;margin-left:12px;border:1px solid #475569">'

st.markdown(f"""
<div class="app-header">
  <div>
    <h1>💳 TPV Insight Pro</h1>
    <p>Payments Analytics Agent · NL-to-SQL</p>
  </div>
  <div style="display:flex;align-items:center;text-align:right">
    <div>
        <span style="color:#cbd5e1;font-size:.82rem">Signed in as</span><br>
        <strong style="color:white;font-size:.92rem">{user['name']}</strong>
        <span style="font-size:.75rem;margin-left:6px;color:#94a3b8">
        {'🔵 Google' if user.get('method')=='google' else '📧 Email'}
        </span>
    </div>
    {avatar_html}
  </div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"### 👋 {user['name']}")
    st.caption(user['email'])
    st.divider()

    st.markdown("**🤖 Model Status**")
    if groq_ok:
        if not using_fb:
            st.markdown(f"""
                <div class="model-row model-active">
                  <div class="model-dot dot-green"></div>{PRIMARY_MODEL}
                  <span class="badge badge-blue">PRIMARY</span>
                </div>
                <div class="model-row model-standby">
                  <div class="model-dot dot-grey"></div>{FALLBACK_MODEL}
                  <span style="font-size:.7rem;color:#9ca3af">standby</span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div class="model-row model-offline">
                  <div class="model-dot dot-red"></div>{PRIMARY_MODEL}
                  <span class="badge badge-red">UNAVAILABLE</span>
                </div>
                <div class="model-row model-active">
                  <div class="model-dot dot-green"></div>{FALLBACK_MODEL}
                  <span class="badge badge-green">FALLBACK ✓</span>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.error("Groq API offline — check GROQ_API_KEY")

    st.divider()
    st.markdown("**📊 Dataset**")
    try:
        info  = db.get_date_range()
        count = db.query("SELECT COUNT(*) AS n FROM payments").iloc[0]["n"]
        st.metric("Rows",       f"{count:,}")
        st.metric("Date Range", f"{info['min_date'][:7]} → {info['max_date'][:7]}")
        st.metric("Fiscal Yrs", " · ".join(info["fiscal_years"]))
    except Exception as e:
        st.error(f"DB: {e}")

    st.divider()
    st.markdown("**💡 Try These**")
    for q in [
        "How did total ecosystem TPV perform last week vs last month?",
        "What was our total active customer base last month?",
        "Which segment had the worst churn rate last month?",
        "How has average invoice size evolved over last 12 months?",
        "Build a health scorecard for each segment today.",
        "Growth-vs-retention matrix for all segments?",
    ]:
        if st.button(q, use_container_width=True, key=f"ex_{q[:22]}"):
            st.session_state.pending_question = q

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        if st.button("🗑️ Clear Chat", use_container_width=True, type="primary"):
            st.session_state.messages = []
            agent.clear_history()
            st.toast("Chat cleared!", icon="✅")
            st.rerun()
    with c2:
        if st.button("🚪 Sign Out", use_container_width=True):
            st.session_state.update({"user":None,"messages":[],"agent":None})
            st.rerun()
    st.caption(f"💬 {len(st.session_state.messages)} messages")
    if triggered:
        st.toast(f"⚡ {triggered} scheduled report(s) ran", icon="📅")

# ── Auto-chart helper ─────────────────────────────────────────────────────────
def _show_auto_chart(df):
    num = df.select_dtypes("number").columns.tolist()
    cat = df.select_dtypes("object").columns.tolist()
    if not num: return
    dc = next((c for c in df.columns if "date" in c.lower()), None)
    if dc:
        try:
            st.plotly_chart(px.line(df,x=dc,y=num[0],
                color=cat[0] if cat else None,
                title=num[0],template="plotly_dark"),use_container_width=True)
            return
        except: pass
    if cat and len(df)<=30:
        try:
            st.plotly_chart(px.bar(df.head(20),x=cat[0],y=num[0],
                title=f"{num[0]} by {cat[0]}",template="plotly_dark"),use_container_width=True)
        except: pass

# ════════════════════════════════════════════════════════════════════════════
# TABS
# ════════════════════════════════════════════════════════════════════════════
tab_chat, tab_sched, tab_hist = st.tabs(["💬 Chat", "📅 Scheduled Reports", "📜 My History"])

# ── CHAT TAB ──────────────────────────────────────────────────────────────────
with tab_chat:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "user":
                st.markdown(msg["content"])
            else:
                st.markdown(msg.get("interpretation", msg.get("content","")))
                if msg.get("thought"):
                    with st.expander("🧠 Analytical Approach"):
                        st.markdown(f'<div class="thought-block">{msg["thought"]}</div>', unsafe_allow_html=True)
                if msg.get("sql"):
                    with st.expander("🔍 SQL Query"):
                        st.code(msg["sql"], language="sql")
                if msg.get("df") is not None:
                    df = pd.DataFrame(msg["df"])
                    if not df.empty:
                        with st.expander(f"📋 Results ({len(df)} rows)"):
                            st.dataframe(df, use_container_width=True)
                            _show_auto_chart(df)
                if msg.get("retries"):
                    st.caption(f"⚡ Auto-repaired in {msg['retries']} attempt(s)")

    user_input = st.session_state.pop("pending_question", None)
    if user_input is None:
        user_input = st.chat_input("Ask anything about your payments data…") or None

    if user_input:
        st.session_state.messages.append({"role":"user","content":user_input})
        with st.chat_message("user"):
            st.markdown(user_input)
        with st.chat_message("assistant"):
            if not groq_ok:
                st.error("Groq API unreachable. Check GROQ_API_KEY.")
            else:
                with st.spinner("Thinking…"):
                    t0   = time.time()
                    resp = agent.run_with_history(user_input, user_email=user["email"])
                    elapsed = time.time() - t0
                if resp.error:
                    st.error(f"**Error:** {resp.error}")
                    st.session_state.messages.append({"role":"assistant","content":resp.error})
                else:
                    if resp.thought:
                        with st.expander("🧠 Analytical Approach"):
                            st.markdown(f'<div class="thought-block">{resp.thought}</div>', unsafe_allow_html=True)
                    if resp.sql:
                        with st.expander("🔍 SQL Query"):
                            st.code(resp.sql, language="sql")
                    st.markdown(resp.interpretation)
                    if resp.results_df is not None and not resp.results_df.empty:
                        with st.expander(f"📋 Results ({len(resp.results_df)} rows)"):
                            st.dataframe(resp.results_df, use_container_width=True)
                            _show_auto_chart(resp.results_df)
                    model_note = f"⚡ `{active_m}`" + (" (fallback)" if using_fb else "")
                    if resp.retries_used > 0:
                        model_note += f" · repaired {resp.retries_used}x"
                    st.caption(f"{model_note} · {elapsed:.1f}s")
                    st.session_state.messages.append({
                        "role":"assistant","thought":resp.thought,"sql":resp.sql,
                        "interpretation":resp.interpretation,
                        "df":resp.results_df.to_dict() if resp.results_df is not None else None,
                        "retries":resp.retries_used,
                    })
        st.rerun()

# ── SCHEDULER TAB ─────────────────────────────────────────────────────────────
with tab_sched:
    col_l, col_r = st.columns([1, 1.6], gap="large")

    with col_l:
        st.subheader("➕ Create Report")
        with st.form("create_report", clear_on_submit=True):
            rname  = st.text_input("Report Name", placeholder="Weekly TPV Summary")
            rprompt= st.text_area("Question / Prompt",
                placeholder="How did total ecosystem TPV perform last week vs last month?",
                height=120)
            rsched = st.selectbox("Run Every", list(SCHEDULE_LABELS.keys()),
                                  format_func=lambda k: SCHEDULE_LABELS[k])
            rtime  = st.time_input("At time (HH:MM)", value=None, step=3600)
            time_str = rtime.strftime("%H:%M") if rtime else "08:00"
            rday = None
            if rsched == "weekly":
                rday = st.selectbox("On day", DAY_OPTIONS)
            elif rsched == "monthly":
                rday = st.selectbox("Day of month", [str(i) for i in range(1,29)])
            sub = st.form_submit_button("Schedule Report", use_container_width=True, type="primary")
        if sub:
            if not rname or not rprompt:
                st.error("Name and prompt required.")
            else:
                ok, msg = create_report(user["email"], rname, rprompt,
                                        rsched, time_str, rday or "1")
                if ok:
                    st.success(msg); st.rerun()
                else:
                    st.error(msg)

    with col_r:
        st.subheader("📋 My Reports")
        reports = get_user_reports(user["email"])
        if not reports:
            st.info("No reports yet. Create one on the left.")
        else:
            for r in reports:
                badge = ('<span class="badge badge-green">● ACTIVE</span>'
                         if r["status"]=="active"
                         else '<span class="badge badge-yellow">⏸ PAUSED</span>')
                sl    = SCHEDULE_LABELS.get(r["schedule_type"], r["schedule_type"])
                day_info = (f"on {r.get('schedule_day','')} " 
                            if r["schedule_type"] in ("weekly","monthly") else "")
                st.markdown(f"""
                <div class="report-card">
                  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:.3rem">
                    <span class="report-name">{r['name']}</span>{badge}
                  </div>
                  <div class="report-meta">🔁 {sl} {day_info}at {r.get('schedule_time','08:00')} &nbsp;·&nbsp;
                    Next: <b>{str(r.get('next_run','—'))[:16]}</b> &nbsp;·&nbsp;
                    Last: {str(r.get('last_run','Never'))[:16] if r.get('last_run') else 'Never'}
                  </div>
                  <div class="report-meta" style="margin-top:4px;color:#374151">
                    <i>"{r['prompt'][:90]}{'…' if len(r['prompt'])>90 else ''}"</i>
                  </div>
                </div>
                """, unsafe_allow_html=True)

                if r.get("last_result"):
                    with st.expander("📄 Last Result"):
                        st.markdown(r["last_result"])

                bc = st.columns(3)
                with bc[0]:
                    if r["status"]=="active":
                        if st.button("⏸ Pause", key=f"p_{r['id']}", use_container_width=True):
                            ok,msg = pause_report(r["id"],user["email"]); st.toast(msg); st.rerun()
                    else:
                        if st.button("▶ Resume", key=f"rs_{r['id']}", use_container_width=True):
                            ok,msg = resume_report(r["id"],user["email"]); st.toast(msg); st.rerun()
                with bc[1]:
                    if st.button("▷ Run Now", key=f"rn_{r['id']}", use_container_width=True):
                        with st.spinner("Running…"):
                            ok,result = run_now(r["id"],user["email"],agent)
                        
                        if ok:
                            st.success("Done!")
                            st.markdown(result)
                        else:
                            st.error(result)
                            
                        st.rerun()
                with bc[2]:
                    if st.button("🗑 Delete", key=f"d_{r['id']}", use_container_width=True):
                        ok,msg = delete_report(r["id"],user["email"]); st.toast(msg); st.rerun()
                st.divider()

# ── HISTORY TAB ───────────────────────────────────────────────────────────────
with tab_hist:
    st.subheader("📜 My Query History")
    st.caption("Detailed log of your analytical queries, confidence scores, and performance metrics.")
    
    my_hist = history_db.get_history(user_email=user["email"], limit=100)
    
    if my_hist.empty:
        st.info("No query history yet. Start a conversation in the Chat tab!")
    else:
        # Prepare display dataframe
        disp_df = my_hist.copy()
        disp_df["Time"] = pd.to_datetime(disp_df["created_at"]).dt.strftime("%Y-%m-%d %H:%M")
        disp_df["Latency"] = (disp_df["total_ms"] / 1000).map("{:.1f}s".format)
        disp_df["Confidence"] = (disp_df["confidence_score"] * 100).map("{:.1f}%".format)
        disp_df["Status"] = disp_df["success"].map({1: "✅ Success", 0: "❌ Failed"})
        disp_df["Repair"] = disp_df["had_repair"].map({1: "🔧 Fixed", 0: "—"})
        
        # Rename and select columns for the table
        table_df = disp_df[[
            "Time", "question", "question_category", "Status", 
            "Confidence", "rows_returned", "Latency", "Repair"
        ]].rename(columns={
            "question": "Question",
            "question_category": "Category",
            "rows_returned": "Rows"
        })
        
        # Display as interactive dataframe
        st.dataframe(
            table_df,
            use_container_width=True,
            column_config={
                "Confidence": st.column_config.TextColumn("Confidence", help="Composite trust score"),
                "Rows": st.column_config.NumberColumn("Rows", format="%d"),
                "Time": st.column_config.TextColumn("Time"),
            },
            hide_index=True
        )
        
        st.divider()
        st.markdown("#### 🔍 Record Detail")
        selected_q = st.selectbox(
            "Select a query to view full SQL and Interpretation:",
            options=my_hist.index,
            format_func=lambda i: f"{my_hist.loc[i, 'created_at'][:16]} | {my_hist.loc[i, 'question'][:80]}..."
        )
        
        if selected_q is not None:
            full_rec = history_db.get_record(my_hist.loc[selected_q, "id"])
            if full_rec:
                det_c1, det_c2 = st.columns([1, 1])
                with det_c1:
                    st.markdown("**Question Category:** " + str(full_rec.get("question_category", "N/A")).capitalize())
                    st.markdown("**Model Used:** `" + str(full_rec.get("model_used", "N/A")) + "`")
                with det_c2:
                    st.markdown("**SQL Complexity:** " + str(full_rec.get("sql_complexity", "N/A")).capitalize())
                    st.markdown("**Repair Attempts:** " + str(full_rec.get("retries_used", 0)))
                
                if full_rec.get("interpretation"):
                    st.info("**Interpretation**\n\n" + full_rec["interpretation"])
                
                if full_rec.get("sql_generated"):
                    st.markdown("**Generated SQL**")
                    st.code(full_rec["sql_generated"], language="sql")
                
                if full_rec.get("thought"):
                    with st.expander("🧠 View Analytical Thought Process"):
                        st.markdown(f'<div class="thought-block">{full_rec["thought"]}</div>', unsafe_allow_html=True)
                
                if full_rec.get("error_message"):
                    st.error("**Error Message:** " + full_rec["error_message"])
