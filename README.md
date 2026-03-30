# TPV Insight Pro 🏦
### Local Payments Analytics Agent — NL-to-SQL Agentic RAG on MacBook Air M4

---

## Architecture Overview

```
User Question (Natural Language)
        ↓
  [Context Injector]  ←── TPV Driver Tree + Data Dictionary
        ↓
  [Groq API / gpt-oss-120b]  ← ~500 tok/s cloud inference
        ↓
  [SQL Generator]      ← produces DuckDB SQL
        ↓
  [DuckDB Engine]      ← queries payments.csv in-memory (local)
        ↓
  [Result Formatter]   ← converts rows → readable answer
        ↓
  [Streamlit Chat UI]  ← conversation interface
```

**Stack:**
- **LLM:** Groq API + `gpt-oss-120b` (~500 tok/s, OpenAI-compatible endpoint)
- **SQL Engine:** DuckDB (zero-setup, reads CSV directly, runs fully local)
- **RAG Context:** TPV driver tree + data dictionary injected as system prompt
- **UI:** Streamlit chat interface
- **Language:** Python 3.11+

---

## Setup (one-time)

### 1. Set your Groq API key
```bash
export GROQ_API_KEY="your_groq_api_key_here"
# Add to ~/.zshrc to persist across sessions
```

### 2. Install Python deps
```bash
cd tpv-insight-pro
pip install -r requirements.txt
```

### 3. Run the app
```bash
streamlit run src/app.py
```

Open `http://localhost:8501` in your browser. No local GPU needed — Groq handles inference.

---

## Project Structure

```
tpv-insight-pro/
├── data/
│   └── payments.csv          ← your dataset (4,359 rows × 60 cols)
├── src/
│   ├── app.py                ← Streamlit chat UI (main entry point)
│   ├── agent.py              ← Agentic orchestrator (plan → SQL → answer)
│   ├── nl_to_sql.py          ← NL-to-SQL via Groq gpt-oss-120b
│   ├── db.py                 ← DuckDB query engine (local)
│   └── knowledge_base.py     ← TPV driver tree + data dictionary (RAG)
├── scripts/
│   └── test_queries.py       ← CLI test runner for the 15 business Qs
├── requirements.txt
└── README.md
```

---

## Example Questions

**Retrieval:**
- "How did total ecosystem payment volume perform last week vs last month?"
- "What was our total active customer base last month?"

**Analysis:**
- "Which segments are driving new customer additions this quarter?"
- "How has average invoice size evolved over the last 12 months?"

**Insights:**
- "Build a health scorecard for each segment today."
- "Where does each segment sit on a growth-vs-retention matrix?"

---

## ⚠️ Security Note
For production or shared repos, always use environment variables or Streamlit Secrets:
```bash
export GROQ_API_KEY="your_key_here"
```
