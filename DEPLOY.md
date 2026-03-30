# Deploying TPV Insight Pro to Streamlit Cloud

## Step 1 — Push to GitHub

```bash
cd tpv-insight-pro

# Initialise git (if not already done)
git init
git add .
git commit -m "Initial commit — TPV Insight Pro"

# Create a repo on github.com, then:
git remote add origin https://github.com/YOUR_USERNAME/tpv-insight-pro.git
git branch -M main
git push -u origin main
```

> ✅ The `.gitignore` excludes `.streamlit/secrets.toml` so your API key is never committed.

---

## Step 2 — Deploy on Streamlit Cloud

1. Go to **https://share.streamlit.io** and sign in with GitHub
2. Click **"New app"**
3. Fill in:
   - **Repository:** `YOUR_USERNAME/tpv-insight-pro`
   - **Branch:** `main`
   - **Main file path:** `src/app.py`
4. Click **"Advanced settings"** → **Secrets** and paste:
   ```toml
   GROQ_API_KEY = "gsk_your_key_here"
   ```
5. Click **"Deploy"**

Your app will be live at:
`https://YOUR_USERNAME-tpv-insight-pro-srcapp-XXXX.streamlit.app`

---

## Step 3 — Verify it works

The app reads `data/payments.csv` relative to the project root.
Streamlit Cloud checks out your full repo, so the CSV is available automatically.

If you see a `FileNotFoundError` for `payments.csv`, check that the file
was committed to git (it may have been excluded by a `.gitignore` pattern).
Verify with:

```bash
git ls-files data/payments.csv
# Should print: data/payments.csv
```

If it's missing:
```bash
git add -f data/payments.csv
git commit -m "Add payments dataset"
git push
```

---

## Local development (with secrets)

Create `.streamlit/secrets.toml` (excluded from git automatically):

```toml
GROQ_API_KEY = "gsk_your_key_here"
```

Then run:
```bash
streamlit run src/app.py
```
