# Resume Matcher - Production Starter

This version is a cleaned, deployment-ready starter based on your localhost app.

## What was changed
- removed local SQLite/runtime/cache/resume sample state from the app package
- added a root `requirements.txt`
- added `.gitignore`
- added `wsgi.py`, `Procfile`, and `render.yaml` for deployment
- switched Flask secret key + host/port to environment variables
- kept your existing app routes and pipeline scripts intact

## Local run
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python wsgi.py
```

## Render deploy
1. Push this folder to GitHub
2. Create a new Render Web Service from the repo
3. Set environment variables from `.env.example`
4. Deploy

## Important
This zip is **deployment prep**, not the full SaaS conversion yet.
It does **not** add:
- email/password accounts
- database-backed users
- cloud resume storage
- Stripe subscriptions
- premium-plan gating

Those are the next phase after the app is live online.

## Recommended next stack
- Hosting: Render
- Auth + DB + Storage: Supabase
- Billing: Stripe
