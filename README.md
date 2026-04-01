# Job Finder / RoleMatcher

Job matching platform with a Python reference app, Supabase schema/setup scripts, admin utilities, and a Cloudflare Pages frontend.

## Project layout

- `pages_frontend/` — Cloudflare Pages frontend with Supabase auth, upload flow, and public config function
- `web_ui/` — Flask reference app, templates, static assets, Supabase helpers, and premium helper code
- `job_matcher.py` — ranking pipeline used by the Python app after resume text extraction
- `resume_scraper.py` — OCR and resume parsing pipeline used by the Python app
- `matcher_taxonomy.py` / `semantic_role_classifier.py` / `shared_model_registry.py` — role classification, taxonomy enrichment, and shared model loading
- `scripts/` — maintenance utilities such as Supabase job upserts and local health checks
- `supabase_sql/` — ordered Supabase schema and policy scripts

## Included in this sanitized repo

- real environment files were removed
- local virtual environments and runtime outputs were removed
- cached match outputs, resume workspaces, archives, and local databases were removed
- duplicate legacy SQL files were removed in favor of `supabase_sql/`
- local JSON data snapshots were removed so the repo stays lightweight and Supabase remains the primary source of truth for jobs and user data

## Quick start for the Python app

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # Windows: copy .env.example .env
python wsgi.py
```

Open `http://127.0.0.1:5000`.

## Quick start for the Cloudflare Pages frontend

- Cloudflare Pages root directory: `pages_frontend`
- Build command: leave blank
- Build output directory: `.`
- Required Cloudflare variables: `SUPABASE_URL`, `SUPABASE_ANON_KEY`

## Environment variables

Set these in `.env` locally and in your deployment environment where applicable:

- `SECRET_KEY`
- `SUPABASE_URL`
- `SUPABASE_PUBLISHABLE_KEY` for the Python app
- `SUPABASE_ANON_KEY` for the Pages frontend
- `SUPABASE_SECRET_KEY`
- `SUPABASE_AUTH_REDIRECT_URL`
- `SUPABASE_RESUMES_BUCKET`
- `SUPABASE_ACTIVE_PREFIX`
- `SUPABASE_ARCHIVE_PREFIX`
- `MAX_RESUMES`
- `OPENAI_API_KEY` if premium OpenAI-backed features are enabled
- `PREMIUM_ACCESS_CODE` / `PREMIUM_ADMIN_CODE` if code-based premium unlocks are enabled

## Supabase setup

1. Run the SQL files in `supabase_sql/` in order.
2. Create the `resumes` storage bucket.
3. Add your local and production auth redirect URLs in Supabase Auth settings.
4. Populate the `jobs` table before running matching in a fresh environment.

## Loading jobs into Supabase

Use the utility script in `scripts/jobs_update.py` to upload or upsert a local jobs JSON file into the `jobs` table.

```bash
python scripts/jobs_update.py
```

By default it reads `scraped_jobs.json` from your local machine if present. You can also point it to any file with `SCRAPED_JOBS_FILE=...`.

## Placeholder runtime directories kept in the repo

- `resumes/`
- `resume_archive/`
- `match_cache/`
- `match_cache_archive/`
- `web_ui/runtime/`

## Notes

- Keep all real secrets in your host environment variables, not in the repo.
- The Python app remains available as a reference/local path while `pages_frontend/` is the deployed web frontend.
