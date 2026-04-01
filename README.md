# resume-match-ai-platform

Resume matching platform with two paths kept in the same repo:

- **Website path (current target):** Cloudflare Pages frontend + browser-side heavy compute + thin Cloudflare backend + Supabase
- **Local reference path:** the original Python OCR/matching pipeline kept in the repo for local demos and recruiter/project walkthroughs

## Current architecture

### Browser / client-side heavy pipeline
The deployed website now pushes the heavy **free-path** work onto the user's device:

- PDF text extraction in the browser with **PDF.js**
- OCR fallback in the browser with **Tesseract.js**
- local scoring/ranking in a browser worker
- browser-side semantic rerank attempt with a browser-compatible embedding model
- final results saved back to Supabase

### Thin backend
Cloudflare Pages Functions now only handle thin secure tasks:

- public Supabase config
- dashboard counts
- candidate job fetch for the browser pipeline
- premium code unlock
- premium rerank requests with the private OpenAI key

### Database / storage
Supabase is still used for:

- auth
- resumes table + storage bucket
- jobs table
- free match results
- premium match results
- profile premium flags / usage counts

## Project layout

- `pages_frontend/` — deployed website frontend + browser worker + Cloudflare Pages Functions
- `web_ui/` — original Flask reference app kept for local/server demos
- `resume_scraper.py` — original Python OCR/reference pipeline
- `job_matcher.py` — original Python ranking/reference pipeline
- `semantic_role_classifier.py` / `shared_model_registry.py` — original Python semantic/reference components
- `scripts/` — admin/util scripts such as job upserts
- `supabase_sql/` — ordered Supabase schema and policy scripts

## What changed in this version

### Website path
Added a browser-first pipeline that matches the architecture discussed in chat:

- website stays a **website**, not an app
- heavy free-path work runs on the **user's CPU/RAM**
- premium secrets stay backend-only
- original Python scripts remain in the repo as the local first implementation/reference path

Main added files:

- `pages_frontend/browser-pipeline-client.js`
- `pages_frontend/browser-pipeline-worker.js`
- `pages_frontend/functions/api/jobs-candidate-set.js`
- `pages_frontend/functions/api/premium-unlock.js`
- `pages_frontend/functions/api/premium-run.js`

## Quick start for the website path

### Cloudflare Pages settings
- **Root directory:** `pages_frontend`
- **Build command:** leave blank
- **Build output directory:** `.`

### Required Cloudflare environment variables / secrets
Set these in Cloudflare Pages / Workers:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SECRET_KEY`
- `OPENAI_API_KEY` for premium backend calls
- `OPENAI_MODEL` optional, defaults to `gpt-4o-mini`
- `PREMIUM_ACCESS_CODE`
- `PREMIUM_ADMIN_CODE`

## Quick start for the Python reference path

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # Windows: copy .env.example .env
python wsgi.py
```

Open `http://127.0.0.1:5000`.

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

## Included in this sanitized repo

- real environment files were removed
- local virtual environments and runtime outputs were removed
- cached match outputs, resume workspaces, archives, and local databases were removed
- duplicate legacy SQL files were removed in favor of `supabase_sql/`
- local JSON data snapshots were removed so the repo stays lightweight and Supabase remains the primary source of truth for jobs and user data

## Notes

- Keep all real secrets in Cloudflare/Supabase environment variables, not in the repo.
- The Python app is still here as the original local/server implementation.
- The website path is now the main path for your browser-compute architecture.
