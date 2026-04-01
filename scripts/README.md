# Utility scripts

- `jobs_update.py` uploads or upserts a local scraped jobs JSON file into the Supabase `jobs` table.
- `check_supabase_local.py` runs a quick local health check against `/api/supabase-status`.
- `scrape_jobs.py` and `url_scraper.py` are optional local data collection utilities.

Example:

```bash
python scripts/jobs_update.py
```

You can point `jobs_update.py` at a different input file with:

```bash
SCRAPED_JOBS_FILE=path/to/jobs.json python scripts/jobs_update.py
```
