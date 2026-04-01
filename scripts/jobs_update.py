import json
import os
from datetime import datetime, timezone
from typing import Any

from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_SECRET_KEY:
    raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY")

INPUT_JSON = os.environ.get("SCRAPED_JOBS_FILE", "scraped_jobs.json")
TABLE_NAME = os.environ.get("SUPABASE_JOBS_TABLE", "jobs")
UPSERT_BATCH_SIZE = int(os.environ.get("UPSERT_BATCH_SIZE", "500"))

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def normalize_country(location: str | None) -> str | None:
    if not location:
        return None
    loc = location.lower()
    country_aliases = [
        ("canada", "Canada"),
        ("united states", "USA"),
        (" usa", "USA"),
        ("u.s.", "USA"),
        ("united kingdom", "United Kingdom"),
        ("uk", "United Kingdom"),
        ("ireland", "Ireland"),
        ("poland", "Poland"),
        ("israel", "Israel"),
        ("germany", "Germany"),
        ("france", "France"),
        ("india", "India"),
        ("japan", "Japan"),
        ("korea", "Korea, Republic of"),
        ("singapore", "Singapore"),
        ("malaysia", "Malaysia"),
        ("mexico", "Mexico"),
        ("costa rica", "Costa Rica"),
        ("australia", "Australia"),
        ("netherlands", "Netherlands"),
        ("taiwan", "Taiwan"),
        ("china", "China"),
    ]
    for needle, normalized in country_aliases:
        if needle in loc:
            return normalized
    return None


def normalize_work_mode(value: str | None, description_text: str | None) -> str:
    candidates = " ".join([value or "", description_text or ""]).lower()
    if "hybrid" in candidates:
        return "hybrid"
    if "remote" in candidates or "work from home" in candidates or "virtual" in candidates:
        return "remote"
    if "on-site" in candidates or "onsite" in candidates or "regular onsite presence" in candidates:
        return "on-site"
    return "on-site"


def ensure_json_compatible(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [ensure_json_compatible(v) for v in value]
    if isinstance(value, dict):
        return {str(k): ensure_json_compatible(v) for k, v in value.items()}
    return str(value)


def coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_job_id(job: dict[str, Any]) -> str | None:
    job_id = clean_text(job.get("job_id"))
    if job_id:
        return job_id
    source_url = clean_text(job.get("source_url"))
    if source_url:
        return source_url
    title = clean_text(job.get("title")) or ""
    company = clean_text(job.get("company")) or ""
    location = clean_text(job.get("location")) or ""
    fallback = " | ".join(part for part in [title, company, location] if part)
    return fallback or None


def normalize_job(job: dict[str, Any]) -> dict[str, Any] | None:
    title = clean_text(job.get("title"))
    company = clean_text(job.get("company"))
    location = clean_text(job.get("location"))
    description_text = clean_text(job.get("description_text") or job.get("description"))
    source_url = clean_text(job.get("source_url") or job.get("url"))
    job_id = build_job_id(job)

    if not job_id:
        return None

    country = clean_text(job.get("country")) or normalize_country(location)
    work_mode = normalize_work_mode(clean_text(job.get("work_mode")), description_text)
    degree_fields = job.get("degree_fields")
    if isinstance(degree_fields, str):
        degree_fields = [degree_fields]

    normalized = {
        "job_id": job_id,
        "title": title,
        "company": company,
        "location": location,
        "country": country,
        "work_mode": work_mode,
        "description_text": description_text,
        "source_url": source_url,
        "posted_date": clean_text(job.get("posted_date")),
        "collected_date": clean_text(job.get("collected_date")),
        "job_function": clean_text(job.get("job_function")),
        "job_domain": clean_text(job.get("job_domain")),
        "job_category_key": clean_text(job.get("job_category_key")),
        "job_category_confidence": coerce_float(job.get("job_category_confidence")),
        "job_category_scores": ensure_json_compatible(job.get("job_category_scores")),
        "experience_needed_years": coerce_float(job.get("experience_needed_years")),
        "degree_level_min": clean_text(job.get("degree_level_min")),
        "degree_family": clean_text(job.get("degree_family")),
        "degree_fields": ensure_json_compatible(degree_fields),
        "raw_json": ensure_json_compatible(job),
        "updated_at": utc_now_iso(),
    }
    return normalized


def chunked(items: list[dict[str, Any]], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


with open(INPUT_JSON, "r", encoding="utf-8") as f:
    jobs = json.load(f)

if not isinstance(jobs, list):
    raise RuntimeError(f"{INPUT_JSON} must contain a JSON array of jobs")

batch = []
skipped = 0
for job in jobs:
    normalized = normalize_job(job)
    if normalized is None:
        skipped += 1
        continue
    batch.append(normalized)

uploaded = 0
for chunk in chunked(batch, UPSERT_BATCH_SIZE):
    (
        supabase
        .table(TABLE_NAME)
        .upsert(chunk, on_conflict="job_id")
        .execute()
    )
    uploaded += len(chunk)
    print(f"Uploaded/updated {uploaded}/{len(batch)} jobs...")

print(f"Done. Uploaded/updated {uploaded} jobs. Skipped {skipped} jobs.")
