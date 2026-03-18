from __future__ import annotations

import json
import os
import re
import sys
import time
import threading
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from functools import wraps
import hashlib
import shutil
import sqlite3

import pycountry
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
from werkzeug.utils import secure_filename
from PIL import Image

from web_ui.supabase_db import get_supabase_auth, get_supabase_db

APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent

# Project scripts (UNCHANGED)
JOB_MATCHER = PROJECT_DIR / "job_matcher.py"
RESUME_SCRAPER = PROJECT_DIR / "resume_scraper.py"
JOB_SCRAPER = PROJECT_DIR / "scrape_jobs.py"
URL_SCRAPER = PROJECT_DIR / "url_scraper.py"

# Project data (UNCHANGED)
RESUMES_DIR = PROJECT_DIR / "resumes"
SCANNED_RESUMES_JSON = PROJECT_DIR / "scanned_resumes.json"
SCRAPED_JOBS_JSON = PROJECT_DIR / "scraped_jobs.json"
MATCHES_JSON = PROJECT_DIR / "resume_job_matches.json"
MATCHES_DB = PROJECT_DIR / "resume_job_matches.db"
MAX_RESUMES = int(os.environ.get("MAX_RESUMES", "3"))
RESUME_MANIFEST_JSON = PROJECT_DIR / "resumes_manifest.json"
RESUME_ARCHIVE_DIR = PROJECT_DIR / "Resume archive"
RESUME_ARCHIVE_MANIFEST_JSON = RESUME_ARCHIVE_DIR / "archive_manifest.json"
MATCH_CACHE_DIR = PROJECT_DIR / "match_cache"
MATCH_CACHE_ARCHIVE_DIR = PROJECT_DIR / "match_cache_archive"

# Internal runtime logs (kept for debugging; NOT exposed in UI)
RUNTIME_DIR = APP_DIR / "runtime"
LAST_STDOUT = RUNTIME_DIR / "last_stdout.txt"
LAST_STDERR = RUNTIME_DIR / "last_stderr.txt"

ALLOWED_RESUME_EXTS = {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff"}
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

SUPABASE_DB = get_supabase_db()
SUPABASE_AUTH = get_supabase_auth()
_SUPABASE_JOBS_CACHE: Optional[Tuple[float, List[Dict[str, Any]], Dict[str, Dict[str, Any]]]] = None
SUPABASE_JOBS_CACHE_TTL_S = int(os.environ.get("SUPABASE_JOBS_CACHE_TTL_S", "60"))

app = Flask(__name__)

for _p in [RESUMES_DIR, RESUME_ARCHIVE_DIR, MATCH_CACHE_DIR, MATCH_CACHE_ARCHIVE_DIR, RUNTIME_DIR]:
    _p.mkdir(parents=True, exist_ok=True)

app.secret_key = os.environ.get("SECRET_KEY", "dev-only-change-me")


# ---------------------------
# Speed: mtime-based JSON cache
# ---------------------------
_JSON_CACHE: Dict[str, Tuple[float, Any]] = {}

def safe_load_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        m = path.stat().st_mtime
        key = str(path.resolve())
        hit = _JSON_CACHE.get(key)
        if hit and hit[0] == m:
            return hit[1]
        data = json.loads(path.read_text(encoding="utf-8"))
        _JSON_CACHE[key] = (m, data)
        return data
    except Exception:
        return default


def safe_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_resume_manifest() -> List[Dict[str, Any]]:
    data = safe_load_json(RESUME_MANIFEST_JSON, [])
    return data if isinstance(data, list) else []


def save_resume_manifest(rows: List[Dict[str, Any]]) -> None:
    safe_write_json(RESUME_MANIFEST_JSON, rows)


def load_resume_archive_manifest() -> List[Dict[str, Any]]:
    data = safe_load_json(RESUME_ARCHIVE_MANIFEST_JSON, [])
    return data if isinstance(data, list) else []


def save_resume_archive_manifest(rows: List[Dict[str, Any]]) -> None:
    safe_write_json(RESUME_ARCHIVE_MANIFEST_JSON, rows)


def make_timestamped_pdf_name(original_name: str) -> str:
    safe_stem = secure_filename(Path(original_name).stem) or "resume"
    stamp = datetime.now().strftime("%m_%d_%y_%H_%M_%S")
    candidate = f"{safe_stem}_{stamp}.pdf"
    out = RESUMES_DIR / candidate
    idx = 2
    while out.exists():
        candidate = f"{safe_stem}_{stamp}_{idx}.pdf"
        out = RESUMES_DIR / candidate
        idx += 1
    return candidate




def get_current_user_id() -> Optional[str]:
    user_id = session.get("user_id")
    if isinstance(user_id, str) and user_id.strip():
        return user_id.strip()
    return None


def get_current_user_email() -> str:
    email = session.get("user_email")
    return email.strip() if isinstance(email, str) else ""


def is_logged_in() -> bool:
    return bool(get_current_user_id())


def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not is_logged_in():
            flash("Please sign in first.")
            return redirect(url_for("login_page", next=request.path))
        return view_func(*args, **kwargs)
    return wrapped


def auth_redirect_target() -> str:
    configured = (os.environ.get("SUPABASE_AUTH_REDIRECT_URL") or "").strip()
    if configured:
        return configured
    return url_for("login_page", confirmed="1", _external=True)


def set_logged_in_session(auth_payload: Dict[str, Any]) -> None:
    user = auth_payload.get("user") if isinstance(auth_payload, dict) else {}
    if not isinstance(user, dict):
        user = {}
    session["user_id"] = str(user.get("id") or "")
    session["user_email"] = str(user.get("email") or "")
    session["access_token"] = str(auth_payload.get("access_token") or "")
    session["refresh_token"] = str(auth_payload.get("refresh_token") or "")


@app.context_processor
def inject_auth_state():
    return {
        "is_logged_in": is_logged_in(),
        "current_user_email": get_current_user_email(),
        "current_user_id": get_current_user_id(),
    }


def get_user_active_resume_count(user_id: Optional[str]) -> int:
    if not user_id:
        return len(list(RESUMES_DIR.glob("*.pdf"))) if RESUMES_DIR.exists() else 0
    if SUPABASE_DB is None:
        return len(list(RESUMES_DIR.glob("*.pdf"))) if RESUMES_DIR.exists() else 0
    try:
        return SUPABASE_DB.count(
            "resumes",
            filters={
                "user_id": f"eq.{user_id}",
                "archived": "eq.false",
            },
        )
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase per-user resume count failed: {repr(e)}")
        return len(list(RESUMES_DIR.glob("*.pdf"))) if RESUMES_DIR.exists() else 0


def get_user_resume_rows(user_id: Optional[str]) -> List[Dict[str, Any]]:
    if not user_id or SUPABASE_DB is None:
        return []
    try:
        return SUPABASE_DB.select(
            "resumes",
            filters={
                "user_id": f"eq.{user_id}",
                "archived": "eq.false",
            },
            order="uploaded_at.desc",
        )
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase per-user resume fetch failed: {repr(e)}")
        return []

def humanize_age(uploaded_at: str) -> str:
    try:
        dt = datetime.fromisoformat(uploaded_at)
    except Exception:
        return "awhile ago"
    delta = datetime.now() - dt
    seconds = max(0, int(delta.total_seconds()))
    minutes = seconds // 60
    hours = seconds // 3600
    days = delta.days
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return "1 minute ago" if minutes == 1 else f"{minutes} minutes ago"
    if hours < 24:
        return "1 hour ago" if hours == 1 else f"{hours} hours ago"
    if days < 7:
        return "1 day ago" if days == 1 else f"{days} days ago"
    weeks = days // 7
    if weeks <= 3:
        return "1 week ago" if weeks == 1 else f"{weeks} weeks ago"
    return "awhile ago"


def get_active_resume_items() -> List[Dict[str, Any]]:
    user_id = get_current_user_id()
    if user_id and SUPABASE_DB is not None:
        rows = get_user_resume_rows(user_id)
        items: List[Dict[str, Any]] = []
        for row in rows:
            stored_filename = str(row.get("stored_filename") or "").strip()
            if not stored_filename:
                continue
            display_stem = str(row.get("display_stem") or Path(stored_filename).stem)
            uploaded_at = str(row.get("uploaded_at") or datetime.now().isoformat())
            items.append({
                "stored_filename": stored_filename,
                "display_stem": display_stem,
                "uploaded_at": uploaded_at,
                "relative_time": humanize_age(uploaded_at),
            })

        counts: Dict[str, int] = {}
        for item in items:
            stem = item["display_stem"]
            counts[stem] = counts.get(stem, 0) + 1
            item["display_label"] = stem if counts[stem] == 1 else f"{stem} ({counts[stem]})"
        return items

    RESUMES_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_resume_manifest()
    by_file = {row.get("stored_filename"): dict(row) for row in manifest if isinstance(row, dict)}
    active_files = sorted((p.name for p in RESUMES_DIR.glob("*.pdf")), key=lambda fn: str(by_file.get(fn, {}).get("uploaded_at", "")), reverse=True)
    items: List[Dict[str, Any]] = []
    for fn in active_files:
        row = by_file.get(fn, {})
        display_stem = str(row.get("display_stem") or Path(fn).stem)
        uploaded_at = str(row.get("uploaded_at") or datetime.fromtimestamp((RESUMES_DIR / fn).stat().st_mtime).isoformat())
        items.append({
            "stored_filename": fn,
            "display_stem": display_stem,
            "uploaded_at": uploaded_at,
            "relative_time": humanize_age(uploaded_at),
        })

    counts: Dict[str, int] = {}
    for item in items:
        stem = item["display_stem"]
        counts[stem] = counts.get(stem, 0) + 1
        item["display_label"] = stem if counts[stem] == 1 else f"{stem} ({counts[stem]})"
    return items


def remove_resume_from_outputs(resume_id: str, stored_filename: str) -> None:
    scanned = safe_load_json(SCANNED_RESUMES_JSON, [])
    if isinstance(scanned, list):
        scanned = [row for row in scanned if not (isinstance(row, dict) and (str(row.get("resume_id", "")) == resume_id or str(row.get("file_name", "")) == stored_filename))]
        safe_write_json(SCANNED_RESUMES_JSON, scanned)

    matches = safe_load_json(MATCHES_JSON, [])
    if isinstance(matches, list):
        matches = [row for row in matches if not (isinstance(row, dict) and str(row.get("resume_id", "")) == resume_id)]
        safe_write_json(MATCHES_JSON, matches)

    if MATCHES_DB.exists():
        try:
            conn = sqlite3.connect(MATCHES_DB)
            cur = conn.cursor()
            cur.execute("DELETE FROM matches WHERE resume_id = ?", (resume_id,))
            conn.commit()
            conn.close()
        except Exception:
            pass


def archive_resume_cache(resume_id: str) -> None:
    MATCH_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    MATCH_CACHE_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    src = MATCH_CACHE_DIR / f"{resume_id}.json"
    if not src.exists():
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = MATCH_CACHE_ARCHIVE_DIR / f"{resume_id}_{stamp}.json"
    shutil.move(str(src), str(dst))


# ---------------------------
# Country list (all ISO) + inference
# ---------------------------
COUNTRY_LIST = sorted(set([c.name for c in pycountry.countries] + ["Taiwan", "Remote"]), key=lambda x: x.lower())
WORK_MODE_OPTIONS = ["Remote", "Hybrid", "On-site"]
ALIASES = {
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "south korea": "Korea, Republic of",
    "korea": "Korea, Republic of",
    "taiwan": "Taiwan",
    "palestine": "Palestine, State of",
}
REGION_CODES = {
    # Canada
    "AB": "Canada", "BC": "Canada", "MB": "Canada", "NB": "Canada", "NL": "Canada", "NS": "Canada",
    "NT": "Canada", "NU": "Canada", "ON": "Canada", "PE": "Canada", "QC": "Canada", "SK": "Canada", "YT": "Canada",
    # United States
    "AL": "United States", "AK": "United States", "AZ": "United States", "AR": "United States", "CA": "United States",
    "CO": "United States", "CT": "United States", "DE": "United States", "FL": "United States", "GA": "United States",
    "HI": "United States", "ID": "United States", "IL": "United States", "IN": "United States", "IA": "United States",
    "KS": "United States", "KY": "United States", "LA": "United States", "ME": "United States", "MD": "United States",
    "MA": "United States", "MI": "United States", "MN": "United States", "MS": "United States", "MO": "United States",
    "MT": "United States", "NE": "United States", "NV": "United States", "NH": "United States", "NJ": "United States",
    "NM": "United States", "NY": "United States", "NC": "United States", "ND": "United States", "OH": "United States",
    "OK": "United States", "OR": "United States", "PA": "United States", "RI": "United States", "SC": "United States",
    "SD": "United States", "TN": "United States", "TX": "United States", "UT": "United States", "VT": "United States",
    "VA": "United States", "WA": "United States", "WV": "United States", "WI": "United States", "WY": "United States",
    "DC": "United States",
}
_COUNTRY_PATTERNS: List[Tuple[str, re.Pattern]] = []
for name in COUNTRY_LIST:
    if name == "Remote":
        continue
    _COUNTRY_PATTERNS.append((name, re.compile(rf"(?i)(^|[\s,(/-]){re.escape(name)}($|[\s,)/-])")))

def _match_alias_or_country(text: str) -> str:
    for k, v in ALIASES.items():
        if re.search(rf"(?i)(^|[\s,(/-]){re.escape(k)}($|[\s,)/-])", text):
            return v
    for name, pat in _COUNTRY_PATTERNS:
        if pat.search(text):
            return name
    return ""

def infer_country(location: str) -> str:
    if not location:
        return ""

    s = str(location).strip()
    if not s:
        return ""
    s_low = s.lower()

    direct = _match_alias_or_country(s)
    if direct:
        return direct

    parts = [part.strip() for part in re.split(r"[,/|()-]+", s) if part.strip()]
    for part in reversed(parts):
        part_low = part.lower()
        if part_low in ALIASES:
            return ALIASES[part_low]

        try:
            if len(part) == 2:
                if part.upper() in REGION_CODES:
                    return REGION_CODES[part.upper()]
                c = pycountry.countries.get(alpha_2=part.upper())
                if c:
                    return c.name
            if len(part) == 3:
                c = pycountry.countries.get(alpha_3=part.upper())
                if c:
                    return c.name
        except Exception:
            pass

        direct_part = _match_alias_or_country(part)
        if direct_part:
            return direct_part

    if "remote" in s_low:
        cleaned = re.sub(r"(?i)\bremote\b", " ", s)
        cleaned_match = _match_alias_or_country(cleaned)
        if cleaned_match:
            return cleaned_match
        return "Remote"

    token_matches = re.findall(r"\b[A-Za-z]{2,3}\b", s)
    for token in reversed(token_matches):
        token_up = token.upper()
        if token_up in REGION_CODES:
            return REGION_CODES[token_up]
        try:
            c = pycountry.countries.get(alpha_2=token_up)
            if c:
                return c.name
            c = pycountry.countries.get(alpha_3=token_up)
            if c:
                return c.name
        except Exception:
            pass

    return ""

def infer_work_mode(title: str, location: str, description: str) -> str:
    text = " ".join([str(title or ""), str(location or ""), str(description or "")]).strip().lower()
    if not text:
        return "On-site"

    hybrid_patterns = [
        r"\bhybrid\b",
        r"\bsplit (?:their )?time between\b",
        r"\bon[- ]site and off[- ]site\b",
        r"\bin office .* remote\b",
        r"\bremote .* in office\b",
    ]
    remote_patterns = [
        r"\bremote\b",
        r"\bwork from home\b",
        r"\bwfh\b",
        r"\bhome[- ]based\b",
        r"\btelecommut\w*\b",
        r"\bvirtual\b",
        r"\bdistributed team\b",
        r"\banywhere\b",
    ]
    onsite_patterns = [
        r"\bon[- ]site\b",
        r"\bonsite\b",
        r"\bon site\b",
        r"\bin[- ]office\b",
        r"\bin office\b",
        r"\bon campus\b",
        r"\bat our office\b",
        r"\bregular onsite presence\b",
    ]

    for pat in hybrid_patterns:
        if re.search(pat, text):
            return "Hybrid"
    for pat in remote_patterns:
        if re.search(pat, text):
            return "Remote"
    for pat in onsite_patterns:
        if re.search(pat, text):
            return "On-site"
    return "On-site"


# ---------------------------
# Speed: enriched jobs cache (country computed once per scraped_jobs.json mtime)
# ---------------------------
_JOBS_ENRICH_CACHE: Optional[Tuple[float, List[Dict[str, Any]], Dict[str, Dict[str, Any]]]] = None

def get_jobs_enriched() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    global _JOBS_ENRICH_CACHE, _SUPABASE_JOBS_CACHE

    if SUPABASE_DB is not None:
        now_ts = time.time()
        if _SUPABASE_JOBS_CACHE and (now_ts - _SUPABASE_JOBS_CACHE[0]) < SUPABASE_JOBS_CACHE_TTL_S:
            return _SUPABASE_JOBS_CACHE[1], _SUPABASE_JOBS_CACHE[2]
        try:
            raw = SUPABASE_DB.select("jobs", order="posted_date.desc.nullslast,title.asc", limit=5000)
            out_list: List[Dict[str, Any]] = []
            out_map: Dict[str, Dict[str, Any]] = {}
            for j in raw:
                if not isinstance(j, dict):
                    continue
                jj = dict(j)
                jj["country"] = str(j.get("country") or infer_country(str(j.get("location", ""))))
                jj["work_mode"] = str(j.get("work_mode") or infer_work_mode(
                    str(j.get("title", "")),
                    str(j.get("location", "")),
                    str(j.get("description_text", "")),
                ))
                out_list.append(jj)
                jid = str(j.get("job_id", ""))
                if jid:
                    out_map[jid] = jj
            _SUPABASE_JOBS_CACHE = (now_ts, out_list, out_map)
            return out_list, out_map
        except Exception as e:
            _rt_append(LAST_STDERR, f"Supabase jobs fallback to JSON: {repr(e)}")

    global _JOBS_ENRICH_CACHE
    if not SCRAPED_JOBS_JSON.exists():
        return [], {}
    m = SCRAPED_JOBS_JSON.stat().st_mtime
    if _JOBS_ENRICH_CACHE and _JOBS_ENRICH_CACHE[0] == m:
        return _JOBS_ENRICH_CACHE[1], _JOBS_ENRICH_CACHE[2]

    raw = safe_load_json(SCRAPED_JOBS_JSON, [])
    out_list: List[Dict[str, Any]] = []
    out_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, list):
        for j in raw:
            if not isinstance(j, dict):
                continue
            jj = dict(j)
            jj["country"] = infer_country(str(j.get("location", "")))
            jj["work_mode"] = infer_work_mode(
                str(j.get("title", "")),
                str(j.get("location", "")),
                str(j.get("description_text", "")),
            )
            out_list.append(jj)
            jid = str(j.get("job_id", ""))
            if jid:
                out_map[jid] = jj

    _JOBS_ENRICH_CACHE = (m, out_list, out_map)
    return out_list, out_map


# ---------------------------
# Speed: grouped matches cache (per resume) once per matches.json mtime
# ---------------------------
_MATCH_GROUP_CACHE: Optional[Tuple[float, Dict[str, List[Dict[str, Any]]], List[str]]] = None

def get_matches_grouped() -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    global _MATCH_GROUP_CACHE
    if not MATCHES_JSON.exists():
        return {}, []
    m = MATCHES_JSON.stat().st_mtime
    if _MATCH_GROUP_CACHE and _MATCH_GROUP_CACHE[0] == m:
        return _MATCH_GROUP_CACHE[1], _MATCH_GROUP_CACHE[2]

    matches = safe_load_json(MATCHES_JSON, [])
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    if isinstance(matches, list):
        for row in matches:
            if not isinstance(row, dict):
                continue
            rid = str(row.get("resume_id", ""))
            if not rid:
                continue
            grouped.setdefault(rid, []).append(row)

    for rid, rows in grouped.items():
        rows.sort(key=lambda x: float(x.get("final_match_percent", 0.0)), reverse=True)

    resume_ids = sorted(grouped.keys())
    _MATCH_GROUP_CACHE = (m, grouped, resume_ids)
    return grouped, resume_ids


def resume_id_to_name() -> Dict[str, str]:
    data = safe_load_json(SCANNED_RESUMES_JSON, [])
    out: Dict[str, str] = {}
    if isinstance(data, list):
        for r in data:
            if isinstance(r, dict):
                rid = str(r.get("resume_id", ""))
                name = r.get("display_name") or r.get("original_filename") or r.get("file_name") or r.get("filename") or r.get("file") or rid
                if rid:
                    out[rid] = str(name)
    return out


def _tail_file(path: Path, max_chars: int = 6000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        return text[-max_chars:]
    except Exception:
        return ""


def parse_job_date(value: Any):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    fmts = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            pass

    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def date_filter_match(posted_value: Any, mode: str) -> bool:
    mode = (mode or "all").strip().lower()
    if mode == "all":
        return True

    dt = parse_job_date(posted_value)
    if dt is None:
        return False

    today = datetime.utcnow().date()
    if mode == "week":
        return dt >= (today - timedelta(days=7))
    if mode == "month":
        return dt >= (today - timedelta(days=30))
    return True


def page_label(page_num: int) -> str:
    start_rank = (page_num - 1) * 10 + 1
    end_rank = page_num * 10
    return f"{start_rank}-{end_rank}"


# ---------------------------
# Pipeline runner + smooth progress
# ---------------------------
RUN_LOCK = threading.RLock()
RUN_THREAD: Optional[threading.Thread] = None

RUN_STATE: Dict[str, Any] = {
    "running": False,
    "started_epoch": None,
    "message": "Idle",
    "error": "",
    "current_step": None,
    "current_step_started_epoch": None,
    "step_index": 0,
    "total_steps": 0,
}

EXPECTED_STEP_S = {
    "scrape_jobs.py": 45,
    "resume_scraper.py": 60,
    "job_matcher.py": 45,
    "url_scraper.py": 30,
}

def _rt_init() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    if not LAST_STDOUT.exists():
        LAST_STDOUT.write_text("", encoding="utf-8")
    if not LAST_STDERR.exists():
        LAST_STDERR.write_text("", encoding="utf-8")

def _rt_reset() -> None:
    _rt_init()
    LAST_STDOUT.write_text("", encoding="utf-8")
    LAST_STDERR.write_text("", encoding="utf-8")

def _rt_append(path: Path, line: str) -> None:
    _rt_init()
    with path.open("a", encoding="utf-8", errors="ignore") as f:
        f.write(line)
        if not line.endswith("\n"):
            f.write("\n")

def run_script_streaming(script_path: Path, timeout_s: int = 1800) -> Dict[str, Any]:
    if not script_path.exists():
        _rt_append(LAST_STDERR, f"Missing file: {script_path.name}")
        return {"ok": False, "returncode": None}

    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    cmd = [sys.executable, "-u", str(script_path)]
    _rt_append(LAST_STDOUT, f"\n===== START {script_path.name} @ {datetime.now().strftime('%Y-%m-%d %I:%M %p')} =====")

    start = time.time()
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env,
    )

    timed_out = False

    def pump(stream, out_path: Path):
        try:
            for line in iter(stream.readline, ""):
                _rt_append(out_path, line.rstrip("\n"))
        finally:
            try:
                stream.close()
            except Exception:
                pass

    t_out = threading.Thread(target=pump, args=(proc.stdout, LAST_STDOUT), daemon=True)
    t_err = threading.Thread(target=pump, args=(proc.stderr, LAST_STDERR), daemon=True)
    t_out.start()
    t_err.start()

    while True:
        if proc.poll() is not None:
            break
        if (time.time() - start) > timeout_s:
            timed_out = True
            try:
                proc.kill()
            except Exception:
                pass
            break
        time.sleep(0.25)

    t_out.join(timeout=2)
    t_err.join(timeout=2)

    rc = proc.returncode if not timed_out else None
    ok = (rc == 0) and (not timed_out)
    _rt_append(LAST_STDOUT, f"===== END {script_path.name} (ok={ok}, rc={rc}) =====\n")
    return {"ok": ok, "returncode": rc}

def smooth_progress_pct() -> float:
    with RUN_LOCK:
        total = int(RUN_STATE.get("total_steps") or 0)
        if total <= 0:
            return 0.0

        running = bool(RUN_STATE.get("running"))
        msg = str(RUN_STATE.get("message") or "").lower()
        idx = int(RUN_STATE.get("step_index") or 0)
        cur = RUN_STATE.get("current_step")
        cur_start = RUN_STATE.get("current_step_started_epoch")

        if (not running) and msg.startswith("finished"):
            return 100.0
        if (not running) and msg.startswith("failed"):
            completed = max(0, idx - 1)
            return min(99.0, (completed / total) * 100.0)

        completed = max(0, idx - 1)
        in_step = 0.0

        if running and cur and cur_start:
            elapsed = max(0.0, time.time() - float(cur_start))
            expected = float(EXPECTED_STEP_S.get(str(cur), 45.0))

            base = min(0.95, (elapsed / expected) * 0.95)

            if elapsed > expected:
                creep = min(0.04, (elapsed - expected) / (expected * 6.0) * 0.04)
                base = min(0.99, 0.95 + creep)

            in_step = base

        return min(99.0, max(0.0, ((completed + in_step) / total) * 100.0))

def start_background_pipeline(do_job_scraper: bool, do_resume_scraper: bool, do_job_matcher: bool) -> bool:
    global RUN_THREAD
    with RUN_LOCK:
        if RUN_STATE["running"]:
            return False

        steps: List[Path] = []
        if do_job_scraper:
            steps.append(JOB_SCRAPER)
        if do_resume_scraper:
            steps.append(RESUME_SCRAPER)
        if do_job_matcher:
            steps.append(JOB_MATCHER)

        RUN_STATE["running"] = True
        RUN_STATE["started_epoch"] = time.time()
        RUN_STATE["message"] = "Running"
        RUN_STATE["error"] = ""
        RUN_STATE["current_step"] = None
        RUN_STATE["current_step_started_epoch"] = None
        RUN_STATE["step_index"] = 0
        RUN_STATE["total_steps"] = len(steps)

        _rt_reset()

        def runner():
            try:
                for i, script in enumerate(steps):
                    with RUN_LOCK:
                        RUN_STATE["current_step"] = script.name
                        RUN_STATE["current_step_started_epoch"] = time.time()
                        RUN_STATE["step_index"] = i + 1

                    r = run_script_streaming(script, timeout_s=1800)
                    if not r.get("ok"):
                        with RUN_LOCK:
                            RUN_STATE["running"] = False
                            RUN_STATE["message"] = "Failed"
                            RUN_STATE["error"] = f"{script.name} failed"
                            RUN_STATE["current_step_started_epoch"] = None
                        return

                with RUN_LOCK:
                    RUN_STATE["running"] = False
                    RUN_STATE["message"] = "Finished"
                    RUN_STATE["error"] = ""
                    RUN_STATE["current_step"] = None
                    RUN_STATE["current_step_started_epoch"] = None
            except Exception as e:
                _rt_append(LAST_STDERR, f"Runner exception: {repr(e)}")
                with RUN_LOCK:
                    RUN_STATE["running"] = False
                    RUN_STATE["message"] = "Failed"
                    RUN_STATE["error"] = repr(e)
                    RUN_STATE["current_step"] = None
                    RUN_STATE["current_step_started_epoch"] = None

        RUN_THREAD = threading.Thread(target=runner, daemon=True)
        RUN_THREAD.start()
        return True


# ---------------------------
# Upload helper: image -> PDF
# ---------------------------
def convert_image_to_pdf(image_path: Path, out_pdf_path: Path) -> None:
    img = Image.open(image_path)
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    elif img.mode != "RGB":
        img = img.convert("RGB")
    out_pdf_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_pdf_path, "PDF", resolution=150.0)


# ---------------------------
# Auth
# ---------------------------
@app.get("/auth/signup")
def signup_page():
    return render_template("signup.html", title="Sign up")


@app.post("/auth/signup")
def signup_submit():
    if SUPABASE_AUTH is None:
        flash("Supabase auth is not configured yet. Add SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY in Render.")
        return redirect(url_for("signup_page"))

    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password") or ""
    confirm = request.form.get("confirm_password") or ""

    if not email:
        flash("Please enter an email address.")
        return redirect(url_for("signup_page"))
    if len(password) < 8:
        flash("Password must be at least 8 characters.")
        return redirect(url_for("signup_page"))
    if password != confirm:
        flash("Passwords do not match.")
        return redirect(url_for("signup_page"))

    try:
        SUPABASE_AUTH.sign_up(email, password, email_redirect_to=auth_redirect_target())
        flash("Account created. Check your email and click the verification link, then sign in.")
        return redirect(url_for("login_page"))
    except Exception as e:
        flash(f"Sign up failed: {e}")
        return redirect(url_for("signup_page"))


@app.get("/auth/login")
def login_page():
    if request.args.get("confirmed") == "1":
        flash("Email confirmed. You can sign in now.")
    return render_template("login.html", title="Sign in", next_url=(request.args.get("next") or ""))


@app.post("/auth/login")
def login_submit():
    if SUPABASE_AUTH is None:
        flash("Supabase auth is not configured yet. Add SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY in Render.")
        return redirect(url_for("login_page"))

    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password") or ""
    next_url = (request.form.get("next") or "").strip()
    if not email or not password:
        flash("Enter your email and password.")
        return redirect(url_for("login_page", next=next_url))

    try:
        auth_payload = SUPABASE_AUTH.sign_in_password(email, password)
        set_logged_in_session(auth_payload)
        flash("Signed in.")
        if next_url and next_url.startswith("/"):
            return redirect(next_url)
        return redirect(url_for("dashboard"))
    except Exception as e:
        flash(f"Sign in failed: {e}")
        return redirect(url_for("login_page", next=next_url))


@app.post("/auth/logout")
def logout_submit():
    session.pop("user_id", None)
    session.pop("user_email", None)
    session.pop("access_token", None)
    session.pop("refresh_token", None)
    flash("Signed out.")
    return redirect(url_for("login_page"))


# ---------------------------
# Pages
# ---------------------------
@app.get("/")
def dashboard():
    scanned = safe_load_json(SCANNED_RESUMES_JSON, [])
    matches = safe_load_json(MATCHES_JSON, [])

    jobs_count = 0
    if SUPABASE_DB is not None:
        try:
            jobs_count = SUPABASE_DB.count("jobs", count_column="job_id")
        except Exception as e:
            _rt_append(LAST_STDERR, f"Supabase jobs count failed: {repr(e)}")
    if jobs_count == 0:
        jobs = safe_load_json(SCRAPED_JOBS_JSON, [])
        jobs_count = len(jobs) if isinstance(jobs, list) else 0

    stats = {
        "resumes_uploaded": get_user_active_resume_count(get_current_user_id()),
        "resumes_scanned": len(scanned) if isinstance(scanned, list) else 0,
        "jobs_scraped": jobs_count,
        "matches_rows": len(matches) if isinstance(matches, list) else 0,
    }
    return render_template("dashboard.html", stats=stats, logged_in=is_logged_in())

@app.get("/upload")
@login_required
def upload_page():
    resumes = get_active_resume_items()
    current_user_id = get_current_user_id()
    return render_template("upload.html", resumes=resumes, max_resumes=MAX_RESUMES, current_resume_count=get_user_active_resume_count(current_user_id))

@app.post("/upload/resume")
@login_required
def upload_resume():
    if "file" not in request.files:
        flash("No file selected.")
        return redirect(url_for("upload_page"))

    f = request.files["file"]
    if not f or f.filename.strip() == "":
        flash("No file selected.")
        return redirect(url_for("upload_page"))

    ext = Path(f.filename).suffix.lower()
    if ext not in ALLOWED_RESUME_EXTS:
        flash(f"Unsupported file type: {ext}")
        return redirect(url_for("upload_page"))

    RESUMES_DIR.mkdir(parents=True, exist_ok=True)
    current_user_id = get_current_user_id()
    active_resume_count = get_user_active_resume_count(current_user_id)
    if active_resume_count >= MAX_RESUMES:
        flash(f"You can only keep up to {MAX_RESUMES} resumes for now. Delete one before uploading another.")
        return redirect(url_for("upload_page"))

    safe_stem = secure_filename(Path(f.filename).stem) or "resume"
    uploaded_at = datetime.now().isoformat()
    stored_filename = make_timestamped_pdf_name(f.filename)
    out_pdf = RESUMES_DIR / stored_filename

    try:
        if ext in ALLOWED_IMAGE_EXTS or ext in {".tif", ".tiff"}:
            temp_name = f"__tmp__{secure_filename(f.filename)}"
            temp_path = RESUMES_DIR / temp_name
            f.save(str(temp_path))
            try:
                convert_image_to_pdf(temp_path, out_pdf)
            finally:
                temp_path.unlink(missing_ok=True)
            flash(f"Uploaded image → converted to PDF: {safe_stem}.pdf")
        else:
            f.save(str(out_pdf))
            flash(f"Uploaded: {safe_stem}.pdf")
    except Exception as e:
        out_pdf.unlink(missing_ok=True)
        flash(f"Upload failed: {e}")
        return redirect(url_for("upload_page"))

    manifest = [row for row in load_resume_manifest() if isinstance(row, dict)]
    manifest.append({
        "stored_filename": stored_filename,
        "display_stem": safe_stem,
        "uploaded_at": uploaded_at,
    })
    save_resume_manifest(manifest)

    if SUPABASE_DB is not None:
        try:
            row_payload = {
                "stored_filename": stored_filename,
                "file_name": f"{safe_stem}.pdf",
                "display_stem": safe_stem,
                "storage_path": str(out_pdf.relative_to(PROJECT_DIR)).replace("\\", "/"),
                "uploaded_at": uploaded_at,
                "archived": False,
                "archive_filename": None,
                "archived_at": None,
            }
            if current_user_id:
                row_payload["user_id"] = current_user_id
            SUPABASE_DB.upsert_many(
                "resumes",
                [row_payload],
                on_conflict="stored_filename",
            )
        except Exception as e:
            _rt_append(LAST_STDERR, f"Supabase resume metadata sync failed: {repr(e)}")

    return redirect(url_for("upload_page"))


@app.post("/upload/delete/<path:stored_filename>")
@login_required
def delete_resume(stored_filename: str):
    stored_filename = Path(stored_filename).name
    resume_path = RESUMES_DIR / stored_filename
    if not resume_path.exists():
        flash("Resume file was not found.")
        return redirect(url_for("upload_page"))

    manifest = [row for row in load_resume_manifest() if isinstance(row, dict)]
    row = next((r for r in manifest if str(r.get("stored_filename", "")) == stored_filename), None)
    remaining = [r for r in manifest if str(r.get("stored_filename", "")) != stored_filename]
    save_resume_manifest(remaining)

    scanned = safe_load_json(SCANNED_RESUMES_JSON, [])
    resume_id = "RES-" + file_sha1(resume_path)[:12]
    if isinstance(scanned, list):
        for item in scanned:
            if isinstance(item, dict) and str(item.get("file_name", "")) == stored_filename and str(item.get("resume_id", "")).strip():
                resume_id = str(item.get("resume_id"))
                break

    RESUME_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_name = stored_filename
    archive_path = RESUME_ARCHIVE_DIR / archive_name
    if archive_path.exists():
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{Path(stored_filename).stem}_{stamp}{Path(stored_filename).suffix}"
        archive_path = RESUME_ARCHIVE_DIR / archive_name

    shutil.move(str(resume_path), str(archive_path))

    archive_manifest = [r for r in load_resume_archive_manifest() if isinstance(r, dict)]
    archive_manifest.append({
        "stored_filename": stored_filename,
        "archive_filename": archive_name,
        "display_stem": str((row or {}).get("display_stem") or Path(stored_filename).stem),
        "uploaded_at": str((row or {}).get("uploaded_at") or datetime.now().isoformat()),
        "archived_at": datetime.now().isoformat(),
        "resume_id": resume_id,
    })
    save_resume_archive_manifest(archive_manifest)

    if SUPABASE_DB is not None:
        try:
            current_user_id = get_current_user_id()
            update_filters = {"stored_filename": f"eq.{stored_filename}"}
            if current_user_id:
                update_filters["user_id"] = f"eq.{current_user_id}"
            SUPABASE_DB.update(
                "resumes",
                {
                    "archived": True,
                    "archive_filename": archive_name,
                    "archived_at": datetime.now().isoformat(),
                },
                filters=update_filters,
            )
        except Exception as e:
            _rt_append(LAST_STDERR, f"Supabase resume archive sync failed: {repr(e)}")

    archive_resume_cache(resume_id)
    remove_resume_from_outputs(resume_id, stored_filename)
    flash("Resume archived.")
    return redirect(url_for("upload_page"))

@app.get("/run")
@login_required
def run_page():
    return render_template("run.html")

@app.post("/run/pipeline")
@login_required
def run_pipeline():
    # Start button always runs Resume OCR + Job Matcher together.
    # Job scraping is intentionally disabled from the UI.
    do_job_scraper = False
    do_resume_scraper = True
    do_job_matcher = True

    ok = start_background_pipeline(do_job_scraper, do_resume_scraper, do_job_matcher)
    if not ok:
        flash("Already running.")
    else:
        flash("Started. Resume OCR + Job Matcher are running now.")
    return redirect(url_for("run_page"))

@app.get("/results")
@login_required
def results_page():
    grouped, resume_ids = get_matches_grouped()
    rid_name = resume_id_to_name()
    _, job_map = get_jobs_enriched()

    selected = request.args.get("resume_id")
    if selected not in grouped and resume_ids:
        selected = resume_ids[0]

    country_filter = (request.args.get("country") or "").strip()
    work_mode_filter = (request.args.get("work_mode") or "").strip()
    posted_filter = (request.args.get("posted_range") or "all").strip().lower()
    if posted_filter not in {"all", "week", "month"}:
        posted_filter = "all"

    try:
        page = int(request.args.get("page") or "1")
    except Exception:
        page = 1
    page = max(1, page)

    rows = grouped.get(selected, []) if selected else []
    filtered: List[Dict[str, Any]] = []

    # Keep original closeness ordering from the matcher output
    for r in rows:
        jid = str(r.get("job_id", ""))
        src = job_map.get(jid, {})
        url = r.get("url") or src.get("source_url") or ""
        country = src.get("country") or infer_country(str(r.get("location", "")))
        posted_value = (
            r.get("posted_date")
            or src.get("posted_date")
            or r.get("date_posted")
            or src.get("date_posted")
            or src.get("collected_date")
            or r.get("collected_date")
            or ""
        )

        work_mode = src.get("work_mode") or infer_work_mode(
            str(r.get("title", "")),
            str(r.get("location", "")),
            str(src.get("description_text", "")),
        )

        if country_filter and country_filter != country:
            continue
        if work_mode_filter and work_mode_filter != work_mode:
            continue
        if not date_filter_match(posted_value, posted_filter):
            continue

        filtered.append({
            **r,
            "best_url": url,
            "country": country,
            "work_mode": work_mode,
            "posted_date_display": str(posted_value) if posted_value else "Unknown",
        })

    total_results = len(filtered)
    total_pages = max(1, (total_results + 9) // 10) if total_results > 0 else 1
    page = min(page, total_pages)
    start_idx = (page - 1) * 10
    end_idx = start_idx + 10
    page_rows = filtered[start_idx:end_idx]

    page_options = [
        {"value": i, "label": page_label(i)}
        for i in range(1, total_pages + 1)
    ]

    return render_template(
        "results.html",
        resume_ids=resume_ids,
        selected_id=selected,
        resume_name_map=rid_name,
        rows=page_rows,
        countries=COUNTRY_LIST,
        work_modes=WORK_MODE_OPTIONS,
        country_filter=country_filter,
        work_mode_filter=work_mode_filter,
        posted_filter=posted_filter,
        page=page,
        page_options=page_options,
        total_results=total_results,
        range_start=(start_idx + 1 if total_results else 0),
        range_end=min(end_idx, total_results),
    )

@app.get("/jobs")
def jobs_page():
    jobs_list, _ = get_jobs_enriched()

    q = (request.args.get("q") or "").strip().lower()
    country_filter = (request.args.get("country") or "").strip()
    work_mode_filter = (request.args.get("work_mode") or "").strip()

    # Fast path (most common): no filters -> only show first 80
    if not q and not country_filter and not work_mode_filter:
        return render_template(
            "jobs.html",
            jobs=jobs_list[:80],
            q=q,
            countries=COUNTRY_LIST,
            work_modes=WORK_MODE_OPTIONS,
            country_filter=country_filter,
            work_mode_filter=work_mode_filter,
        )

    view: List[Dict[str, Any]] = []
    for j in jobs_list:
        title = str(j.get("title", "")).lower()
        company = str(j.get("company", "")).lower()
        loc = str(j.get("location", "")).lower()
        ctry = str(j.get("country", ""))
        work_mode = str(j.get("work_mode", "On-site"))

        if q and (q not in title and q not in company and q not in loc):
            continue
        if country_filter and country_filter != ctry:
            continue
        if work_mode_filter and work_mode_filter != work_mode:
            continue

        view.append(j)

    return render_template(
        "jobs.html",
        jobs=view,
        q=q,
        countries=COUNTRY_LIST,
        work_modes=WORK_MODE_OPTIONS,
        country_filter=country_filter,
        work_mode_filter=work_mode_filter,
    )


@app.get("/api/supabase-status")
def api_supabase_status():
    status = {
        "has_url": bool(os.environ.get("SUPABASE_URL")),
        "has_secret_key": bool(os.environ.get("SUPABASE_SECRET_KEY")),
        "has_publishable_key": bool(os.environ.get("SUPABASE_PUBLISHABLE_KEY")),
        "auth_configured": SUPABASE_AUTH is not None,
        "jobs_table_available": False,
        "jobs_count": 0,
        "resumes_table_available": False,
    }
    if SUPABASE_DB is None:
        return jsonify(status)
    try:
        status["jobs_count"] = SUPABASE_DB.count("jobs", count_column="job_id")
        status["jobs_table_available"] = True
    except Exception as e:
        status["jobs_error"] = repr(e)
    try:
        SUPABASE_DB.select("resumes", columns="id", limit=1)
        status["resumes_table_available"] = True
    except Exception as e:
        status["resumes_error"] = repr(e)
    return jsonify(status)

# ---------------------------
# API: status only (no /logs, no /download, no /api/tail)
# ---------------------------
@app.get("/api/status")
def api_status():
    with RUN_LOCK:
        running = bool(RUN_STATE.get("running"))
        started_epoch = RUN_STATE.get("started_epoch")
        elapsed = int(time.time() - started_epoch) if (running and started_epoch) else 0
        error = RUN_STATE.get("error", "")
        message = RUN_STATE.get("message", "Idle")

    error_detail = ""
    if (not running) and str(message).lower().startswith("failed"):
        error_detail = _tail_file(LAST_STDERR, max_chars=4000)

    return jsonify({
        "running": running,
        "message": message,
        "error": error,
        "error_detail": error_detail,
        "current_step": RUN_STATE.get("current_step"),
        "elapsed_s": elapsed,
        "progress_pct_smooth": round(smooth_progress_pct(), 1),
    })

if __name__ == "__main__":
    app.run(
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "5000")),
        debug=False,
        threaded=True,
        use_reloader=False,
    )
