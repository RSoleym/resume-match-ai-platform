from __future__ import annotations

import io
import importlib
import contextlib
import json
import os
import re
import sys
import tempfile
import time
import threading
import subprocess
import uuid
from datetime import date, datetime, timedelta, timezone
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
from matcher_taxonomy import enrich_job_record, normalize_country_name, canonicalize_work_mode, shortlist_jobs_for_resume
from shared_model_registry import warm_sentence_transformer
from web_ui.premium_openai import score_jobs_with_openai, search_live_jobs_with_openai, OpenAIConfigError

APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent


def load_local_env() -> None:
    candidates = [
        PROJECT_DIR / ".env",
        PROJECT_DIR / ".env.local",
        APP_DIR / ".env",
        APP_DIR / ".env.local",
    ]
    for env_path in candidates:
        if not env_path.exists():
            continue
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export "):].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                    value = value[1:-1]
                os.environ.setdefault(key, value)
        except Exception:
            continue


load_local_env()

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
MAX_RESUMES = 1
RESUME_MANIFEST_JSON = PROJECT_DIR / "resumes_manifest.json"
RESUME_ARCHIVE_DIR = PROJECT_DIR / "resume_archive"
RESUME_ARCHIVE_MANIFEST_JSON = RESUME_ARCHIVE_DIR / "archive_manifest.json"
MATCH_CACHE_DIR = PROJECT_DIR / "match_cache"
MATCH_CACHE_ARCHIVE_DIR = PROJECT_DIR / "match_cache_archive"

# Internal runtime logs (kept for debugging; NOT exposed in UI)
RUNTIME_DIR = APP_DIR / "runtime"
USER_WORKSPACES_DIR = RUNTIME_DIR / "user_workspaces"
LAST_STDOUT = RUNTIME_DIR / "last_stdout.txt"
LAST_STDERR = RUNTIME_DIR / "last_stderr.txt"

ALLOWED_RESUME_EXTS = {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff"}
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}
MAX_RESUME_FILE_BYTES = int(os.environ.get("MAX_RESUME_FILE_BYTES", str(1024 * 1024)))

SUPABASE_DB = get_supabase_db()
SUPABASE_AUTH = get_supabase_auth()
SUPABASE_RESUMES_BUCKET = (os.environ.get("SUPABASE_RESUMES_BUCKET") or "resumes").strip() or "resumes"
SUPABASE_ACTIVE_PREFIX = (os.environ.get("SUPABASE_ACTIVE_PREFIX") or "users").strip().strip("/") or "users"
SUPABASE_ARCHIVE_PREFIX = (os.environ.get("SUPABASE_ARCHIVE_PREFIX") or "archive").strip().strip("/") or "archive"
_SUPABASE_JOBS_CACHE: Optional[Tuple[float, List[Dict[str, Any]], Dict[str, Dict[str, Any]]]] = None
SUPABASE_JOBS_CACHE_TTL_S = int(os.environ.get("SUPABASE_JOBS_CACHE_TTL_S", "60"))
_JOBS_COUNT_CACHE: Optional[Tuple[float, int]] = None
_MODEL_WARMUP_LOCK = threading.Lock()
_MODEL_WARMUP_STATE: Dict[str, Any] = {"started": False, "done": False, "ok": False, "started_epoch": 0.0, "finished_epoch": 0.0, "message": "idle"}

PREMIUM_ACCESS_CODE = (os.environ.get("PREMIUM_ACCESS_CODE") or "").strip()
PREMIUM_ADMIN_CODE = (os.environ.get("PREMIUM_ADMIN_CODE") or "").strip()
PREMIUM_MAX_SEARCHES = max(1, int(os.environ.get("PREMIUM_MAX_SEARCHES", "1")))
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_WEB_MODEL = (os.environ.get("OPENAI_WEB_MODEL") or "gpt-5").strip() or "gpt-5"

app = Flask(__name__)

for _p in [RESUMES_DIR, RESUME_ARCHIVE_DIR, MATCH_CACHE_DIR, MATCH_CACHE_ARCHIVE_DIR, RUNTIME_DIR, USER_WORKSPACES_DIR]:
    _p.mkdir(parents=True, exist_ok=True)

app.secret_key = os.environ.get("SECRET_KEY", "dev-only-change-me")


def ensure_model_warmup_started() -> None:
    with _MODEL_WARMUP_LOCK:
        if _MODEL_WARMUP_STATE.get("started"):
            return
        _MODEL_WARMUP_STATE.update({"started": True, "done": False, "ok": False, "started_epoch": time.time(), "finished_epoch": 0.0, "message": "warming"})

        def _runner() -> None:
            ok = False
            msg = "warming"
            try:
                warm_sentence_transformer(os.environ.get("ROLEMATCHER_CATEGORY_MODEL", "all-MiniLM-L6-v2"))
                # warm prototype embeddings / caches in-process so first pipeline run is faster
                import semantic_role_classifier as _src
                import matcher_taxonomy as _mt
                try:
                    _src._get_label_embeddings("function", _src.FUNCTION_PROTOTYPES)
                    _src._get_label_embeddings("domain", _src.DOMAIN_PROTOTYPES)
                except Exception:
                    pass
                try:
                    _mt._get_category_semantic_state()
                except Exception:
                    pass
                ok = True
                msg = "warm"
            except Exception as e:
                msg = f"warm_failed: {e}"
            finally:
                with _MODEL_WARMUP_LOCK:
                    _MODEL_WARMUP_STATE.update({"done": True, "ok": ok, "finished_epoch": time.time(), "message": msg})

        threading.Thread(target=_runner, daemon=True, name="model-warmup").start()


class _RTWriter(io.TextIOBase):
    def __init__(self, out_path: Path):
        self.out_path = out_path
        self._buf = ""

    def write(self, s: str) -> int:
        if not s:
            return 0
        self._buf += str(s)
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            _rt_append(self.out_path, line)
        return len(s)

    def flush(self) -> None:
        if self._buf:
            _rt_append(self.out_path, self._buf)
            self._buf = ""


def run_module_streaming(module_name: str, func_name: str, *, extra_env: Optional[Dict[str, str]] = None, cwd: Optional[Path] = None) -> Dict[str, Any]:
    start = time.time()
    old_env: Dict[str, Optional[str]] = {}
    old_cwd = os.getcwd()
    out_writer = _RTWriter(LAST_STDOUT)
    err_writer = _RTWriter(LAST_STDERR)
    ok = False
    err: Optional[BaseException] = None
    try:
        if extra_env:
            for k, v in extra_env.items():
                old_env[k] = os.environ.get(k)
                os.environ[str(k)] = str(v)
        if cwd is not None:
            os.chdir(str(cwd))
        mod = importlib.import_module(module_name)
        mod = importlib.reload(mod)
        fn = getattr(mod, func_name)
        with contextlib.redirect_stdout(out_writer), contextlib.redirect_stderr(err_writer):
            fn()
        ok = True
        return {"ok": True, "returncode": 0, "elapsed_s": time.time() - start}
    except BaseException as e:
        err = e
        with contextlib.redirect_stderr(err_writer):
            print(f"Runner exception in {module_name}.{func_name}: {repr(e)}")
        return {"ok": False, "returncode": 1, "elapsed_s": time.time() - start, "error": repr(e)}
    finally:
        try:
            out_writer.flush()
            err_writer.flush()
        except Exception:
            pass
        if cwd is not None:
            os.chdir(old_cwd)
        if extra_env:
            for k, old in old_env.items():
                if old is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = old


@app.before_request
def _maybe_start_warmup() -> None:
    ensure_model_warmup_started()


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
    suffix = uuid.uuid4().hex[:6]
    return f"{safe_stem}_{stamp}_{suffix}.pdf"


def sanitize_user_key(user_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(user_id or "anon"))


def get_user_workspace_dir(user_id: str) -> Path:
    return USER_WORKSPACES_DIR / sanitize_user_key(user_id)


def get_user_workspace_paths(user_id: str, *, base_dir: Optional[Path] = None) -> Dict[str, Path]:
    base = base_dir or get_user_workspace_dir(user_id)
    return {
        "base": base,
        "resumes": base / "resumes",
        "scanned": base / "scanned_resumes.json",
        "manifest": base / "resumes_manifest.json",
        "matches": base / "resume_job_matches.json",
        "matches_db": base / "resume_job_matches.db",
        "jobs": base / "scraped_jobs.json",
        "match_cache": base / "match_cache",
        "match_cache_archive": base / "match_cache_archive",
    }


def make_active_storage_path(user_id: str, stored_filename: str) -> str:
    return f"{SUPABASE_ACTIVE_PREFIX}/{sanitize_user_key(user_id)}/active/{Path(stored_filename).name}"


def make_archive_storage_path(user_id: str, stored_filename: str, archived_at: Optional[datetime] = None) -> str:
    stamp = (archived_at or datetime.now(timezone.utc)).strftime("%Y%m%d_%H%M%S")
    stem = Path(stored_filename).stem
    suffix = Path(stored_filename).suffix or ".pdf"
    archive_name = f"{stem}_{stamp}{suffix}"
    return f"{SUPABASE_ARCHIVE_PREFIX}/{sanitize_user_key(user_id)}/{archive_name}"


def get_resume_storage_path(row: Dict[str, Any]) -> str:
    return str(row.get("storage_path") or "").strip()


def upload_resume_bytes_to_supabase(user_id: str, stored_filename: str, data: bytes, *, content_type: str = "application/pdf") -> str:
    if SUPABASE_DB is None:
        raise RuntimeError("Supabase DB is not configured.")
    object_path = make_active_storage_path(user_id, stored_filename)
    SUPABASE_DB.upload_bytes(SUPABASE_RESUMES_BUCKET, object_path, data, content_type=content_type, upsert=True)
    return object_path


def download_resume_bytes_from_supabase(row: Dict[str, Any]) -> bytes:
    if SUPABASE_DB is None:
        raise RuntimeError("Supabase DB is not configured.")
    object_path = get_resume_storage_path(row)
    if not object_path:
        raise RuntimeError("Resume storage_path is missing.")
    return SUPABASE_DB.download_bytes(SUPABASE_RESUMES_BUCKET, object_path)


def archive_resume_in_supabase(user_id: str, row: Dict[str, Any]) -> str:
    if SUPABASE_DB is None:
        raise RuntimeError("Supabase DB is not configured.")
    source_path = get_resume_storage_path(row)
    if not source_path:
        raise RuntimeError("Resume storage_path is missing.")
    dest_path = make_archive_storage_path(user_id, str(row.get("stored_filename") or "resume.pdf"))
    try:
        SUPABASE_DB.move_storage_object(SUPABASE_RESUMES_BUCKET, source_path, dest_path)
    except Exception:
        data = SUPABASE_DB.download_bytes(SUPABASE_RESUMES_BUCKET, source_path)
        SUPABASE_DB.upload_bytes(SUPABASE_RESUMES_BUCKET, dest_path, data, content_type="application/pdf", upsert=True)
        SUPABASE_DB.delete_storage_objects(SUPABASE_RESUMES_BUCKET, [source_path])
    return dest_path


def load_user_workspace_json(user_id: Optional[str], key: str, default: Any) -> Any:
    if not user_id:
        return default
    paths = get_user_workspace_paths(user_id)
    path = paths.get(key)
    if not isinstance(path, Path) or not path.exists():
        return default
    return safe_load_json(path, default)


def chunked(rows: List[Dict[str, Any]], size: int = 200) -> List[List[Dict[str, Any]]]:
    return [rows[i:i + size] for i in range(0, len(rows), size)]


def get_resume_local_path(stored_filename: str, storage_path: str = "") -> Optional[Path]:
    candidates = []
    if stored_filename:
        candidates.append(RESUMES_DIR / Path(stored_filename).name)
    if storage_path:
        candidates.append(PROJECT_DIR / storage_path)
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def parse_resume_scan_payload(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                return loaded
        except Exception:
            pass
        return {"resume_text": raw}
    return {}


def get_user_scanned_rows(user_id: Optional[str]) -> List[Dict[str, Any]]:
    if not user_id or SUPABASE_DB is None:
        return []
    try:
        rows = SUPABASE_DB.select(
            "resumes",
            filters={
                "user_id": f"eq.{user_id}",
                "archived": "eq.false",
                "parsed_text": "not.is.null",
            },
            order="uploaded_at.desc",
            limit=500,
        )
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase per-user scanned resume fetch failed: {repr(e)}")
        return []

    out: List[Dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        payload = parse_resume_scan_payload(row.get("parsed_text"))
        rid = str(row.get("id") or "").strip()
        stored_filename = str(row.get("stored_filename") or "").strip()
        file_name = str(row.get("file_name") or stored_filename or "resume.pdf")
        display_name = str(row.get("display_stem") or Path(file_name).stem)
        resume_text = str(payload.get("resume_text") or payload.get("text") or "")
        out.append({
            "resume_id": rid,
            "stored_filename": stored_filename,
            "file_name": stored_filename or file_name,
            "display_name": display_name,
            "display_stem": display_name,
            "summary": str(payload.get("summary") or ""),
            "education": str(payload.get("education") or ""),
            "skills": str(payload.get("skills") or ""),
            "experience": str(payload.get("experience") or ""),
            "projects": str(payload.get("projects") or ""),
            "resume_text": resume_text,
            "candidate_country": str(payload.get("candidate_country") or ""),
            "candidate_experience_years": payload.get("candidate_experience_years"),
            "candidate_degree_level": str(payload.get("candidate_degree_level") or "none"),
            "candidate_degree_family": str(payload.get("candidate_degree_family") or "General"),
            "candidate_degree_fields": payload.get("candidate_degree_fields") or [],
            "candidate_category": str(payload.get("candidate_category") or "General"),
            "candidate_category_confidence": payload.get("candidate_category_confidence"),
            "candidate_function": str(payload.get("candidate_function") or ""),
            "candidate_function_scores": payload.get("candidate_function_scores") or {},
            "candidate_domain": str(payload.get("candidate_domain") or ""),
            "candidate_domain_scores": payload.get("candidate_domain_scores") or {},
            "candidate_category_key": str(payload.get("candidate_category_key") or ""),
            "candidate_category_key_confidence": payload.get("candidate_category_key_confidence"),
            "collected_date": str(payload.get("collected_date") or str(row.get("uploaded_at") or "")[:10]),
        })
    return out


def get_user_match_rows(user_id: Optional[str]) -> List[Dict[str, Any]]:
    if not user_id or SUPABASE_DB is None:
        return []
    try:
        rows = SUPABASE_DB.select(
            "match_results",
            filters={"user_id": f"eq.{user_id}"},
            order="created_at.desc",
            limit=500,
        )
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase per-user matches fetch failed: {repr(e)}")
        return []

    out: List[Dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        resume_id = str(row.get("resume_id") or "").strip()
        raw_results = row.get("results_json")
        if not isinstance(raw_results, list):
            continue
        for idx, item in enumerate(raw_results, start=1):
            if not isinstance(item, dict):
                continue
            rr = dict(item)
            rr["resume_id"] = resume_id
            if not rr.get("rank"):
                rr["rank"] = idx
            out.append(rr)
    return out


def get_user_resume_name_map(user_id: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in get_user_resume_rows(user_id):
        if not isinstance(row, dict):
            continue
        rid = str(row.get("id") or "").strip()
        if not rid:
            continue
        name = row.get("display_stem") or Path(str(row.get("file_name") or row.get("stored_filename") or rid)).stem or rid
        out[rid] = str(name)
    return out


def stage_user_workspace(user_id: str) -> Tuple[Dict[str, Path], List[Dict[str, Any]], List[str]]:
    base = Path(tempfile.mkdtemp(prefix=f"rolematcher_{sanitize_user_key(user_id)}_"))
    paths = get_user_workspace_paths(user_id, base_dir=base)
    for key, path in paths.items():
        if key in {"base", "resumes", "match_cache", "match_cache_archive"}:
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)

    active_rows = get_user_resume_rows(user_id)
    manifest_rows: List[Dict[str, Any]] = []
    staged_files: List[str] = []
    for row in active_rows:
        if not isinstance(row, dict):
            continue
        stored_filename = str(row.get("stored_filename") or "").strip()
        if not stored_filename:
            continue
        try:
            resume_bytes = download_resume_bytes_from_supabase(row)
        except Exception as e:
            _rt_append(LAST_STDERR, f"Supabase resume download failed for {stored_filename}: {repr(e)}")
            continue
        dst = paths["resumes"] / stored_filename
        dst.write_bytes(resume_bytes)
        manifest_rows.append({
            "resume_id": str(row.get("id") or ""),
            "stored_filename": stored_filename,
            "display_stem": str(row.get("display_stem") or Path(stored_filename).stem),
            "uploaded_at": str(row.get("uploaded_at") or datetime.now(timezone.utc).isoformat()),
        })
        staged_files.append(stored_filename)

    jobs_rows, _ = get_jobs_enriched()
    safe_write_json(paths["manifest"], manifest_rows)
    safe_write_json(paths["scanned"], [])
    safe_write_json(paths["matches"], [])
    safe_write_json(paths["jobs"], jobs_rows if isinstance(jobs_rows, list) else [])
    return paths, active_rows, staged_files

def cleanup_workspace(workspace_paths: Optional[Dict[str, Path]]) -> None:
    if not workspace_paths:
        return
    base = workspace_paths.get("base")
    if isinstance(base, Path) and base.exists():
        shutil.rmtree(base, ignore_errors=True)


def sync_user_pipeline_outputs_to_supabase(user_id: str, workspace_paths: Dict[str, Path], active_rows: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    if SUPABASE_DB is None:
        return False, ["Supabase DB is not configured."]

    active_by_file: Dict[str, Dict[str, Any]] = {}
    for row in active_rows:
        if not isinstance(row, dict):
            continue
        stored_filename = str(row.get("stored_filename") or "").strip()
        if stored_filename:
            active_by_file[stored_filename] = row

    scanned_raw = safe_load_json(workspace_paths["scanned"], [])
    matches_raw = safe_load_json(workspace_paths["matches"], [])

    scan_payloads: Dict[str, Dict[str, Any]] = {}
    scan_hash_to_real: Dict[str, str] = {}
    for row in scanned_raw if isinstance(scanned_raw, list) else []:
        if not isinstance(row, dict):
            continue
        stored_filename = str(row.get("file_name") or "").strip()
        src = active_by_file.get(stored_filename)
        if not src:
            continue
        resume_id = str(src.get("id") or "").strip()
        scan_hash = str(row.get("resume_id") or "").strip()
        if scan_hash and resume_id:
            scan_hash_to_real[scan_hash] = resume_id
        if not resume_id:
            continue
        scan_payloads[resume_id] = {
            "summary": str(row.get("summary") or ""),
            "education": str(row.get("education") or ""),
            "skills": str(row.get("skills") or ""),
            "experience": str(row.get("experience") or ""),
            "projects": str(row.get("projects") or ""),
            "resume_text": str(row.get("resume_text") or ""),
            "collected_date": row.get("collected_date") or str(date.today()),
            "stored_filename": stored_filename,
            "display_name": str(src.get("display_stem") or Path(stored_filename).stem),
        }

    grouped_matches: Dict[str, List[Dict[str, Any]]] = {rid: [] for rid in scan_payloads.keys()}
    fallback_resume_id = next(iter(grouped_matches.keys()), "") if len(grouped_matches) == 1 else ""
    for row in matches_raw if isinstance(matches_raw, list) else []:
        if not isinstance(row, dict):
            continue
        candidate = str(row.get("resume_id") or "").strip()
        matched_resume_id = scan_hash_to_real.get(candidate) or (candidate if candidate in grouped_matches else "") or fallback_resume_id
        if not matched_resume_id:
            continue
        rr = dict(row)
        rr["resume_id"] = matched_resume_id
        grouped_matches.setdefault(matched_resume_id, []).append(rr)

    try:
        for row in active_rows:
            rid = str(row.get("id") or "").strip()
            if rid:
                SUPABASE_DB.update("resumes", {"parsed_text": None}, filters={"id": f"eq.{rid}", "user_id": f"eq.{user_id}"})
    except Exception as e:
        msg = f"Supabase resume OCR clear failed: {repr(e)}"
        issues.append(msg)
        _rt_append(LAST_STDERR, msg)

    for resume_id, payload in scan_payloads.items():
        try:
            SUPABASE_DB.update(
                "resumes",
                {"parsed_text": json.dumps(payload, ensure_ascii=False)},
                filters={"id": f"eq.{resume_id}", "user_id": f"eq.{user_id}"},
            )
        except Exception as e:
            msg = f"Supabase resume OCR sync failed for {resume_id}: {repr(e)}"
            issues.append(msg)
            _rt_append(LAST_STDERR, msg)

    try:
        SUPABASE_DB.delete("match_results", filters={"user_id": f"eq.{user_id}"})
    except Exception as e:
        msg = f"Supabase match_results clear failed: {repr(e)}"
        issues.append(msg)
        _rt_append(LAST_STDERR, msg)

    for resume_id, rows in grouped_matches.items():
        rows = sorted(rows, key=lambda x: (-float(x.get("final_match_percent", 0.0) or 0.0), -float(x.get("raw_match_percent", 0.0) or 0.0)))
        for idx, item in enumerate(rows, start=1):
            item["rank"] = idx
        try:
            SUPABASE_DB.insert_one(
                "match_results",
                {"user_id": user_id, "resume_id": resume_id, "results_json": rows},
            )
        except Exception as e:
            msg = f"Supabase match_results sync failed for {resume_id}: {repr(e)}"
            issues.append(msg)
            _rt_append(LAST_STDERR, msg)

    try:
        db_scanned_count = SUPABASE_DB.count(
            "resumes",
            filters={"user_id": f"eq.{user_id}", "archived": "eq.false", "parsed_text": "not.is.null"},
        )
        if scan_payloads and db_scanned_count == 0:
            msg = "Supabase resume OCR verification failed: no scanned resumes were saved for this user."
            issues.append(msg)
            _rt_append(LAST_STDERR, msg)
    except Exception as e:
        msg = f"Supabase resume OCR verification failed: {repr(e)}"
        issues.append(msg)
        _rt_append(LAST_STDERR, msg)

    try:
        db_match_count = SUPABASE_DB.count("match_results", filters={"user_id": f"eq.{user_id}"})
        if any(grouped_matches.values()) and db_match_count == 0:
            msg = "Supabase match_results verification failed: no match rows were saved for this user."
            issues.append(msg)
            _rt_append(LAST_STDERR, msg)
    except Exception as e:
        msg = f"Supabase match_results verification failed: {repr(e)}"
        issues.append(msg)
        _rt_append(LAST_STDERR, msg)

    return len(issues) == 0, issues

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


def get_pipeline_country_preferences() -> Tuple[str, List[str]]:
    mode = str(session.get("pipeline_location_mode") or "current").strip().lower()
    if mode not in {"current", "all", "selected"}:
        mode = "current"
    selected = session.get("pipeline_selected_countries")
    if not isinstance(selected, list):
        selected = []
    selected = [str(x).strip() for x in selected if str(x).strip()]
    return mode, selected


def save_pipeline_country_preferences(mode: str, selected: List[str]) -> None:
    session["pipeline_location_mode"] = mode
    session["pipeline_selected_countries"] = selected

def allowed_result_countries(location_mode: str, selected_countries: List[str], scanned_rows: List[Dict[str, Any]], selected_resume_id: Optional[str]) -> List[str]:
    mode = (location_mode or "current").strip().lower()
    selected_clean = [normalize_country_name(c) for c in selected_countries if normalize_country_name(c)]
    selected_clean = list(dict.fromkeys(selected_clean))
    scanned_map = {str(r.get("resume_id") or "").strip(): r for r in scanned_rows if isinstance(r, dict)}

    if mode == "all":
        return [c for c in COUNTRY_LIST if c != "Remote"]
    if mode == "selected" and selected_clean:
        return selected_clean

    chosen_resume = scanned_map.get(str(selected_resume_id or "").strip(), {})
    candidate_country = normalize_country_name(str(chosen_resume.get("candidate_country") or ""))
    if candidate_country and candidate_country != "Remote":
        return [candidate_country]

    all_scanned_countries = [normalize_country_name(str(r.get("candidate_country") or "")) for r in scanned_rows if isinstance(r, dict)]
    all_scanned_countries = [c for c in all_scanned_countries if c and c != "Remote"]
    all_scanned_countries = list(dict.fromkeys(all_scanned_countries))
    if all_scanned_countries:
        return all_scanned_countries

    return [c for c in selected_clean if c != "Remote"]


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
    base = (request.url_root or "").rstrip("/")
    return f"{base}{url_for('auth_confirmed')}"


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
        "PREMIUM_MAX_SEARCHES": PREMIUM_MAX_SEARCHES,
    }


def get_user_profile_row(user_id: Optional[str]) -> Dict[str, Any]:
    if not user_id or SUPABASE_DB is None:
        return {}
    try:
        rows = SUPABASE_DB.select("profiles", filters={"id": f"eq.{user_id}"}, limit=1)
        if rows and isinstance(rows[0], dict):
            return rows[0]
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase profile fetch failed: {repr(e)}")
    return {}


def update_user_profile(user_id: Optional[str], values: Dict[str, Any]) -> bool:
    if not user_id or SUPABASE_DB is None:
        return False
    try:
        SUPABASE_DB.update("profiles", values, filters={"id": f"eq.{user_id}"})
        return True
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase profile update failed: {repr(e)}")
        return False


def get_user_premium_state(user_id: Optional[str]) -> Dict[str, Any]:
    row = get_user_profile_row(user_id)
    searches_used = 0
    try:
        searches_used = int(row.get("premium_searches_used") or 0)
    except Exception:
        searches_used = 0
    premium_access = bool(row.get("premium_access"))
    admin_access = bool(row.get("premium_admin_access"))
    locked = bool(premium_access and (not admin_access) and searches_used >= PREMIUM_MAX_SEARCHES)
    remaining = max(0, PREMIUM_MAX_SEARCHES - searches_used)
    return {
        "premium_access": premium_access,
        "premium_granted_at": row.get("premium_granted_at"),
        "premium_source": row.get("premium_source"),
        "premium_admin_access": admin_access,
        "premium_admin_granted_at": row.get("premium_admin_granted_at"),
        "premium_admin_source": row.get("premium_admin_source"),
        "premium_searches_used": searches_used,
        "premium_searches_remaining": remaining,
        "premium_locked": locked,
    }


def increment_premium_search_count(user_id: Optional[str]) -> bool:
    if not user_id:
        return False
    state = get_user_premium_state(user_id)
    used = int(state.get("premium_searches_used") or 0) + 1
    return update_user_profile(user_id, {"premium_searches_used": used, "premium_last_run_at": datetime.now(timezone.utc).isoformat()})


def get_user_premium_rows(user_id: Optional[str]) -> List[Dict[str, Any]]:
    if not user_id or SUPABASE_DB is None:
        return []
    try:
        rows = SUPABASE_DB.select("premium_match_results", filters={"user_id": f"eq.{user_id}"}, order="created_at.desc", limit=100)
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase premium results fetch failed: {repr(e)}")
        return []
    out: List[Dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        resume_id = str(row.get("resume_id") or "").strip()
        raw_results = row.get("results_json")
        if not isinstance(raw_results, list):
            continue
        for idx, item in enumerate(raw_results, start=1):
            if not isinstance(item, dict):
                continue
            rr = dict(item)
            rr["resume_id"] = resume_id
            rr["premium_created_at"] = row.get("created_at")
            filters_json = row.get("filters_json") if isinstance(row.get("filters_json"), dict) else {}
            rr["premium_filters_json"] = filters_json
            if not rr.get("rank"):
                rr["rank"] = idx
            out.append(rr)
    return out


def get_premium_grouped() -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    user_id = get_current_user_id()
    rows = get_user_premium_rows(user_id)
    scanned_rows = get_user_scanned_rows(user_id)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("resume_id") or "").strip()
        if not rid:
            continue
        grouped.setdefault(rid, []).append(dict(row))
    for rid, rr in grouped.items():
        rr.sort(key=lambda x: float(x.get("final_match_percent", 0.0) or 0.0), reverse=True)
    resume_ids = [str(r.get("resume_id") or "").strip() for r in scanned_rows if isinstance(r, dict) and str(r.get("resume_id") or "").strip()]
    if not resume_ids:
        resume_ids = sorted(grouped.keys())
    else:
        seen: set[str] = set()
        resume_ids = [rid for rid in resume_ids if not (rid in seen or seen.add(rid))]
        for rid in sorted(grouped.keys()):
            if rid not in seen:
                resume_ids.append(rid)
                seen.add(rid)
    return grouped, resume_ids


def save_user_premium_results(user_id: str, resume_id: str, filters_payload: Dict[str, Any], rows: List[Dict[str, Any]]) -> bool:
    if SUPABASE_DB is None:
        return False
    try:
        SUPABASE_DB.delete("premium_match_results", filters={"user_id": f"eq.{user_id}", "resume_id": f"eq.{resume_id}"})
    except Exception:
        pass
    try:
        SUPABASE_DB.insert_one("premium_match_results", {"user_id": user_id, "resume_id": resume_id, "filters_json": filters_payload, "results_json": rows})
        return True
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase premium results save failed: {repr(e)}")
        return False


def get_scanned_resume_row_map(user_id: Optional[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in get_user_scanned_rows(user_id):
        if not isinstance(row, dict):
            continue
        rid = str(row.get("resume_id") or "").strip()
        if rid:
            out[rid] = row
    return out


def build_premium_resume_context(scan_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "candidate_country": str(scan_row.get("candidate_country") or ""),
        "candidate_experience_years": scan_row.get("candidate_experience_years"),
        "candidate_degree_level": str(scan_row.get("candidate_degree_level") or "none"),
        "candidate_degree_family": str(scan_row.get("candidate_degree_family") or "General"),
        "candidate_degree_fields": scan_row.get("candidate_degree_fields") or [],
        "candidate_category": str(scan_row.get("candidate_category") or "General"),
        "candidate_category_key": str(scan_row.get("candidate_category_key") or ""),
        "candidate_category_confidence": scan_row.get("candidate_category_confidence"),
        "candidate_category_key_confidence": scan_row.get("candidate_category_key_confidence"),
        "candidate_function": str(scan_row.get("candidate_function") or ""),
        "candidate_function_scores": scan_row.get("candidate_function_scores") or {},
        "candidate_domain": str(scan_row.get("candidate_domain") or ""),
        "candidate_domain_scores": scan_row.get("candidate_domain_scores") or {},
        "summary": str(scan_row.get("summary") or ""),
        "education": str(scan_row.get("education") or ""),
        "skills": str(scan_row.get("skills") or ""),
        "experience": str(scan_row.get("experience") or ""),
        "projects": str(scan_row.get("projects") or ""),
        "resume_text": str(scan_row.get("resume_text") or ""),
    }


def apply_job_filters(rows: List[Dict[str, Any]], *, country_filter: str, work_mode_filter: str, posted_filter: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        country = normalize_country_name(str(row.get("country") or infer_country(str(row.get("location") or ""))))
        work_mode = canonicalize_work_mode(str(row.get("work_mode") or infer_work_mode(str(row.get("title") or ""), str(row.get("location") or ""), str(row.get("description_text") or ""))))
        posted_value = row.get("posted_date") or row.get("date_posted") or row.get("collected_date") or ""
        if country_filter and country != country_filter:
            continue
        if work_mode_filter and work_mode != work_mode_filter:
            continue
        if not date_filter_match(posted_value, posted_filter):
            continue
        row["country"] = country
        row["work_mode"] = work_mode
        out.append(row)
    return out


def build_premium_result_rows(resume_id: str, shortlisted_jobs: List[Dict[str, Any]], scored: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    score_map = {str(x.get("job_id") or "").strip(): x for x in scored if isinstance(x, dict) and str(x.get("job_id") or "").strip()}
    rows: List[Dict[str, Any]] = []
    for idx, job in enumerate(shortlisted_jobs, start=1):
        jid = str(job.get("job_id") or f"AUTO-{idx:05d}").strip()
        payload = score_map.get(jid, {})
        score = payload.get("match_percentage")
        try:
            score_f = max(0.0, min(100.0, float(score)))
        except Exception:
            score_f = 0.0
        rows.append({
            "resume_id": resume_id,
            "job_id": jid,
            "rank": idx,
            "title": str(job.get("title") or ""),
            "company": str(job.get("company") or ""),
            "location": str(job.get("location") or ""),
            "country": str(job.get("country") or ""),
            "work_mode": canonicalize_work_mode(str(job.get("work_mode") or "On-site")),
            "job_category": str(job.get("job_category") or "General"),
            "raw_match_percent": round(score_f, 2),
            "final_match_percent": round(score_f, 2),
            "penalty_applied": False,
            "url": str(job.get("source_url") or job.get("url") or job.get("job_url") or ""),
            "posted_date": str(job.get("posted_date") or ""),
            "posted_date_display": str(job.get("posted_date") or "Unknown") if str(job.get("posted_date") or "").strip() else "Unknown",
            "premium_reason": str(payload.get("reason") or ""),
            "premium_model": OPENAI_MODEL,
            "prefilter_score": job.get("prefilter_score"),
        })
    rows.sort(key=lambda x: (-float(x.get("final_match_percent", 0.0) or 0.0), -float(x.get("prefilter_score") or 0.0)))
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return rows[:250]



def build_premium_live_result_rows(resume_id: str, live_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, job in enumerate(live_rows, start=1):
        try:
            score_f = max(0.0, min(100.0, float(job.get("match_percentage") or 0.0)))
        except Exception:
            score_f = 0.0
        url = str(job.get("source_url") or job.get("url") or "")
        title = str(job.get("title") or "")
        company = str(job.get("company") or "")
        location = str(job.get("location") or "")
        country = normalize_country_name(str(job.get("country") or infer_country(location)))
        rows.append({
            "resume_id": resume_id,
            "job_id": str(job.get("job_id") or url or f"WEB-{idx:05d}"),
            "rank": idx,
            "title": title,
            "company": company,
            "location": location,
            "country": country,
            "work_mode": canonicalize_work_mode(str(job.get("work_mode") or infer_work_mode(title, location, ""))),
            "job_category": str(job.get("job_category") or job.get("job_function") or "General"),
            "raw_match_percent": round(score_f, 2),
            "final_match_percent": round(score_f, 2),
            "penalty_applied": False,
            "url": url,
            "posted_date": str(job.get("posted_date") or ""),
            "posted_date_display": str(job.get("posted_date") or "Unknown") if str(job.get("posted_date") or "").strip() else "Unknown",
            "premium_reason": str(job.get("reason") or ""),
            "premium_model": str(job.get("search_model") or OPENAI_WEB_MODEL),
            "prefilter_score": score_f,
        })
    rows.sort(key=lambda x: (-float(x.get("final_match_percent", 0.0) or 0.0), str(x.get("title") or "").lower()))
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return rows[:250]

def get_user_premium_resume_ids() -> List[str]:
    return [str(r.get("resume_id") or "").strip() for r in get_user_scanned_rows(get_current_user_id()) if isinstance(r, dict) and str(r.get("resume_id") or "").strip()]


def get_user_premium_resume_name_map() -> Dict[str, str]:
    scanned_map = get_scanned_resume_row_map(get_current_user_id())
    out: Dict[str, str] = {}
    for rid, row in scanned_map.items():
        out[rid] = str(row.get("display_stem") or row.get("display_name") or rid)
    return out


def get_default_premium_scan_row(user_id: Optional[str]) -> Optional[Dict[str, Any]]:
    rows = get_user_scanned_rows(user_id)
    return rows[0] if rows else None


def get_default_premium_resume_id(user_id: Optional[str]) -> str:
    row = get_default_premium_scan_row(user_id)
    return str(row.get("resume_id") or "").strip() if isinstance(row, dict) else ""


def get_user_active_premium_resume_count(user_id: Optional[str]) -> int:
    try:
        return len(get_user_premium_resume_ids())
    except Exception:
        return 0


def get_user_active_resume_count(user_id: Optional[str]) -> int:
    if not user_id or SUPABASE_DB is None:
        return 0
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
        return 0


def get_user_match_count(user_id: Optional[str]) -> int:
    try:
        return len(get_user_match_rows(user_id))
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase per-user match count failed: {repr(e)}")
        return 0


def format_public_plus_count(value: int) -> str:
    value = max(0, int(value or 0))
    if value < 10:
        return str(value)
    magnitude = 10 ** (len(str(value)) - 1)
    return f"{(value // magnitude) * magnitude}+"


def get_total_scanned_resume_count() -> int:
    if SUPABASE_DB is None:
        return 0
    try:
        return SUPABASE_DB.count("resumes", filters={"archived": "eq.false", "parsed_text": "not.is.null"})
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase total scanned resume count failed: {repr(e)}")
        return 0


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
    if dt.tzinfo is None:
        now_dt = datetime.now()
    else:
        now_dt = datetime.now(dt.tzinfo)
    delta = now_dt - dt
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


def remove_resume_from_outputs(resume_id: str, stored_filename: str, display_stem: str = "") -> None:
    stored_filename = (stored_filename or "").strip()
    display_stem = (display_stem or "").strip()
    stored_stem = Path(stored_filename).stem if stored_filename else ""

    def _matches_resume_row(row: Dict[str, Any]) -> bool:
        row_resume_id = str(row.get("resume_id", "")).strip()
        row_file_name = str(row.get("file_name", "")).strip()
        row_stem = Path(row_file_name).stem if row_file_name else ""
        if resume_id and row_resume_id == resume_id:
            return True
        if stored_filename and row_file_name == stored_filename:
            return True
        if stored_stem and row_stem == stored_stem:
            return True
        if display_stem and row_stem == display_stem:
            return True
        return False

    scanned = safe_load_json(SCANNED_RESUMES_JSON, [])
    if isinstance(scanned, list):
        scanned = [row for row in scanned if not (isinstance(row, dict) and _matches_resume_row(row))]
        safe_write_json(SCANNED_RESUMES_JSON, scanned)

    matches = safe_load_json(MATCHES_JSON, [])
    if isinstance(matches, list):
        if resume_id:
            matches = [row for row in matches if not (isinstance(row, dict) and str(row.get("resume_id", "")).strip() == resume_id)]
            safe_write_json(MATCHES_JSON, matches)

    if MATCHES_DB.exists() and resume_id:
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


def remove_resume_from_user_workspace(user_id: Optional[str], resume_id: str, stored_filename: str, display_stem: str = "") -> None:
    if not user_id:
        return

    paths = get_user_workspace_paths(user_id)
    stored_filename = (stored_filename or "").strip()
    display_stem = (display_stem or "").strip()
    stored_stem = Path(stored_filename).stem if stored_filename else ""

    def _matches_resume_row(row: Dict[str, Any]) -> bool:
        row_resume_id = str(row.get("resume_id", "")).strip()
        row_file_name = str(row.get("file_name", "") or row.get("stored_filename", "")).strip()
        row_stem = Path(row_file_name).stem if row_file_name else ""
        row_display = str(row.get("display_name", "") or row.get("display_stem", "")).strip()
        if resume_id and row_resume_id == resume_id:
            return True
        if stored_filename and row_file_name == stored_filename:
            return True
        if stored_stem and row_stem == stored_stem:
            return True
        if display_stem and (row_stem == display_stem or row_display == display_stem):
            return True
        return False

    for key in ("scanned", "matches", "manifest"):
        path = paths.get(key)
        if not isinstance(path, Path) or not path.exists():
            continue
        data = safe_load_json(path, [])
        if not isinstance(data, list):
            continue
        cleaned = [row for row in data if not (isinstance(row, dict) and _matches_resume_row(row))]
        safe_write_json(path, cleaned)

    workspace_resume = paths["resumes"] / stored_filename
    if stored_filename and workspace_resume.exists():
        try:
            workspace_resume.unlink()
        except Exception:
            pass

    if resume_id:
        for cache_dir in (paths["match_cache"], paths["match_cache_archive"]):
            cache_file = cache_dir / f"{resume_id}.json"
            if cache_file.exists():
                try:
                    cache_file.unlink()
                except Exception:
                    pass


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

    city_hints = {
        "toronto": "Canada",
        "markham": "Canada",
        "mississauga": "Canada",
        "waterloo": "Canada",
        "ottawa": "Canada",
        "montreal": "Canada",
        "vancouver": "Canada",
        "calgary": "Canada",
        "edmonton": "Canada",
        "halifax": "Canada",
        "new york": "United States",
        "austin": "United States",
        "san jose": "United States",
        "san francisco": "United States",
        "santa clara": "United States",
        "chandler": "United States",
        "folsom": "United States",
        "bengaluru": "India",
        "bangalore": "India",
        "hyderabad": "India",
        "pune": "India",
        "mumbai": "India",
    }
    for city, country in city_hints.items():
        if city in s_low:
            return country

    parts = [part.strip() for part in re.split(r"[,/|()-]+", s) if part.strip()]
    for part in reversed(parts):
        part_low = part.lower()
        if part_low in ALIASES:
            return ALIASES[part_low]

        direct_part = _match_alias_or_country(part)
        if direct_part:
            return direct_part

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

    if "remote" in s_low:
        cleaned = re.sub(r"(?i)\bremote\b", " ", s)
        cleaned_match = _match_alias_or_country(cleaned)
        if cleaned_match:
            return cleaned_match
        return "Remote"

    return ""


def _country_to_alpha2(country_name: str) -> str:
    name = normalize_country_name(str(country_name or ""))
    if not name or name == "Remote":
        return ""
    try:
        c = pycountry.countries.get(name=name)
        if c:
            return str(c.alpha_2)
    except Exception:
        pass
    alias_to_alpha2 = {
        "United States": "US",
        "United Kingdom": "GB",
        "Korea, Republic of": "KR",
        "Taiwan": "TW",
        "Canada": "CA",
    }
    return alias_to_alpha2.get(name, "")


def _normalize_region_value(value: str, country_name: str = "") -> str:
    region = str(value or "").strip().strip(",")
    if not region:
        return ""
    region = re.sub(r"\s+", " ", region)
    if region.lower() in {"remote", "virtual", "worldwide", "global"}:
        return ""
    alpha2 = _country_to_alpha2(country_name)
    compact = region.upper()
    if alpha2 and re.fullmatch(r"[A-Z]{2,3}", compact or ""):
        try:
            sub = pycountry.subdivisions.get(code=f"{alpha2}-{compact}")
            if sub and getattr(sub, "name", None):
                return str(sub.name)
        except Exception:
            pass
    return region


def infer_region(location: str, country_hint: str = "") -> str:
    """Legacy helper name kept for compatibility; now returns inferred city."""
    s = re.sub(r"\s+", " ", str(location or "").strip())
    if not s:
        return ""
    if re.search(r"(remote|virtual|worldwide|global)", s, flags=re.IGNORECASE):
        return ""

    country = normalize_country_name(str(country_hint or infer_country(s)))

    def _strip_country_tokens(value: str) -> str:
        out = re.sub(r"\s+", " ", str(value or "").strip(" ,-"))
        if not out:
            return ""
        variants = [country] if country else []
        if country == "United States":
            variants += ["United States of America", "USA", "US"]
        elif country == "Canada":
            variants += ["CAN", "CA"]
        elif country == "United Kingdom":
            variants += ["UK", "GB", "Great Britain"]
        elif country == "Korea, Republic of":
            variants += ["South Korea", "Korea"]
        for variant in [v for v in variants if v]:
            out = re.sub(rf"(?i){re.escape(variant)}", "", out)
        out = re.sub(r"[A-Z]{2,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip(" ,-")
        return out

    candidates: List[str] = []
    hy_parts = [part.strip() for part in re.split(r"\s+-\s+", s) if part.strip()]
    if hy_parts:
        candidates.append(hy_parts[-1])
        if len(hy_parts) >= 2:
            candidates.append(hy_parts[-2])
    comma_parts = [part.strip() for part in s.split(",") if part.strip()]
    if comma_parts:
        candidates.append(comma_parts[-1])
        if len(comma_parts) >= 2:
            candidates.append(comma_parts[-2])
    candidates.append(s)

    seen: set[str] = set()
    for raw in candidates:
        candidate = _strip_country_tokens(raw)
        if not candidate:
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)

        candidate = re.sub(r"^\d+\s+", "", candidate).strip()
        candidate = re.sub(r"^[A-Z]{2,6}\s+", "", candidate).strip()
        candidate = re.sub(r"\s+", " ", candidate).strip(" ,-")

        # Remove trailing province/state names when they are appended after the city.
        if country:
            try:
                alpha2 = _country_to_alpha2(country)
                if alpha2:
                    subs = [sd for sd in list(pycountry.subdivisions) if getattr(sd, 'country_code', '') == alpha2]
                    for sub in sorted(subs, key=lambda sd: len(getattr(sd, 'name', '') or ''), reverse=True):
                        sub_name = str(getattr(sub, 'name', '') or '').strip()
                        if sub_name and candidate.lower().endswith(sub_name.lower()):
                            trimmed = candidate[:-len(sub_name)].strip(" ,-")
                            if trimmed:
                                candidate = trimmed
                                break
            except Exception:
                pass

        # Prefer a short, readable place phrase.
        if not candidate or len(candidate) > 60:
            continue
        if re.fullmatch(r"[A-Z]{2,6}(?:\s+[A-Z]{2,6})*", candidate):
            continue
        return candidate
    return ""


def collect_region_options(rows: List[Dict[str, Any]], country_filter: str = "") -> List[str]:
    opts: List[str] = []
    seen = set()
    wanted_country = normalize_country_name(str(country_filter or ""))
    for row in rows:
        row_country = normalize_country_name(str(row.get("country") or infer_country(str(row.get("location") or ""))))
        if wanted_country and row_country != wanted_country:
            continue
        region = infer_region(str(row.get("location") or ""), row_country)
        if not region:
            continue
        key = region.lower()
        if key in seen:
            continue
        seen.add(key)
        opts.append(region)
    return sorted(opts, key=lambda x: x.lower())


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
                jj = enrich_job_record(dict(j))
                out_list.append(jj)
                jid = str(j.get("job_id", ""))
                if jid:
                    out_map[jid] = jj
            _SUPABASE_JOBS_CACHE = (now_ts, out_list, out_map)
            return out_list, out_map
        except Exception as e:
            _rt_append(LAST_STDERR, f"Supabase jobs fetch failed: {repr(e)}")
            return [], {}

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
            jj = enrich_job_record(dict(j))
            out_list.append(jj)
            jid = str(j.get("job_id", ""))
            if jid:
                out_map[jid] = jj

    _JOBS_ENRICH_CACHE = (m, out_list, out_map)
    return out_list, out_map


def get_jobs_count_fast() -> int:
    global _JOBS_COUNT_CACHE
    if SUPABASE_DB is not None:
        now_ts = time.time()
        if _JOBS_COUNT_CACHE and (now_ts - _JOBS_COUNT_CACHE[0]) < SUPABASE_JOBS_CACHE_TTL_S:
            return _JOBS_COUNT_CACHE[1]
        try:
            count = SUPABASE_DB.count("jobs")
            _JOBS_COUNT_CACHE = (now_ts, count)
            return count
        except Exception as e:
            _rt_append(LAST_STDERR, f"Supabase jobs count failed: {repr(e)}")
            return 0

    if SCRAPED_JOBS_JSON.exists():
        raw = safe_load_json(SCRAPED_JOBS_JSON, [])
        return len(raw) if isinstance(raw, list) else 0
    return 0


_GENERIC_CAREERS_HOST_PATH_SNIPPETS = [
    ("nvidia.com", "/about-nvidia/careers"),
    ("intel.com", "/content/www/us/en/jobs"),
]

_LOCAL_JOB_URL_CACHE: Optional[Dict[str, str]] = None

def _looks_like_generic_careers_url(url: str) -> bool:
    raw = str(url or "").strip()
    if not raw:
        return True
    try:
        parsed = urlparse(raw)
    except Exception:
        return False
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower().rstrip("/")
    if not host:
        return False
    if "myworkdayjobs.com" in host and "/job/" in path:
        return False
    if "linkedin.com" in host and "/jobs/view/" in path:
        return False
    if "greenhouse.io" in host and "/jobs/" in path:
        return False
    if "lever.co" in host and "/jobs/" in path:
        return False
    if "ashbyhq.com" in host and "/job/" in path:
        return False
    if "smartrecruiters.com" in host and "/job/" in path:
        return False
    for host_snip, path_snip in _GENERIC_CAREERS_HOST_PATH_SNIPPETS:
        if host_snip in host and path_snip in path:
            return True
    if path.endswith('/careers') or path.endswith('/jobs') or path.endswith('/careers-home'):
        return True
    if '/careers/' in path and '/job/' not in path and '/jobs/' not in path and '/view/' not in path:
        return True
    return False


def _get_local_job_url_cache() -> Dict[str, str]:
    global _LOCAL_JOB_URL_CACHE
    if _LOCAL_JOB_URL_CACHE is not None:
        return _LOCAL_JOB_URL_CACHE
    out: Dict[str, str] = {}
    try:
        if SCRAPED_JOBS_JSON.exists():
            raw = safe_load_json(SCRAPED_JOBS_JSON, [])
            if isinstance(raw, list):
                for job in raw:
                    if not isinstance(job, dict):
                        continue
                    jid = str(job.get('job_id') or '').strip()
                    surl = str(job.get('source_url') or job.get('url') or job.get('job_url') or '').strip()
                    if jid and surl:
                        out[jid] = surl
    except Exception:
        pass
    _LOCAL_JOB_URL_CACHE = out
    return out


def _pick_best_url_from_row(row: Dict[str, Any]) -> str:
    for key in ('source_url', 'url', 'job_url'):
        value = str(row.get(key) or '').strip()
        if value and not _looks_like_generic_careers_url(value):
            return value
    for key in ('source_url', 'url', 'job_url'):
        value = str(row.get(key) or '').strip()
        if value:
            return value
    return ''


def resolve_job_posting_url(job_id: str = '', current_url: str = '', title: str = '', company: str = '', location: str = '') -> str:
    current_url = str(current_url or '').strip()
    if current_url and not _looks_like_generic_careers_url(current_url):
        return current_url

    jid = str(job_id or '').strip()
    if jid:
        local_cache = _get_local_job_url_cache()
        local_url = str(local_cache.get(jid) or '').strip()
        if local_url and not _looks_like_generic_careers_url(local_url):
            return local_url

    if SUPABASE_DB is not None and jid:
        try:
            rows = SUPABASE_DB.select(
                'jobs',
                columns='job_id,source_url,url,job_url,title,company,location',
                filters={'job_id': f'eq.{jid}'},
                limit=1,
            )
            if rows:
                picked = _pick_best_url_from_row(rows[0])
                if picked:
                    return picked
        except Exception:
            pass

    if SUPABASE_DB is not None and title:
        filters = {'title': f'eq.{title}'}
        if company:
            filters['company'] = f'eq.{company}'
        try:
            rows = SUPABASE_DB.select(
                'jobs',
                columns='job_id,source_url,url,job_url,title,company,location',
                filters=filters,
                limit=10,
            )
            norm_loc = str(location or '').strip().lower()
            for row in rows or []:
                row_loc = str(row.get('location') or '').strip().lower()
                if norm_loc and row_loc and norm_loc != row_loc:
                    continue
                picked = _pick_best_url_from_row(row)
                if picked:
                    return picked
            for row in rows or []:
                picked = _pick_best_url_from_row(row)
                if picked:
                    return picked
        except Exception:
            pass

    return current_url


@app.get('/job/open')
@login_required
def open_posting_redirect():
    job_id = (request.args.get('job_id') or '').strip()
    current_url = (request.args.get('url') or '').strip()
    title = (request.args.get('title') or '').strip()
    company = (request.args.get('company') or '').strip()
    location = (request.args.get('location') or '').strip()
    target = resolve_job_posting_url(job_id=job_id, current_url=current_url, title=title, company=company, location=location)
    if target:
        return redirect(target)
    flash('Posting URL unavailable for this result.', 'error')
    next_url = (request.args.get('next') or '').strip()
    if next_url:
        return redirect(next_url)
    return redirect(url_for('results_page'))


# ---------------------------
# Speed: grouped matches cache (per resume) once per matches.json mtime
# ---------------------------
_MATCH_GROUP_CACHE: Optional[Tuple[float, Dict[str, List[Dict[str, Any]]], List[str]]] = None

def get_matches_grouped() -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    user_id = get_current_user_id()
    matches = get_user_match_rows(user_id)
    scanned_rows = get_user_scanned_rows(user_id)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    if isinstance(matches, list):
        for row in matches:
            if not isinstance(row, dict):
                continue
            rid = str(row.get("resume_id", "")).strip()
            if not rid:
                continue
            grouped.setdefault(rid, []).append(dict(row))

    for rid, rows in grouped.items():
        rows.sort(key=lambda x: float(x.get("final_match_percent", 0.0)), reverse=True)

    resume_ids = [str(r.get("resume_id") or "").strip() for r in scanned_rows if isinstance(r, dict) and str(r.get("resume_id") or "").strip()]
    if not resume_ids:
        resume_ids = sorted(grouped.keys())
    else:
        seen: set[str] = set()
        resume_ids = [rid for rid in resume_ids if not (rid in seen or seen.add(rid))]
        for rid in sorted(grouped.keys()):
            if rid not in seen:
                resume_ids.append(rid)
                seen.add(rid)
    return grouped, resume_ids


def resume_id_to_name() -> Dict[str, str]:
    return get_user_resume_name_map(get_current_user_id())


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




def location_query_match(query: str, region_value: str, full_location: str) -> bool:
    q = str(query or "").strip().lower()
    if not q:
        return True
    region_text = str(region_value or "").strip().lower()
    location_text = str(full_location or "").strip().lower()
    return q in region_text or q in location_text

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
    "finished_elapsed_s": 0,
}

PREMIUM_RUN_LOCK = threading.RLock()
PREMIUM_RUN_THREAD: Optional[threading.Thread] = None
PREMIUM_RUN_STATE: Dict[str, Any] = {
    "running": False,
    "started_epoch": None,
    "message": "Idle",
    "error": "",
    "current_step": None,
    "current_step_started_epoch": None,
    "progress_pct": 0.0,
    "finished_elapsed_s": 0,
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

def run_script_streaming(script_path: Path, timeout_s: int = 1800, *, cwd: Optional[Path] = None, extra_env: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    if not script_path.exists():
        _rt_append(LAST_STDERR, f"Missing file: {script_path.name}")
        return {"ok": False, "returncode": None}

    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("TOKENIZERS_PARALLELISM", "false")
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("OPENBLAS_NUM_THREADS", "1")
    env.setdefault("MKL_NUM_THREADS", "1")
    env.setdefault("NUMEXPR_NUM_THREADS", "1")
    env.setdefault("ROLEMATCHER_LOW_MEMORY", "1")
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items() if v is not None})

    cmd = [sys.executable, "-u", str(script_path)]
    _rt_append(LAST_STDOUT, f"\n===== START {script_path.name} @ {datetime.now().strftime('%Y-%m-%d %I:%M %p')} =====")

    start = time.time()
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd or PROJECT_DIR),
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

def start_background_pipeline(do_job_scraper: bool, do_resume_scraper: bool, do_job_matcher: bool, *, location_mode: str, selected_countries: List[str]) -> Tuple[bool, str]:
    global RUN_THREAD
    current_user_id = get_current_user_id()
    if not current_user_id:
        return False, "Please sign in first."

    with RUN_LOCK:
        if RUN_STATE["running"]:
            return False, "Already running."

        steps: List[Path] = []
        if do_job_scraper:
            steps.append(JOB_SCRAPER)
        if do_resume_scraper:
            steps.append(RESUME_SCRAPER)
        if do_job_matcher:
            steps.append(JOB_MATCHER)

        RUN_STATE["running"] = True
        RUN_STATE["started_epoch"] = time.time()
        RUN_STATE["message"] = "Starting"
        RUN_STATE["error"] = ""
        RUN_STATE["current_step"] = "Preparing workspace"
        RUN_STATE["current_step_started_epoch"] = time.time()
        RUN_STATE["step_index"] = 0
        RUN_STATE["total_steps"] = len(steps) + 1
        RUN_STATE["finished_elapsed_s"] = 0

        _rt_reset()
        _rt_append(LAST_STDOUT, f"Starting pipeline for user {current_user_id}.")

        def runner():
            workspace_paths: Optional[Dict[str, Path]] = None
            active_rows: List[Dict[str, Any]] = []
            try:
                workspace_paths, active_rows, staged_files = stage_user_workspace(current_user_id)
                _rt_append(LAST_STDOUT, f"Prepared workspace for user {current_user_id} with {len(staged_files)} staged resume(s).")
                if not staged_files and (do_resume_scraper or do_job_matcher):
                    with RUN_LOCK:
                        RUN_STATE["running"] = False
                        RUN_STATE["message"] = "Failed"
                        RUN_STATE["error"] = "No active Supabase resumes were found for this account. Upload a PDF first."
                        RUN_STATE["current_step"] = None
                        RUN_STATE["current_step_started_epoch"] = None
                        RUN_STATE["finished_elapsed_s"] = int(max(0.0, time.time() - float(RUN_STATE.get("started_epoch") or time.time())))
                    return

                extra_env = {
                    "ROLEMATCHER_RESUMES_DIR": str(workspace_paths["resumes"]),
                    "ROLEMATCHER_SCANNED_RESUMES_JSON": str(workspace_paths["scanned"]),
                    "ROLEMATCHER_RESUMES_MANIFEST_JSON": str(workspace_paths["manifest"]),
                    "ROLEMATCHER_SCRAPED_JOBS_JSON": str(workspace_paths["jobs"]),
                    "ROLEMATCHER_MATCHES_JSON": str(workspace_paths["matches"]),
                    "ROLEMATCHER_MATCHES_DB": str(workspace_paths["matches_db"]),
                    "ROLEMATCHER_MATCH_CACHE_DIR": str(workspace_paths["match_cache"]),
                    "ROLEMATCHER_MATCH_CACHE_ARCHIVE_DIR": str(workspace_paths["match_cache_archive"]),
                    "ROLEMATCHER_JOB_EMBED_CACHE_JSON": str(PROJECT_DIR / "job_embeddings_cache.json"),
                    "ROLEMATCHER_LOCATION_MODE": str(location_mode),
                    "ROLEMATCHER_SELECTED_COUNTRIES": ",".join(selected_countries or []),
                    "ROLEMATCHER_MAX_PREFILTER_JOBS": "250",
                    "ROLEMATCHER_EXPERIENCE_PENALTY_POINTS": "30",
                    "ROLEMATCHER_DEGREE_PENALTY_POINTS": "24",
                    "ROLEMATCHER_CATEGORY_PENALTY_POINTS": "36",
                    "ROLEMATCHER_PARALLEL_SCORE_WORKERS": "3",
                    "ROLEMATCHER_PARALLEL_ROW_WORKERS": "3",
                    "ROLEMATCHER_OCR_PAGE_WORKERS": "3",
                }

                ensure_model_warmup_started()
                for i, script in enumerate(steps):
                    with RUN_LOCK:
                        RUN_STATE["message"] = "Running"
                        RUN_STATE["current_step"] = script.name
                        RUN_STATE["current_step_started_epoch"] = time.time()
                        RUN_STATE["step_index"] = i + 2

                    if script == RESUME_SCRAPER:
                        r = run_module_streaming("resume_scraper", "main", cwd=PROJECT_DIR, extra_env=extra_env)
                    elif script == JOB_MATCHER:
                        r = run_module_streaming("job_matcher", "run", cwd=PROJECT_DIR, extra_env=extra_env)
                    else:
                        r = run_script_streaming(script, timeout_s=1800, cwd=PROJECT_DIR, extra_env=extra_env)
                    if not r.get("ok"):
                        with RUN_LOCK:
                            RUN_STATE["running"] = False
                            RUN_STATE["message"] = "Failed"
                            RUN_STATE["error"] = f"{script.name} failed"
                            RUN_STATE["current_step_started_epoch"] = None
                            RUN_STATE["finished_elapsed_s"] = int(max(0.0, time.time() - float(RUN_STATE.get("started_epoch") or time.time())))
                        return

                sync_ok, sync_issues = sync_user_pipeline_outputs_to_supabase(current_user_id, workspace_paths, active_rows)
                with RUN_LOCK:
                    RUN_STATE["running"] = False
                    RUN_STATE["current_step"] = None
                    RUN_STATE["current_step_started_epoch"] = None
                    RUN_STATE["finished_elapsed_s"] = int(max(0.0, time.time() - float(RUN_STATE.get("started_epoch") or time.time())))
                    if sync_ok:
                        RUN_STATE["message"] = "Finished"
                        RUN_STATE["error"] = ""
                    else:
                        RUN_STATE["message"] = "Failed"
                        RUN_STATE["error"] = "; ".join(sync_issues[:2]) or "Supabase sync failed"
            except Exception as e:
                _rt_append(LAST_STDERR, f"Runner exception: {repr(e)}")
                with RUN_LOCK:
                    RUN_STATE["running"] = False
                    RUN_STATE["message"] = "Failed"
                    RUN_STATE["error"] = repr(e)
                    RUN_STATE["current_step"] = None
                    RUN_STATE["current_step_started_epoch"] = None
                    RUN_STATE["finished_elapsed_s"] = int(max(0.0, time.time() - float(RUN_STATE.get("started_epoch") or time.time())))
            finally:
                cleanup_workspace(workspace_paths)

        RUN_THREAD = threading.Thread(target=runner, daemon=True)
        RUN_THREAD.start()
        return True, "Started. Preparing workspace and reusing the warmed AI models now."

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
        flash("Supabase auth is not configured yet. Add SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY in your local .env file or in Render.")
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
        flash("Account created. Check your email and click the verification link, then come back here and sign in.")
        return redirect(url_for("login_page"))
    except Exception as e:
        flash(f"Sign up failed: {e}")
        return redirect(url_for("signup_page"))


@app.get("/auth/confirmed")
def auth_confirmed():
    flash("Email confirmed. You can sign in now.")
    return redirect(url_for("login_page", confirmed="1"))


@app.get("/auth/login")
def login_page():
    if request.args.get("confirmed") == "1":
        flash("Email confirmed. You can sign in now.")
    return render_template("login.html", title="Sign in", next_url=(request.args.get("next") or ""))


@app.post("/auth/login")
def login_submit():
    if SUPABASE_AUTH is None:
        flash("Supabase auth is not configured yet. Add SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY in your local .env file or in Render.")
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
    current_user_id = get_current_user_id()
    jobs_count = get_jobs_count_fast()

    logged_in = is_logged_in()
    if not logged_in:
        public_scanned = get_total_scanned_resume_count()
        public_stats = {
            "jobs_scraped": jobs_count,
            "resumes_scanned": public_scanned,
            "jobs_scraped_display": format_public_plus_count(jobs_count),
            "resumes_scanned_display": format_public_plus_count(public_scanned),
        }
        return render_template("dashboard.html", public_stats=public_stats, logged_in=False)

    scanned = get_user_scanned_rows(current_user_id)
    stats = {
        "resumes_uploaded": get_user_active_resume_count(current_user_id),
        "resumes_scanned": len(scanned) if isinstance(scanned, list) else 0,
        "jobs_scraped": jobs_count,
        "matches_rows": get_user_match_count(current_user_id),
    }
    return render_template("dashboard.html", stats=stats, logged_in=True)

@app.get("/upload")
@login_required
def upload_page():
    resumes = get_active_resume_items()
    current_user_id = get_current_user_id()
    return render_template("upload.html", resumes=resumes, max_resumes=MAX_RESUMES, current_resume_count=get_user_active_resume_count(current_user_id))

@app.post("/upload/resume")
@login_required
def upload_resume():
    if SUPABASE_DB is None:
        flash("Supabase is not configured yet.")
        return redirect(url_for("upload_page"))

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

    current_user_id = get_current_user_id()
    active_resume_count = get_user_active_resume_count(current_user_id)
    if active_resume_count >= MAX_RESUMES:
        flash(f"You can only keep up to {MAX_RESUMES} resumes for now. Delete one before uploading another.")
        return redirect(url_for("upload_page"))

    safe_stem = secure_filename(Path(f.filename).stem) or "resume"
    uploaded_at = datetime.now(timezone.utc).isoformat()
    stored_filename = make_timestamped_pdf_name(f.filename)

    try:
        raw_bytes = f.read()
        if len(raw_bytes) > MAX_RESUME_FILE_BYTES:
            flash(f"File is too large. Keep uploads under {MAX_RESUME_FILE_BYTES // (1024 * 1024)} MB.")
            return redirect(url_for("upload_page"))
        if ext in ALLOWED_IMAGE_EXTS or ext in {".tif", ".tiff"}:
            with Image.open(io.BytesIO(raw_bytes)) as img:
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")
                elif img.mode != "RGB":
                    img = img.convert("RGB")
                pdf_buf = io.BytesIO()
                img.save(pdf_buf, "PDF", resolution=150.0)
                upload_bytes = pdf_buf.getvalue()
            flash(f"Uploaded image → converted to PDF: {safe_stem}.pdf")
        else:
            upload_bytes = raw_bytes
            flash(f"Uploaded: {safe_stem}.pdf")

        if len(upload_bytes) > MAX_RESUME_FILE_BYTES:
            flash(f"Final PDF is too large. Keep uploads under {MAX_RESUME_FILE_BYTES // (1024 * 1024)} MB.")
            return redirect(url_for("upload_page"))

        storage_path = upload_resume_bytes_to_supabase(current_user_id, stored_filename, upload_bytes, content_type="application/pdf")

        row_payload = {
            "user_id": current_user_id,
            "stored_filename": stored_filename,
            "file_name": f"{safe_stem}.pdf",
            "display_stem": safe_stem,
            "storage_path": storage_path,
            "uploaded_at": uploaded_at,
            "archived": False,
            "archive_filename": None,
            "archived_at": None,
        }
        SUPABASE_DB.upsert_many("resumes", [row_payload], on_conflict="stored_filename")
    except Exception as e:
        flash(f"Upload failed: {e}")
        return redirect(url_for("upload_page"))

    return redirect(url_for("upload_page"))


@app.post("/upload/delete/<path:stored_filename>")
@login_required
def delete_resume(stored_filename: str):
    if SUPABASE_DB is None:
        flash("Supabase is not configured yet.")
        return redirect(url_for("upload_page"))

    stored_filename = Path(stored_filename).name
    current_user_id = get_current_user_id()
    if not current_user_id:
        flash("Please sign in first.")
        return redirect(url_for("login_page"))

    try:
        rows = SUPABASE_DB.select(
            "resumes",
            filters={
                "user_id": f"eq.{current_user_id}",
                "stored_filename": f"eq.{stored_filename}",
                "archived": "eq.false",
            },
            limit=1,
        )
    except Exception as e:
        flash(f"Delete failed: {e}")
        return redirect(url_for("upload_page"))

    if not rows:
        flash("Resume was not found for this account.")
        return redirect(url_for("upload_page"))

    resume_row = rows[0]
    display_stem = str(resume_row.get("display_stem") or Path(stored_filename).stem)
    uploaded_at = str(resume_row.get("uploaded_at") or datetime.now(timezone.utc).isoformat())
    archived_at = datetime.now(timezone.utc).isoformat()
    archived_storage_path = ""
    archive_name = None

    try:
        archived_storage_path = archive_resume_in_supabase(current_user_id, resume_row)
        archive_name = Path(archived_storage_path).name
    except Exception as e:
        _rt_append(LAST_STDERR, f"Supabase storage archive failed for {stored_filename}: {repr(e)}")
        archived_storage_path = get_resume_storage_path(resume_row)
        archive_name = Path(archived_storage_path).name if archived_storage_path else stored_filename

    resume_id = str(resume_row.get("id") or "").strip()

    try:
        SUPABASE_DB.update(
            "resumes",
            {
                "archived": True,
                "archive_filename": archive_name,
                "archived_at": archived_at,
                "storage_path": archived_storage_path,
                "parsed_text": None,
            },
            filters={"user_id": f"eq.{current_user_id}", "stored_filename": f"eq.{stored_filename}"},
        )

        if resume_id:
            SUPABASE_DB.delete("match_results", filters={"user_id": f"eq.{current_user_id}", "resume_id": f"eq.{resume_id}"})
    except Exception as e:
        flash(f"Delete failed: {e}")
        return redirect(url_for("upload_page"))

    flash("resume_archived in Supabase. Saved OCR and match results for that resume were removed from the database.")
    return redirect(url_for("upload_page"))




def start_background_premium_search(*, resume_id: str, country_filter: str, region_filter: str, work_mode_filter: str, posted_filter: str) -> Tuple[bool, str]:
    global PREMIUM_RUN_THREAD
    user_id = get_current_user_id()
    if not user_id:
        return False, "Please sign in first."

    state = get_user_premium_state(user_id)
    if not state.get("premium_access"):
        return False, "Enter the premium code first."
    if state.get("premium_locked") and not state.get("premium_admin_access"):
        return False, f"You have used your {PREMIUM_MAX_SEARCHES} premium search. Enter the admin code to continue."

    scan_row = get_default_premium_scan_row(user_id)
    if resume_id:
        scan_map = get_scanned_resume_row_map(user_id)
        scan_row = scan_map.get(resume_id) or scan_row
    if not scan_row:
        return False, "Upload a resume and run the free pipeline once first."
    resume_id = str(scan_row.get("resume_id") or resume_id or "").strip()

    with PREMIUM_RUN_LOCK:
        if PREMIUM_RUN_STATE.get("running"):
            return False, "Premium is already running."
        PREMIUM_RUN_STATE.update({
            "running": True,
            "started_epoch": time.time(),
            "message": "Starting",
            "error": "",
            "current_step": "Getting things ready",
            "current_step_started_epoch": time.time(),
            "progress_pct": 1.0,
            "finished_elapsed_s": 0,
        })

    def runner() -> None:
        try:
            resume_context = build_premium_resume_context(scan_row)

            def progress_cb(label: str, idx: int, total: int) -> None:
                total = max(1, int(total or 1))
                idx = max(1, int(idx or 1))
                pct = min(88.0, 10.0 + ((idx / total) * 70.0))
                with PREMIUM_RUN_LOCK:
                    PREMIUM_RUN_STATE.update({
                        "message": "Running",
                        "current_step": label,
                        "current_step_started_epoch": time.time(),
                        "progress_pct": pct,
                    })

            with PREMIUM_RUN_LOCK:
                PREMIUM_RUN_STATE.update({"message": "Running", "current_step": "Looking for live jobs", "progress_pct": 8.0})

            live_rows = search_live_jobs_with_openai(
                resume_context,
                api_key=(os.environ.get("OPENAI_API_KEY") or "").strip(),
                model=OPENAI_WEB_MODEL,
                country_filter=country_filter,
                city_filter=region_filter,
                work_mode_filter=work_mode_filter,
                posted_range=posted_filter,
                max_results=250,
                progress_cb=progress_cb,
                user_identifier=user_id,
            )

            filtered_rows: List[Dict[str, Any]] = []
            relaxed_rows: List[Dict[str, Any]] = []
            for row in live_rows:
                row_country = normalize_country_name(str(row.get("country") or infer_country(str(row.get("location") or ""))))
                row_work_mode = canonicalize_work_mode(str(row.get("work_mode") or "On-site"))
                posted_value = str(row.get("posted_date") or "")
                region_ok = True
                if region_filter:
                    region_ok = location_query_match(region_filter, infer_region(str(row.get("location") or ""), row_country), str(row.get("location") or ""))
                country_ok = True
                if country_filter:
                    country_ok = (row_country == country_filter) or (row_work_mode == "Remote") or (not row_country)
                work_ok = True
                if work_mode_filter:
                    work_ok = row_work_mode == work_mode_filter or not row.get("work_mode")
                # For live web search, do not throw away unknown dates; the search prompt already biases recency.
                posted_ok = True
                if posted_value:
                    posted_ok = date_filter_match(posted_value, posted_filter)
                row["country"] = row_country
                if country_ok and work_ok:
                    relaxed_rows.append(row)
                if country_ok and work_ok and region_ok and posted_ok:
                    filtered_rows.append(row)

            if not filtered_rows:
                filtered_rows = relaxed_rows

            if not filtered_rows:
                raise RuntimeError("No live jobs matched your current filters.")

            with PREMIUM_RUN_LOCK:
                PREMIUM_RUN_STATE.update({"message": "Running", "current_step": "Saving results", "progress_pct": 94.0})

            result_rows = build_premium_live_result_rows(resume_id, filtered_rows)
            filters_payload = {
                "country": country_filter,
                "region": region_filter,
                "work_mode": work_mode_filter,
                "posted_range": posted_filter,
                "source": "live_web_search",
                "openai_model": OPENAI_WEB_MODEL,
            }
            if not save_user_premium_results(user_id, resume_id, filters_payload, result_rows):
                raise RuntimeError("Premium results were found, but saving them failed.")
            increment_premium_search_count(user_id)
            with PREMIUM_RUN_LOCK:
                PREMIUM_RUN_STATE.update({
                    "running": False,
                    "message": "Finished",
                    "error": "",
                    "current_step": None,
                    "current_step_started_epoch": None,
                    "progress_pct": 100.0,
                    "finished_elapsed_s": int(max(0.0, time.time() - float(PREMIUM_RUN_STATE.get("started_epoch") or time.time()))),
                })
        except Exception as e:
            with PREMIUM_RUN_LOCK:
                PREMIUM_RUN_STATE.update({
                    "running": False,
                    "message": "Failed",
                    "error": str(e),
                    "current_step": None,
                    "current_step_started_epoch": None,
                    "finished_elapsed_s": int(max(0.0, time.time() - float(PREMIUM_RUN_STATE.get("started_epoch") or time.time()))),
                })

    PREMIUM_RUN_THREAD = threading.Thread(target=runner, daemon=True)
    PREMIUM_RUN_THREAD.start()
    return True, "Started. Premium is now searching live jobs on the web."


def get_premium_run_status_payload() -> Dict[str, Any]:
    with PREMIUM_RUN_LOCK:
        running = bool(PREMIUM_RUN_STATE.get("running"))
        started_epoch = PREMIUM_RUN_STATE.get("started_epoch")
        elapsed = int(time.time() - float(started_epoch)) if (running and started_epoch) else int(PREMIUM_RUN_STATE.get("finished_elapsed_s") or 0)
        message = PREMIUM_RUN_STATE.get("message") or ("Running" if running else "Idle")
        return {
            "running": running,
            "message": message,
            "error": PREMIUM_RUN_STATE.get("error") or "",
            "current_step": PREMIUM_RUN_STATE.get("current_step") or "—",
            "elapsed_s": elapsed,
            "progress_pct_smooth": round(float(PREMIUM_RUN_STATE.get("progress_pct") or (100.0 if str(message).lower().startswith("finished") else 0.0)), 1),
            "error_detail": PREMIUM_RUN_STATE.get("error") or "",
        }

@app.get("/run")
@login_required
def run_page():
    location_mode, selected_countries = get_pipeline_country_preferences()
    available_countries = [c for c in COUNTRY_LIST if c != "Remote"]
    premium_state = get_user_premium_state(get_current_user_id())
    premium_resume_ids = get_user_premium_resume_ids()
    premium_resume_name_map = get_user_premium_resume_name_map()
    premium_default_resume_id = get_default_premium_resume_id(get_current_user_id())
    active_tab = (request.args.get("tab") or "free").strip().lower()
    if active_tab not in {"free", "premium"}:
        active_tab = "free"
    return render_template(
        "run.html",
        countries=available_countries,
        location_mode=location_mode,
        selected_countries=selected_countries,
        premium_state=premium_state,
        premium_resume_ids=premium_resume_ids,
        premium_resume_name_map=premium_resume_name_map,
        premium_default_resume_id=premium_default_resume_id,
        active_run_tab=active_tab,
    )

@app.post("/run/pipeline")
@login_required
def run_pipeline():
    do_job_scraper = False
    do_resume_scraper = True
    do_job_matcher = True
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest" or "application/json" in (request.headers.get("Accept") or "")

    location_mode = (request.form.get("location_mode") or "current").strip().lower()
    if location_mode not in {"current", "all", "selected"}:
        location_mode = "current"
    selected_countries = request.form.getlist("selected_countries")
    selected_countries = [normalize_country_name(c.strip()) for c in selected_countries if c and c.strip()]
    selected_countries = [c for c in selected_countries if c]
    if location_mode != "selected":
        selected_countries = []
    elif not selected_countries:
        if wants_json:
            return jsonify({"ok": False, "message": "Choose at least one country."}), 400
        flash("Choose at least one country.")
        return redirect(url_for("run_page", tab="free"))
    save_pipeline_country_preferences(location_mode, selected_countries)

    ok, message = start_background_pipeline(
        do_job_scraper,
        do_resume_scraper,
        do_job_matcher,
        location_mode=location_mode,
        selected_countries=selected_countries,
    )
    if wants_json:
        return jsonify({"ok": ok, "message": message}), (200 if ok else 400)
    flash(message)
    return redirect(url_for("run_page", tab="free"))

@app.post("/run/premium")
@login_required
def run_premium_pipeline():
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest" or "application/json" in (request.headers.get("Accept") or "")
    resume_id = ""
    country_filter = normalize_country_name((request.form.get("country") or "").strip()) if (request.form.get("country") or "").strip() else ""
    region_filter = (request.form.get("region") or "").strip()
    work_mode_filter = canonicalize_work_mode((request.form.get("work_mode") or "").strip()) if (request.form.get("work_mode") or "").strip() else ""
    posted_filter = (request.form.get("posted_range") or "all").strip().lower()
    if posted_filter not in {"all", "week", "month"}:
        posted_filter = "all"
    ok, message = start_background_premium_search(
        resume_id=resume_id,
        country_filter=country_filter,
        region_filter=region_filter,
        work_mode_filter=work_mode_filter,
        posted_filter=posted_filter,
    )
    if wants_json:
        return jsonify({"ok": ok, "message": message}), (200 if ok else 400)
    flash(message)
    return redirect(url_for("run_page", tab="premium"))

@app.get("/results")
@login_required
def results_page():
    grouped, resume_ids = get_matches_grouped()
    rid_name = resume_id_to_name()
    location_mode, selected_countries = get_pipeline_country_preferences()
    scanned_rows = get_user_scanned_rows(get_current_user_id())

    selected = request.args.get("resume_id")
    if selected not in grouped and resume_ids:
        selected = resume_ids[0]

    allowed_countries = allowed_result_countries(location_mode, selected_countries, scanned_rows, selected)

    country_filter = (request.args.get("country") or "").strip()
    if country_filter and allowed_countries and country_filter not in allowed_countries:
        country_filter = ""
    region_filter = (request.args.get("region") or "").strip()
    work_mode_filter = canonicalize_work_mode((request.args.get("work_mode") or "").strip()) if (request.args.get("work_mode") or "").strip() else ""
    posted_filter = (request.args.get("posted_range") or "all").strip().lower()
    if posted_filter not in {"all", "week", "month"}:
        posted_filter = "all"

    try:
        page = int(request.args.get("page") or "1")
    except Exception:
        page = 1
    page = max(1, page)

    rows = grouped.get(selected, []) if selected else []
    scoped_rows: List[Dict[str, Any]] = []

    # Keep original closeness ordering from the matcher output
    for r in rows:
        url = r.get("url") or r.get("source_url") or r.get("job_url") or ""
        country = normalize_country_name(str(r.get("country") or infer_country(str(r.get("location", "")))))
        region = infer_region(str(r.get("location", "")), country)
        posted_value = (
            r.get("posted_date")
            or r.get("date_posted")
            or r.get("collected_date")
            or ""
        )

        work_mode = canonicalize_work_mode(str(r.get("work_mode") or infer_work_mode(
            str(r.get("title", "")),
            str(r.get("location", "")),
            "",
        )))

        scope_blocks = False
        if location_mode == "selected" and selected_countries:
            scope_blocks = (country not in selected_countries and work_mode != "Remote")
        elif location_mode == "current":
            chosen_country = normalize_country_name(str((next((r.get("candidate_country") for r in scanned_rows if str(r.get("resume_id") or "") == str(selected or "")), "") or "")))
            if chosen_country:
                scope_blocks = (country != chosen_country)
        if scope_blocks:
            continue

        scoped_rows.append({
            **r,
            "best_url": url,
            "country": country,
            "region": region,
            "work_mode": work_mode,
            "posted_date_display": str(posted_value) if posted_value else "Unknown",
        })

    filtered: List[Dict[str, Any]] = []
    for r in scoped_rows:
        if country_filter and country_filter != r.get("country"):
            continue
        if region_filter and not location_query_match(region_filter, str(r.get("region") or ""), str(r.get("location") or "")):
            continue
        if work_mode_filter and work_mode_filter != r.get("work_mode"):
            continue
        if not date_filter_match(str(r.get("posted_date_display") or ""), posted_filter):
            continue
        filtered.append(r)

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
        countries=allowed_countries if allowed_countries else [c for c in COUNTRY_LIST if c != "Remote"],
        work_modes=WORK_MODE_OPTIONS,
        country_filter=country_filter,
        region_filter=region_filter,
        work_mode_filter=work_mode_filter,
        posted_filter=posted_filter,
        page=page,
        page_options=page_options,
        total_results=total_results,
        range_start=(start_idx + 1 if total_results else 0),
        range_end=min(end_idx, total_results),
    )

@app.get("/premium")
@login_required
def premium_page():
    user_id = get_current_user_id()
    premium_state = get_user_premium_state(user_id)
    grouped, resume_ids = get_premium_grouped()
    resume_name_map = get_user_premium_resume_name_map()
    selected = request.args.get("resume_id") or (resume_ids[0] if resume_ids else "")
    if selected not in resume_ids and resume_ids:
        selected = resume_ids[0]

    country_filter = normalize_country_name((request.args.get("country") or "").strip()) if (request.args.get("country") or "").strip() else ""
    region_filter = (request.args.get("region") or "").strip()
    work_mode_filter = canonicalize_work_mode((request.args.get("work_mode") or "").strip()) if (request.args.get("work_mode") or "").strip() else ""
    posted_filter = (request.args.get("posted_range") or "all").strip().lower()
    if posted_filter not in {"all", "week", "month"}:
        posted_filter = "all"
    try:
        page = int(request.args.get("page") or "1")
    except Exception:
        page = 1
    page = max(1, page)

    rows = grouped.get(selected, []) if selected else []
    scoped_rows: List[Dict[str, Any]] = []
    for r in rows:
        url = r.get("url") or r.get("source_url") or r.get("job_url") or ""
        country = normalize_country_name(str(r.get("country") or infer_country(str(r.get("location", "")))))
        region = infer_region(str(r.get("location", "")), country)
        posted_value = r.get("posted_date") or r.get("date_posted") or r.get("collected_date") or ""
        work_mode = canonicalize_work_mode(str(r.get("work_mode") or infer_work_mode(str(r.get("title", "")), str(r.get("location", "")), "")))
        scoped_rows.append({**r, "best_url": url, "country": country, "region": region, "work_mode": work_mode, "posted_date_display": str(posted_value) if posted_value else "Unknown"})

    filtered: List[Dict[str, Any]] = []
    for r in scoped_rows:
        if country_filter and country_filter != r.get("country"):
            continue
        if region_filter and not location_query_match(region_filter, str(r.get("region") or ""), str(r.get("location") or "")):
            continue
        if work_mode_filter and work_mode_filter != r.get("work_mode"):
            continue
        if not date_filter_match(str(r.get("posted_date_display") or ""), posted_filter):
            continue
        filtered.append(r)

    total_results = len(filtered)
    total_pages = max(1, (total_results + 9) // 10) if total_results > 0 else 1
    page = min(page, total_pages)
    start_idx = (page - 1) * 10
    end_idx = start_idx + 10
    page_rows = filtered[start_idx:end_idx]
    page_options = [{"value": i, "label": page_label(i)} for i in range(1, total_pages + 1)]

    return render_template(
        "premium.html",
        premium_state=premium_state,
        resume_ids=resume_ids,
        selected_id=selected,
        resume_name_map=resume_name_map,
        rows=page_rows,
        countries=[c for c in COUNTRY_LIST if c != "Remote"],
        work_modes=WORK_MODE_OPTIONS,
        country_filter=country_filter,
        region_filter=region_filter,
        work_mode_filter=work_mode_filter,
        posted_filter=posted_filter,
        page=page,
        page_options=page_options,
        total_results=total_results,
        range_start=(start_idx + 1 if total_results else 0),
        range_end=min(end_idx, total_results),
        openai_model=OPENAI_MODEL,
    )


@app.post("/premium/unlock")
@login_required
def premium_unlock_submit():
    user_id = get_current_user_id()
    code = (request.form.get("premium_code") or "").strip()
    if not code:
        flash("Enter a premium code.")
        return redirect(url_for("premium_page"))
    if code == PREMIUM_ACCESS_CODE:
        ok = update_user_profile(user_id, {"premium_access": True, "premium_granted_at": datetime.now(timezone.utc).isoformat(), "premium_source": "code"})
        flash("Premium unlocked." if ok else "Premium code was correct, but saving access failed.")
    elif code == PREMIUM_ADMIN_CODE:
        ok = update_user_profile(user_id, {"premium_access": True, "premium_granted_at": datetime.now(timezone.utc).isoformat(), "premium_source": "admin_code", "premium_admin_access": True, "premium_admin_granted_at": datetime.now(timezone.utc).isoformat(), "premium_admin_source": "code"})
        flash("Admin access unlocked." if ok else "Admin code was correct, but saving access failed.")
    else:
        flash("Invalid code.")
    return redirect(url_for("premium_page"))


@app.post("/premium/admin-unlock")
@login_required
def premium_admin_unlock_submit():
    user_id = get_current_user_id()
    code = (request.form.get("admin_code") or "").strip()
    if not code:
        flash("Enter the admin code.")
        return redirect(url_for("premium_page"))
    if code != PREMIUM_ADMIN_CODE:
        flash("Invalid admin code.")
        return redirect(url_for("premium_page"))
    ok = update_user_profile(user_id, {"premium_access": True, "premium_granted_at": datetime.now(timezone.utc).isoformat(), "premium_source": "admin_code", "premium_admin_access": True, "premium_admin_granted_at": datetime.now(timezone.utc).isoformat(), "premium_admin_source": "code"})
    flash("Admin access unlocked." if ok else "Admin code was correct, but saving access failed.")
    return redirect(url_for("premium_page"))


@app.post("/premium/run")
@login_required
def premium_run_submit():
    resume_id = ""
    country_filter = normalize_country_name((request.form.get("country") or "").strip()) if (request.form.get("country") or "").strip() else ""
    region_filter = (request.form.get("region") or "").strip()
    work_mode_filter = canonicalize_work_mode((request.form.get("work_mode") or "").strip()) if (request.form.get("work_mode") or "").strip() else ""
    posted_filter = (request.form.get("posted_range") or "all").strip().lower()
    if posted_filter not in {"all", "week", "month"}:
        posted_filter = "all"
    ok, message = start_background_premium_search(
        resume_id=resume_id,
        country_filter=country_filter,
        region_filter=region_filter,
        work_mode_filter=work_mode_filter,
        posted_filter=posted_filter,
    )
    flash(message)
    return redirect(url_for("run_page", tab="premium"))


@app.get("/jobs")
def jobs_page():
    return redirect(url_for("premium_page"))


@app.get("/jobs-legacy-debug")
def jobs_legacy_debug_page():
    jobs_list, _ = get_jobs_enriched()

    q = (request.args.get("q") or "").strip().lower()
    country_filter = (request.args.get("country") or "").strip()
    work_mode_filter = canonicalize_work_mode((request.args.get("work_mode") or "").strip()) if (request.args.get("work_mode") or "").strip() else ""

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
        region_filter=region_filter,
        regions=available_regions,
        work_mode_filter=work_mode_filter,
    )


@app.get("/api/premium-status")
@login_required
def api_premium_status():
    return jsonify(get_premium_run_status_payload())

@app.get("/api/supabase-status")
def api_supabase_status():
    status = {
        "has_url": bool(os.environ.get("SUPABASE_URL")),
        "has_secret_key": bool(os.environ.get("SUPABASE_SECRET_KEY")),
        "has_publishable_key": bool(os.environ.get("SUPABASE_PUBLISHABLE_KEY")),
        "auth_configured": SUPABASE_AUTH is not None,
        "resume_storage_bucket": SUPABASE_RESUMES_BUCKET,
        "jobs_table_available": False,
        "jobs_count": 0,
        "resumes_table_available": False,
        "match_results_table_available": False,
        "premium_match_results_table_available": False,
        "profiles_premium_columns_available": False,
        "scanned_resume_count": 0,
    }
    if SUPABASE_DB is None:
        return jsonify(status)
    try:
        status["jobs_count"] = get_jobs_count_fast()
        status["jobs_table_available"] = True
    except Exception as e:
        status["jobs_error"] = repr(e)
    try:
        status["scanned_resume_count"] = get_total_scanned_resume_count()
        SUPABASE_DB.select("resumes", columns="id", limit=1)
        status["resumes_table_available"] = True
    except Exception as e:
        status["resumes_error"] = repr(e)
    try:
        SUPABASE_DB.select("match_results", columns="id", limit=1)
        status["match_results_table_available"] = True
    except Exception as e:
        status["match_results_error"] = repr(e)
    try:
        SUPABASE_DB.select("premium_match_results", columns="id", limit=1)
        status["premium_match_results_table_available"] = True
    except Exception as e:
        status["premium_match_results_error"] = repr(e)
    try:
        SUPABASE_DB.select("profiles", columns="premium_access,premium_admin_access,premium_searches_used", limit=1)
        status["profiles_premium_columns_available"] = True
    except Exception as e:
        status["profiles_premium_columns_error"] = repr(e)
    return jsonify(status)

# ---------------------------
# API: status only (no /logs, no /download, no /api/tail)
# ---------------------------
@app.get("/api/status")
def api_status():
    with RUN_LOCK:
        running = bool(RUN_STATE.get("running"))
        started_epoch = RUN_STATE.get("started_epoch")
        elapsed = int(time.time() - started_epoch) if (running and started_epoch) else int(RUN_STATE.get("finished_elapsed_s") or 0)
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
