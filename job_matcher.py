#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import concurrent.futures
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from sentence_transformers import SentenceTransformer, util
    HAVE_SENTENCE_TRANSFORMERS = True
except Exception:
    SentenceTransformer = None
    util = None
    HAVE_SENTENCE_TRANSFORMERS = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESUMES_PATH = os.path.join(BASE_DIR, "scanned_resumes.json")
JOBS_PATH = os.path.join(BASE_DIR, "scraped_jobs.json")
OUTPUT_PATH = os.path.join(BASE_DIR, "resume_job_matches.json")
OUTPUT_DB_PATH = os.path.join(BASE_DIR, "resume_job_matches.db")
MATCH_CACHE_DIR = os.path.join(BASE_DIR, "match_cache")
MATCH_CACHE_ARCHIVE_DIR = os.path.join(BASE_DIR, "match_cache_archive")
JOB_EMBED_CACHE_JSON = os.path.join(BASE_DIR, "job_embeddings_cache.json")

MODEL_NAME = "all-MiniLM-L6-v2"
TOP_N_PRINT = 5
EXPERIENCE_PENALTY_POINTS = 40.0
DEBUG_EXPERIENCE = True

# Fast-match tuning
LEXICAL_PREFILTER_MIN_PENDING = 220
LEXICAL_PREFILTER_TOP_K = 180
SCORE_CHUNK_MIN_ROWS = 256
MAX_SCORE_WORKERS = min(8, max(2, (os.cpu_count() or 4)))
JOB_EMBED_BATCH_SIZE = 128
RESUME_EMBED_BATCH_SIZE = 32

JOB_YEARS_PATTERNS = [
    r"(?P<num>\d+(?:\.\d+)?)\s*(?:\+|\bplus\b)?\s*(?:years|year|yrs|yr)\s*(?:of\s*)?experience",
    r"(?:minimum\s+of|at\s+least|min\.)\s*(?P<num>\d+(?:\.\d+)?)\s*(?:years|year|yrs|yr)",
    r"(?P<num>\d+(?:\.\d+)?)\s*(?:\+|\bplus\b)\s*(?:years|year|yrs|yr)",
]

MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

SEASONS = {"spring": 3, "summer": 6, "fall": 9, "autumn": 9, "winter": 12}

WORK_KEYWORDS = [
    "intern", "engineer", "developer", "co-op", "coop", "contract", "full-time", "part-time",
    "company", "corporation", "inc", "ltd", "assistant", "technician", "designer", "verification",
    "rtl", "firmware", "hydro", "project", "role", "position"
]


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def json_dump(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


def ensure_list(x: Any) -> List[Dict[str, Any]]:
    if x is None:
        return []
    if isinstance(x, list):
        return [i for i in x if isinstance(i, dict)]
    if isinstance(x, dict):
        return [x]
    return []


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def log_stage(name: str, start_ts: float) -> None:
    print(f"[{name}] {time.perf_counter() - start_ts:.2f}s")


def extract_required_years(job_text: str) -> Optional[float]:
    if not job_text:
        return None
    years = []
    for pat in JOB_YEARS_PATTERNS:
        for m in re.finditer(pat, job_text, flags=re.IGNORECASE):
            try:
                years.append(float(m.group("num")))
            except Exception:
                pass
    return max(years) if years else None


def _parse_month_year(s: str) -> Optional[date]:
    if not s:
        return None
    t = s.strip().lower()
    t = re.sub(r"[,\.\)]$", "", t)

    if t in {"present", "current", "now", "today"}:
        today = date.today()
        return date(today.year, today.month, 1)

    m = re.match(r"^(?P<m>\d{1,2})\s*[-/]\s*(?P<y>\d{4})$", t)
    if m:
        mm = max(1, min(12, int(m.group("m"))))
        y = int(m.group("y"))
        return date(y, mm, 1)

    m = re.match(r"^(?P<y>\d{4})\s*[-/]\s*(?P<m>\d{1,2})$", t)
    if m:
        y = int(m.group("y"))
        mm = max(1, min(12, int(m.group("m"))))
        return date(y, mm, 1)

    m = re.match(r"^(?P<mon>[a-z]{3,9})\.?\s+(?P<y>\d{4})$", t)
    if m:
        mon = m.group("mon")
        y = int(m.group("y"))
        mm = MONTHS.get(mon[:3], None)
        if mm:
            return date(y, mm, 1)

    m = re.match(r"^(?P<y>\d{4})\s+(?P<mon>[a-z]{3,9})\.?$", t)
    if m:
        y = int(m.group("y"))
        mon = m.group("mon")
        mm = MONTHS.get(mon[:3], None)
        if mm:
            return date(y, mm, 1)

    m = re.match(r"^(?P<season>spring|summer|fall|autumn|winter)\s+(?P<y>\d{4})$", t)
    if m:
        y = int(m.group("y"))
        mm = SEASONS.get(m.group("season"), None)
        if mm:
            return date(y, mm, 1)

    m = re.match(r"^(?P<y>\d{4})$", t)
    if m:
        return date(int(m.group("y")), 1, 1)

    return None


def _months_between(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + (b.month - a.month)


def _extract_years_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:\+|\bplus\b)?\s*(?:years|year|yrs|yr)\b", text, flags=re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def _extract_date_ranges_from_text(text: str) -> List[Tuple[date, date, int, int]]:
    if not text:
        return []
    t = text.replace("–", "-").replace("—", "-")
    token = r"(?:[A-Za-z]{3,9}\.?\s+\d{4}|\d{1,2}[-/]\d{4}|\d{4}[-/]\d{1,2}|\d{4}\s+[A-Za-z]{3,9}\.?\s*|\d{4}|present|current|spring\s+\d{4}|summer\s+\d{4}|fall\s+\d{4}|autumn\s+\d{4}|winter\s+\d{4})"
    pattern = re.compile(rf"(?P<start>{token})\s*(?:-|to)\s*(?P<end>{token})", flags=re.IGNORECASE)

    ranges = []
    for m in pattern.finditer(t):
        s = _parse_month_year(m.group("start"))
        e = _parse_month_year(m.group("end"))
        if s and e:
            if e < s:
                s, e = e, s
            ranges.append((s, e, m.start(), m.end()))
    return ranges


def _merge_intervals(intervals: List[Tuple[date, date]]) -> List[Tuple[date, date]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: (x[0].year, x[0].month, x[1].year, x[1].month))
    merged = [intervals[0]]
    for s, e in intervals[1:]:
        last_s, last_e = merged[-1]
        if _months_between(last_e, s) <= 1:
            merged[-1] = (last_s, max(last_e, e))
        else:
            merged.append((s, e))
    return merged


def derive_candidate_years(experience_field: Any) -> Optional[float]:
    if experience_field is None:
        return None
    if isinstance(experience_field, (int, float)):
        return float(experience_field)
    if isinstance(experience_field, str):
        y = _extract_years_from_text(experience_field)
        if y is not None:
            return y
        ranges = _extract_date_ranges_from_text(experience_field)
        if ranges:
            total_months = sum(max(0, _months_between(s, e)) for s, e, _, _ in ranges)
            return round(total_months / 12.0, 2)
        return None
    if isinstance(experience_field, list):
        total_months = 0
        found_any = False
        for item in experience_field:
            if isinstance(item, (int, float)):
                found_any = True
                total_months += int(round(float(item) * 12))
                continue
            if isinstance(item, str):
                y = _extract_years_from_text(item)
                if y is not None:
                    found_any = True
                    total_months += int(round(y * 12))
                    continue
                ranges = _extract_date_ranges_from_text(item)
                if ranges:
                    found_any = True
                    total_months += sum(max(0, _months_between(s, e)) for s, e, _, _ in ranges)
                continue
            if isinstance(item, dict):
                for k in ["years", "years_experience", "experience_years", "duration_years"]:
                    if k in item and isinstance(item[k], (int, float, str)):
                        if isinstance(item[k], (int, float)):
                            found_any = True
                            total_months += int(round(float(item[k]) * 12))
                            break
                        if isinstance(item[k], str):
                            y = _extract_years_from_text(item[k])
                            if y is not None:
                                found_any = True
                                total_months += int(round(y * 12))
                                break
                candidates = [
                    (item.get("start_date"), item.get("end_date")),
                    (item.get("from"), item.get("to")),
                    (item.get("start"), item.get("end")),
                ]
                if item.get("dates"):
                    candidates.append((item.get("dates"), None))
                for s_raw, e_raw in candidates:
                    if isinstance(s_raw, str) and e_raw is None:
                        ranges = _extract_date_ranges_from_text(s_raw)
                        if ranges:
                            found_any = True
                            total_months += sum(max(0, _months_between(s, e)) for s, e, _, _ in ranges)
                            break
                    else:
                        s = _parse_month_year(str(s_raw)) if s_raw else None
                        e = _parse_month_year(str(e_raw)) if e_raw else None
                        if s and e:
                            found_any = True
                            total_months += max(0, _months_between(s, e))
                            break
        if found_any:
            return round(total_months / 12.0, 2)
    return None


def extract_section(text: str, start_keys: List[str], end_keys: List[str]) -> str:
    if not text:
        return ""
    start_pos = None
    for k in start_keys:
        m = re.search(rf"{re.escape(k)}", text, flags=re.IGNORECASE)
        if m:
            start_pos = m.start()
            break
    if start_pos is None:
        return ""
    tail = text[start_pos:]
    end_pos = None
    for k in end_keys:
        m = re.search(rf"{re.escape(k)}", tail, flags=re.IGNORECASE)
        if m:
            end_pos = m.start()
            break
    return tail[:end_pos] if end_pos is not None else tail


def _ranges_near_work_keywords(text: str, ranges: List[Tuple[date, date, int, int]]) -> List[Tuple[date, date]]:
    kept: List[Tuple[date, date]] = []
    low = text.lower()
    for s, e, a, b in ranges:
        window_start = max(0, a - 100)
        window_end = min(len(low), b + 100)
        window = low[window_start:window_end]
        if any(k in window for k in WORK_KEYWORDS):
            kept.append((s, e))
    return kept


def get_resume_experience_years(resume: Dict[str, Any]) -> Optional[float]:
    possible_keys = [
        "experience", "experiences", "work_experience", "workExperience",
        "employment_history", "employment", "experience_text", "work_experience_text", "work_history"
    ]
    for k in possible_keys:
        if k in resume and resume.get(k) not in (None, "", [], {}):
            y = derive_candidate_years(resume.get(k))
            if y is not None:
                return y

    resume_text = (resume.get("resume_text") or "").strip()
    if not resume_text:
        return None

    exp_section = extract_section(
        resume_text,
        start_keys=["EXPERIENCE", "WORK EXPERIENCE", "EMPLOYMENT", "PROFESSIONAL EXPERIENCE", "EXPERIENCE:"],
        end_keys=["EDUCATION", "PROJECTS", "SKILLS", "CERTIFICATIONS", "PUBLICATIONS"]
    )
    if exp_section:
        ranges = _extract_date_ranges_from_text(exp_section)
        if ranges:
            intervals = _merge_intervals([(s, e) for s, e, _, _ in ranges])
            total_months = sum(max(0, _months_between(s, e)) for s, e in intervals)
            return round(total_months / 12.0, 2)
        y = _extract_years_from_text(exp_section)
        if y is not None:
            return y

    ranges_all = _extract_date_ranges_from_text(resume_text)
    if not ranges_all:
        return None
    kept = _ranges_near_work_keywords(resume_text, ranges_all)
    if not kept:
        return None
    kept = _merge_intervals(kept)
    total_months = sum(max(0, _months_between(s, e)) for s, e in kept)
    return round(total_months / 12.0, 2)


def print_top_jobs_for_resume(resume_id: str, rows: List[Dict[str, Any]], top_n: int) -> None:
    print("\n" + "=" * 90)
    print(f"Top {top_n} jobs for resume: {resume_id}")
    print("=" * 90)
    for rank, r in enumerate(rows[:top_n], start=1):
        extra = []
        if r.get("penalty_applied"):
            extra.append(f"penalty: -{EXPERIENCE_PENALTY_POINTS:g}")
        req = r.get("required_experience_years")
        have = r.get("resume_experience_years")
        if req is not None and have is not None:
            extra.append(f"exp: {have}y vs req {req}y")
        extra_str = (" | " + ", ".join(extra)) if extra else ""
        print(f"{rank:>2}. {r['final_match_percent']:6.2f}% (raw {r['raw_match_percent']:6.2f}%){extra_str}")
        print(f"    {r.get('title','')} — {r.get('company','')} — {r.get('location','')}")
        if r.get("url"):
            print(f"    {r['url']}")


def write_results_sqlite(rows: List[Dict[str, Any]]) -> None:
    conn = sqlite3.connect(OUTPUT_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode = WAL")
        cur.execute("DROP TABLE IF EXISTS matches")
        cur.execute(
            """
            CREATE TABLE matches (
                resume_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                rank INTEGER,
                title TEXT,
                company TEXT,
                location TEXT,
                raw_match_percent REAL,
                final_match_percent REAL,
                required_experience_years REAL,
                resume_experience_years REAL,
                penalty_applied INTEGER,
                url TEXT
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO matches (
                resume_id, job_id, rank, title, company, location,
                raw_match_percent, final_match_percent,
                required_experience_years, resume_experience_years,
                penalty_applied, url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [(
                str(r.get("resume_id", "")),
                str(r.get("job_id", "")),
                int(r.get("rank", 0) or 0),
                str(r.get("title", "")),
                str(r.get("company", "")),
                str(r.get("location", "")),
                float(r.get("raw_match_percent", 0.0) or 0.0),
                float(r.get("final_match_percent", 0.0) or 0.0),
                None if r.get("required_experience_years") is None else float(r.get("required_experience_years")),
                None if r.get("resume_experience_years") is None else float(r.get("resume_experience_years")),
                1 if bool(r.get("penalty_applied")) else 0,
                str(r.get("url", "")),
            ) for r in rows],
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_matches_resume_rank ON matches (resume_id, rank)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_matches_resume_score ON matches (resume_id, final_match_percent DESC)")
        conn.commit()
    finally:
        conn.close()


def job_identity(job: Dict[str, Any], fallback_idx: int) -> str:
    for key in ("job_id", "url", "job_url", "source_url"):
        value = str(job.get(key, "") or "").strip()
        if value:
            return value
    seed = "|".join([
        str(job.get("title", "")),
        str(job.get("company", "")),
        str(job.get("location", "")),
        str(job.get("description_text", ""))[:200],
        str(fallback_idx),
    ])
    return "JOBHASH-" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def resume_cache_path(resume_id: str) -> str:
    return os.path.join(MATCH_CACHE_DIR, f"{resume_id}.json")


def load_resume_cache(resume_id: str) -> Dict[str, Any]:
    path = resume_cache_path(resume_id)
    if not os.path.exists(path):
        return {"resume_id": resume_id, "jobs": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("jobs"), dict):
            return data
    except Exception:
        pass
    return {"resume_id": resume_id, "jobs": {}}


def save_resume_cache(resume_id: str, jobs_map: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(MATCH_CACHE_DIR, exist_ok=True)
    json_dump(resume_cache_path(resume_id), {"resume_id": resume_id, "jobs": jobs_map})


def load_job_embedding_cache() -> Dict[str, Any]:
    try:
        with open(JOB_EMBED_CACHE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("jobs"), dict):
            return data
    except Exception:
        pass
    return {"model_name": MODEL_NAME, "jobs": {}}


def save_job_embedding_cache(cache: Dict[str, Any]) -> None:
    json_dump(JOB_EMBED_CACHE_JSON, cache)


def encode_jobs_for_model(model: SentenceTransformer, texts: List[str]) -> np.ndarray:
    emb = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True, batch_size=JOB_EMBED_BATCH_SIZE, show_progress_bar=False)
    return np.asarray(emb, dtype=np.float32)


def encode_resumes_for_model(model: SentenceTransformer, texts: List[str]) -> np.ndarray:
    emb = model.encode(texts, convert_to_numpy=True, normalize_embeddings=True, batch_size=RESUME_EMBED_BATCH_SIZE, show_progress_bar=False)
    return np.asarray(emb, dtype=np.float32)


def get_job_embeddings(model: SentenceTransformer, job_payloads: List[Tuple[int, Dict[str, Any], str, str]]) -> Dict[str, np.ndarray]:
    cache = load_job_embedding_cache()
    jobs_map = cache.get("jobs", {}) if isinstance(cache.get("jobs"), dict) else {}
    embedding_map: Dict[str, np.ndarray] = {}
    missing_ids: List[str] = []
    missing_texts: List[str] = []

    for _, _, jid, txt in job_payloads:
        row = jobs_map.get(jid)
        vec = None
        if isinstance(row, dict) and row.get("model_name") == MODEL_NAME:
            emb = row.get("embedding")
            if isinstance(emb, list) and emb:
                try:
                    vec = np.asarray(emb, dtype=np.float32)
                except Exception:
                    vec = None
        if vec is not None:
            embedding_map[jid] = vec
        else:
            missing_ids.append(jid)
            missing_texts.append(txt)

    if missing_texts:
        print(f"Encoding {len(missing_texts)} new job embeddings...")
        new_embs = encode_jobs_for_model(model, missing_texts)
        for jid, vec in zip(missing_ids, new_embs):
            vec = np.asarray(vec, dtype=np.float32)
            embedding_map[jid] = vec
            jobs_map[jid] = {"model_name": MODEL_NAME, "embedding": vec.astype(float).tolist()}
        cache["model_name"] = MODEL_NAME
        cache["jobs"] = jobs_map
        save_job_embedding_cache(cache)

    return embedding_map


def parallel_vector_similarity(resume_vec: np.ndarray, job_vecs: np.ndarray) -> List[float]:
    if job_vecs.size == 0:
        return []
    rows = job_vecs.shape[0]
    if rows < SCORE_CHUNK_MIN_ROWS:
        return np.matmul(job_vecs, resume_vec).astype(np.float32).astype(float).tolist()

    workers = min(MAX_SCORE_WORKERS, rows)
    chunk_size = math.ceil(rows / workers)
    chunks = [(i, job_vecs[i:i + chunk_size]) for i in range(0, rows, chunk_size)]

    def _score_chunk(args: Tuple[int, np.ndarray]) -> Tuple[int, np.ndarray]:
        idx, chunk = args
        return idx, np.matmul(chunk, resume_vec).astype(np.float32)

    parts: List[Tuple[int, np.ndarray]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for item in ex.map(_score_chunk, chunks):
            parts.append(item)
    parts.sort(key=lambda x: x[0])
    merged = np.concatenate([arr for _, arr in parts])
    return merged.astype(float).tolist()


def build_row(resume_id: str, resume_exp_years: Optional[float], job: Dict[str, Any], job_idx: int, raw_sim: float) -> Dict[str, Any]:
    job_id = job.get("job_id") or f"JOB-{job_idx:05d}"
    raw_percent = clamp(raw_sim * 100.0, 0.0, 100.0)
    required_years = extract_required_years(job.get("description_text", ""))
    final_percent = raw_percent
    applied_penalty = False
    if required_years is not None and resume_exp_years is not None and resume_exp_years < required_years:
        final_percent = max(0.0, raw_percent - EXPERIENCE_PENALTY_POINTS)
        applied_penalty = True
    return {
        "resume_id": resume_id,
        "job_id": job_id,
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "raw_match_percent": round(raw_percent, 2),
        "final_match_percent": round(final_percent, 2),
        "required_experience_years": required_years,
        "resume_experience_years": resume_exp_years,
        "penalty_applied": applied_penalty,
        "url": job.get("url", job.get("job_url", "")),
    }


def prebuild_job_tfidf(job_payloads: List[Tuple[int, Dict[str, Any], str, str]]) -> Tuple[TfidfVectorizer, Any]:
    texts = [txt for _, _, _, txt in job_payloads]
    vectorizer = TfidfVectorizer(stop_words="english", max_features=25000, ngram_range=(1, 2), min_df=1)
    matrix = vectorizer.fit_transform(texts)
    return vectorizer, matrix


def lexical_scores_for_resume(vectorizer: TfidfVectorizer, job_matrix: Any, resume_text: str) -> np.ndarray:
    resume_vec = vectorizer.transform([resume_text])
    sims = cosine_similarity(resume_vec, job_matrix)[0]
    return np.asarray(sims, dtype=np.float32)


def pick_candidate_job_ids(pending: List[Tuple[int, Dict[str, Any], str, str]], lexical_score_map: Dict[str, float], top_k: int) -> List[str]:
    ordered = sorted(pending, key=lambda item: lexical_score_map.get(item[2], 0.0), reverse=True)
    return [jid for _, _, jid, _ in ordered[:top_k]]


def run() -> None:
    overall_start = time.perf_counter()
    if not os.path.exists(RESUMES_PATH):
        raise SystemExit(f"Missing file: {RESUMES_PATH}")
    if not os.path.exists(JOBS_PATH):
        raise SystemExit(f"Missing file: {JOBS_PATH}")

    load_start = time.perf_counter()
    resumes_raw = ensure_list(load_json(RESUMES_PATH))
    jobs_raw = ensure_list(load_json(JOBS_PATH))
    log_stage("load_inputs", load_start)

    if not resumes_raw:
        raise SystemExit("No resumes found in scanned_resumes.json")
    if not jobs_raw:
        raise SystemExit("No jobs found in scraped_jobs.json")

    os.makedirs(MATCH_CACHE_DIR, exist_ok=True)
    os.makedirs(MATCH_CACHE_ARCHIVE_DIR, exist_ok=True)

    prep_start = time.perf_counter()
    job_payloads: List[Tuple[int, Dict[str, Any], str, str]] = []
    job_idx_map: Dict[str, int] = {}
    for idx, job in enumerate(jobs_raw):
        txt = (job.get("description_text") or "").strip()
        if not txt:
            continue
        jid = job_identity(job, idx)
        job_payloads.append((idx, job, jid, txt))
        job_idx_map[jid] = len(job_payloads) - 1
    if not job_payloads:
        raise SystemExit("All jobs have empty description_text")
    log_stage("prepare_jobs", prep_start)

    model = None
    if HAVE_SENTENCE_TRANSFORMERS:
        model_start = time.perf_counter()
        print(f"Using SentenceTransformer model: {MODEL_NAME}")
        model = SentenceTransformer(MODEL_NAME)
        log_stage("load_model", model_start)
    else:
        print("sentence_transformers not found. Falling back to TF-IDF cosine similarity.")

    lexical_start = time.perf_counter()
    vectorizer, job_tfidf = prebuild_job_tfidf(job_payloads)
    log_stage("build_job_tfidf", lexical_start)

    all_results: List[Dict[str, Any]] = []
    resume_pending_rows: List[Dict[str, Any]] = []
    resume_pending_meta: Dict[str, Dict[str, Any]] = {}
    candidate_union: Dict[str, Tuple[int, Dict[str, Any], str, str]] = {}

    cache_scan_start = time.perf_counter()
    for i, resume in enumerate(resumes_raw):
        resume_id = str(resume.get("resume_id") or f"RES-{i:04d}")
        resume["resume_id"] = resume_id
        resume_text = (resume.get("resume_text") or "").strip()
        resume_exp_years = get_resume_experience_years(resume)
        if DEBUG_EXPERIENCE:
            print(f"\n[DEBUG] resume_id={resume_id} extracted_resume_experience_years={resume_exp_years}")

        cache = load_resume_cache(resume_id)
        cached_jobs = cache.get("jobs", {}) if isinstance(cache.get("jobs"), dict) else {}
        pending = [(idx, job, jid, txt) for idx, job, jid, txt in job_payloads if jid not in cached_jobs]
        reused = 0
        for _, _, jid, _ in job_payloads:
            row = cached_jobs.get(jid)
            if isinstance(row, dict):
                all_results.append(dict(row))
                reused += 1

        lexical_scores = None
        lexical_score_map: Dict[str, float] = {}
        candidate_ids: set[str] = set()
        if pending and resume_text:
            lexical_scores = lexical_scores_for_resume(vectorizer, job_tfidf, resume_text)
            lexical_score_map = {jid: float(lexical_scores[job_idx_map[jid]]) for _, _, jid, _ in pending}
            if model is not None and len(pending) >= LEXICAL_PREFILTER_MIN_PENDING:
                candidate_ids = set(pick_candidate_job_ids(pending, lexical_score_map, LEXICAL_PREFILTER_TOP_K))
            elif model is not None:
                candidate_ids = {jid for _, _, jid, _ in pending}

            resume_pending_rows.append(resume)
            resume_pending_meta[resume_id] = {
                "resume_exp_years": resume_exp_years,
                "cached_jobs": cached_jobs,
                "pending": pending,
                "lexical_score_map": lexical_score_map,
                "candidate_ids": candidate_ids,
            }
            for item in pending:
                if item[2] in candidate_ids:
                    candidate_union[item[2]] = item

        print(f"Resume {resume_id}: reused {reused} cached jobs, pending {len(pending)} new jobs, semantic_candidates {len(candidate_ids)}")
    log_stage("load_resume_caches", cache_scan_start)

    semantic_score_maps: Dict[str, Dict[str, float]] = {rid: {} for rid in resume_pending_meta}
    if model is not None and candidate_union:
        job_cache_start = time.perf_counter()
        union_payloads = list(candidate_union.values())
        job_embedding_map = get_job_embeddings(model, union_payloads)
        log_stage("prepare_job_embeddings", job_cache_start)

        resume_vec_start = time.perf_counter()
        pending_resumes = [resume for resume in resume_pending_rows if resume_pending_meta[str(resume['resume_id'])]['candidate_ids']]
        resume_vecs = encode_resumes_for_model(model, [(r.get('resume_text') or '').strip() for r in pending_resumes])
        log_stage("encode_pending_resumes", resume_vec_start)

        match_start = time.perf_counter()
        for resume, resume_vec in zip(pending_resumes, resume_vecs):
            resume_id = str(resume['resume_id'])
            candidate_ids = list(resume_pending_meta[resume_id]['candidate_ids'])
            pending_vecs = np.asarray([job_embedding_map[jid] for jid in candidate_ids], dtype=np.float32)
            scores = parallel_vector_similarity(np.asarray(resume_vec, dtype=np.float32), pending_vecs)
            semantic_score_maps[resume_id] = {jid: float(score) for jid, score in zip(candidate_ids, scores)}
        log_stage("semantic_candidate_scoring", match_start)

    build_rows_start = time.perf_counter()
    for resume in resume_pending_rows:
        resume_id = str(resume['resume_id'])
        meta = resume_pending_meta[resume_id]
        resume_exp_years = meta['resume_exp_years']
        cached_jobs = meta['cached_jobs']
        pending = meta['pending']
        lexical_score_map = meta['lexical_score_map']
        semantic_score_map = semantic_score_maps.get(resume_id, {})

        for job_idx, job, jid, _ in pending:
            raw_sim = semantic_score_map.get(jid)
            if raw_sim is None:
                raw_sim = lexical_score_map.get(jid, 0.0)
            row = build_row(resume_id, resume_exp_years, job, job_idx, float(raw_sim))
            cached_jobs[jid] = row
            all_results.append(dict(row))
        save_resume_cache(resume_id, cached_jobs)
    log_stage("build_and_cache_rows", build_rows_start)

    group_start = time.perf_counter()
    by_resume: Dict[str, List[Dict[str, Any]]] = {}
    for r in all_results:
        by_resume.setdefault(r['resume_id'], []).append(r)

    saved_results: List[Dict[str, Any]] = []
    for rid, rows in by_resume.items():
        rows.sort(key=lambda x: (-x['final_match_percent'], -x['raw_match_percent']))
        for rank, row in enumerate(rows, start=1):
            row['rank'] = rank
        print_top_jobs_for_resume(rid, rows, TOP_N_PRINT)
        saved_results.extend(rows)

    json_dump(OUTPUT_PATH, saved_results)
    write_results_sqlite(saved_results)
    log_stage("save_outputs", group_start)
    print(f"\nSaved {len(saved_results)} rows total to: {OUTPUT_PATH} (all ranked jobs per resume)")
    print(f"Saved SQLite database to: {OUTPUT_DB_PATH}")
    log_stage("total", overall_start)


if __name__ == '__main__':
    run()
