#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import math
import os
import sqlite3
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from matcher_taxonomy import (
    category_similarity,
    degree_fit_score,
    enrich_job_record,
    extract_required_years,
    infer_resume_profile,
    shortlist_jobs_for_resume,
    canonicalize_work_mode,
)

from shared_model_registry import get_sentence_transformer

LOW_MEMORY_MODE = os.environ.get("ROLEMATCHER_LOW_MEMORY", "0").strip().lower() in {"1", "true", "yes", "on"}
DISABLE_SENTENCE_TRANSFORMERS = os.environ.get("ROLEMATCHER_DISABLE_ST", "0").strip().lower() in {"1", "true", "yes", "on"}

try:
    from sentence_transformers import SentenceTransformer
    HAVE_SENTENCE_TRANSFORMERS = not DISABLE_SENTENCE_TRANSFORMERS and not LOW_MEMORY_MODE
except Exception:
    SentenceTransformer = None  # type: ignore
    HAVE_SENTENCE_TRANSFORMERS = False

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESUMES_PATH = os.environ.get("ROLEMATCHER_SCANNED_RESUMES_JSON") or os.path.join(BASE_DIR, "scanned_resumes.json")
JOBS_PATH = os.environ.get("ROLEMATCHER_SCRAPED_JOBS_JSON") or os.path.join(BASE_DIR, "scraped_jobs.json")
OUTPUT_PATH = os.environ.get("ROLEMATCHER_MATCHES_JSON") or os.path.join(BASE_DIR, "resume_job_matches.json")
OUTPUT_DB_PATH = os.environ.get("ROLEMATCHER_MATCHES_DB") or os.path.join(BASE_DIR, "resume_job_matches.db")
EMBED_CACHE_PATH = os.environ.get("ROLEMATCHER_JOB_EMBED_CACHE_JSON") or os.path.join(BASE_DIR, "job_embeddings_cache.json")

MODEL_NAME = "all-MiniLM-L6-v2"
TOP_N_PRINT = 5
MAX_JOBS_PER_RESUME = int(os.environ.get("ROLEMATCHER_MAX_PREFILTER_JOBS", "250"))
EXPERIENCE_PENALTY_POINTS = float(os.environ.get("ROLEMATCHER_EXPERIENCE_PENALTY_POINTS", "30"))
DEGREE_PENALTY_POINTS = float(os.environ.get("ROLEMATCHER_DEGREE_PENALTY_POINTS", "24"))
CATEGORY_PENALTY_POINTS = float(os.environ.get("ROLEMATCHER_CATEGORY_PENALTY_POINTS", "36"))
LOCATION_MODE = (os.environ.get("ROLEMATCHER_LOCATION_MODE") or "current").strip().lower()
SELECTED_COUNTRIES = [x.strip() for x in (os.environ.get("ROLEMATCHER_SELECTED_COUNTRIES") or "").split(",") if x.strip()]
PARALLEL_SCORE_WORKERS = min(max(1, int(os.environ.get("ROLEMATCHER_PARALLEL_SCORE_WORKERS", "3"))), max(1, (os.cpu_count() or 4)))
PARALLEL_ROW_WORKERS = min(max(1, int(os.environ.get("ROLEMATCHER_PARALLEL_ROW_WORKERS", "3"))), max(1, (os.cpu_count() or 4)))
if LOW_MEMORY_MODE:
    PARALLEL_SCORE_WORKERS = 1
    PARALLEL_ROW_WORKERS = 1


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def json_dump(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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


def _safe_json_load(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _job_text_hash(job: Dict[str, Any]) -> str:
    text = _job_text(job)
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:20]


def _job_embed_cache_key(job: Dict[str, Any], idx: int) -> str:
    return f"{_canonical_job_id(job, idx)}::{_job_text_hash(job)}"


def _load_embedding_cache(path: str) -> Dict[str, List[float]]:
    raw = _safe_json_load(path, {})
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, List[float]] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            vec = value.get("vector")
        else:
            vec = value
        if isinstance(vec, list) and vec:
            try:
                out[str(key)] = [float(x) for x in vec]
            except Exception:
                continue
    return out


def _save_embedding_cache(path: str, cache: Dict[str, List[float]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {k: {"vector": v} for k, v in cache.items()}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)


def _score_chunk_np_proc(args: Tuple[np.ndarray, np.ndarray]) -> np.ndarray:
    resume_vec, chunk = args
    return np.matmul(chunk, resume_vec).astype(np.float32)


def _build_rows_chunk(args: Tuple[str, Dict[str, Any], List[Dict[str, Any]], Dict[str, float]]) -> List[Dict[str, Any]]:
    resume_id, profile, jobs_chunk, score_map = args
    rows: List[Dict[str, Any]] = []
    for idx, job in enumerate(jobs_chunk):
        jid = _canonical_job_id(job, idx)
        rows.append(build_row(resume_id, profile, job, float(score_map.get(jid, 0.0))))
    return rows


def _job_is_pre_enriched(job: Dict[str, Any]) -> bool:
    needed = [
        "job_category",
        "job_function",
        "job_domain",
        "job_category_key",
        "country",
        "work_mode",
    ]
    return all(str(job.get(k) or "").strip() for k in needed)


def print_top_jobs_for_resume(resume_id: str, rows: List[Dict[str, Any]], top_n: int) -> None:
    print("\n" + "=" * 90)
    print(f"Top {top_n} jobs for resume: {resume_id}")
    print("=" * 90)
    for rank, r in enumerate(rows[:top_n], start=1):
        extra = []
        if r.get("penalty_applied"):
            extra.append(f"penalty: -{r.get('penalty_points', 0):g}")
        if r.get("required_experience_years") is not None:
            extra.append(f"exp req {r.get('required_experience_years')}y")
        if r.get("resume_experience_years") is not None:
            extra.append(f"resume {r.get('resume_experience_years')}y")
        if r.get("job_category"):
            extra.append(f"cat {r.get('job_category')}")
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
                penalty_points REAL,
                job_category TEXT,
                resume_category TEXT,
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
                penalty_applied, penalty_points,
                job_category, resume_category, url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                float(r.get("penalty_points", 0.0) or 0.0),
                str(r.get("job_category", "")),
                str(r.get("resume_category", "")),
                str(r.get("url", "")),
            ) for r in rows],
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_matches_resume_rank ON matches (resume_id, rank)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_matches_resume_score ON matches (resume_id, final_match_percent DESC)")
        conn.commit()
    finally:
        conn.close()


def prebuild_job_tfidf(job_payloads: Sequence[Tuple[int, Dict[str, Any], str]]) -> Tuple[TfidfVectorizer, Any]:
    texts = [txt for _, _, txt in job_payloads]
    vectorizer = TfidfVectorizer(stop_words="english", max_features=25000, ngram_range=(1, 2), min_df=1)
    matrix = vectorizer.fit_transform(texts)
    return vectorizer, matrix


def lexical_scores_for_resume(vectorizer: TfidfVectorizer, job_matrix: Any, resume_text: str) -> np.ndarray:
    resume_vec = vectorizer.transform([resume_text])
    sims = cosine_similarity(resume_vec, job_matrix)[0]
    return np.asarray(sims, dtype=np.float32)


def parallel_vector_similarity(resume_vec: np.ndarray, pending_vecs: np.ndarray) -> List[float]:
    if pending_vecs.size == 0:
        return []
    workers = PARALLEL_SCORE_WORKERS
    if workers <= 1 or pending_vecs.shape[0] < 128:
        return np.matmul(pending_vecs, resume_vec).astype(float).tolist()

    chunk_size = max(64, math.ceil(pending_vecs.shape[0] / workers))
    chunks = [(idx, pending_vecs[idx:idx + chunk_size]) for idx in range(0, pending_vecs.shape[0], chunk_size)]

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


def _canonical_job_id(job: Dict[str, Any], idx: int) -> str:
    value = str(job.get("job_id") or "").strip()
    if value:
        return value
    return f"JOB-{idx:05d}"


def _job_text(job: Dict[str, Any]) -> str:
    return " ".join([
        str(job.get("title") or ""),
        str(job.get("company") or ""),
        str(job.get("location") or ""),
        str(job.get("description_text") or ""),
        str(job.get("job_category") or ""),
        str(job.get("degree_family") or ""),
    ]).strip()


def _semantic_encode_jobs(model: Any, payloads: Sequence[Tuple[int, Dict[str, Any], str]]) -> Dict[str, np.ndarray]:
    cache = _load_embedding_cache(EMBED_CACHE_PATH)
    out: Dict[str, np.ndarray] = {}
    missing_payloads: List[Tuple[int, Dict[str, Any], str, str]] = []

    for idx, job, txt in payloads:
        key = _job_embed_cache_key(job, idx)
        cached_vec = cache.get(key)
        if cached_vec:
            out[_canonical_job_id(job, idx)] = np.asarray(cached_vec, dtype=np.float32)
        else:
            missing_payloads.append((idx, job, txt, key))

    if missing_payloads:
        texts = [_job_text(job) for _, job, _, _ in missing_payloads]
        vecs = model.encode(
            texts,
            batch_size=48 if LOW_MEMORY_MODE else 160,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        for (idx, job, _, key), vec in zip(missing_payloads, vecs):
            arr = np.asarray(vec, dtype=np.float32)
            out[_canonical_job_id(job, idx)] = arr
            cache[key] = arr.astype(float).tolist()
        try:
            _save_embedding_cache(EMBED_CACHE_PATH, cache)
        except Exception:
            pass

    return out


def _semantic_encode_resumes(model: Any, resumes: Sequence[Dict[str, Any]]) -> Dict[str, np.ndarray]:
    texts = []
    ids = []
    for resume in resumes:
        rid = str(resume.get("resume_id") or "")
        if not rid:
            continue
        body = " ".join([
            str(resume.get("summary") or ""),
            str(resume.get("skills") or ""),
            str(resume.get("experience") or ""),
            str(resume.get("projects") or ""),
            str(resume.get("education") or ""),
            str(resume.get("resume_text") or ""),
        ]).strip()
        ids.append(rid)
        texts.append(body)
    if not ids:
        return {}
    vecs = model.encode(texts, batch_size=8 if LOW_MEMORY_MODE else 32, normalize_embeddings=True, show_progress_bar=False)
    return {rid: np.asarray(vec, dtype=np.float32) for rid, vec in zip(ids, vecs)}


def build_row(
    resume_id: str,
    resume_profile: Dict[str, Any],
    job: Dict[str, Any],
    raw_sim: float,
) -> Dict[str, Any]:
    job_id = _canonical_job_id(job, 0)
    raw_percent = clamp(float(raw_sim) * 100.0, 0.0, 100.0)
    required_years = job.get("experience_needed_years")
    if required_years in (None, ""):
        required_years = extract_required_years(str(job.get("description_text") or ""))
    try:
        required_years = None if required_years in (None, "") else float(required_years)
    except Exception:
        required_years = extract_required_years(str(job.get("description_text") or ""))

    resume_exp_years = resume_profile.get("candidate_experience_years")
    category_score = float(job.get("prefilter_category_score") or category_similarity(str(resume_profile.get("candidate_category") or "General"), str(job.get("job_category") or "General")) or 0.0)
    degree_score = degree_fit_score(
        str(resume_profile.get("candidate_degree_level") or "none"),
        str(resume_profile.get("candidate_degree_family") or "General"),
        str(job.get("degree_level_min") or "none"),
        str(job.get("degree_family") or "General"),
    )

    penalty_points = 0.0
    penalty_applied = False
    if category_score < 0.58:
        penalty_points += CATEGORY_PENALTY_POINTS * max(0.0, 1.08 - category_score)
        penalty_applied = True
    if degree_score < 0.8:
        penalty_points += DEGREE_PENALTY_POINTS * max(0.0, 1.02 - degree_score)
        penalty_applied = True
    if required_years is not None and resume_exp_years is not None and float(resume_exp_years) < float(required_years):
        gap = max(0.0, float(required_years) - float(resume_exp_years))
        penalty_points += min(EXPERIENCE_PENALTY_POINTS + 12.0, 8.0 + (gap * 5.5))
        penalty_applied = True

    final_percent = max(0.0, raw_percent - penalty_points)
    return {
        "resume_id": resume_id,
        "job_id": job_id,
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "country": job.get("country", ""),
        "work_mode": canonicalize_work_mode(str(job.get("work_mode", "On-site"))),
        "job_category": job.get("job_category", "General"),
        "resume_category": resume_profile.get("candidate_category", "General"),
        "job_function": job.get("job_function", ""),
        "resume_function": resume_profile.get("candidate_function", ""),
        "job_domain": job.get("job_domain", ""),
        "resume_domain": resume_profile.get("candidate_domain", ""),
        "job_category_key": job.get("job_category_key", ""),
        "resume_category_key": resume_profile.get("candidate_category_key", ""),
        "raw_match_percent": round(raw_percent, 2),
        "final_match_percent": round(final_percent, 2),
        "required_experience_years": required_years,
        "resume_experience_years": resume_exp_years,
        "required_degree_level": job.get("degree_level_min", "none"),
        "resume_degree_level": resume_profile.get("candidate_degree_level", "none"),
        "required_degree_family": job.get("degree_family", "General"),
        "resume_degree_family": resume_profile.get("candidate_degree_family", "General"),
        "penalty_applied": penalty_applied,
        "penalty_points": round(penalty_points, 2),
        "prefilter_score": job.get("prefilter_score"),
        "prefilter_category_score": job.get("prefilter_category_score"),
        "prefilter_degree_score": job.get("prefilter_degree_score"),
        "prefilter_experience_score": job.get("prefilter_experience_score"),
        "prefilter_function_score": job.get("prefilter_function_score"),
        "prefilter_domain_score": job.get("prefilter_domain_score"),
        "url": job.get("source_url") or job.get("url") or job.get("job_url") or "",
        "posted_date": job.get("posted_date") or "",
    }


def run() -> None:
    overall_start = time.perf_counter()
    if not os.path.exists(RESUMES_PATH):
        raise SystemExit(f"Missing file: {RESUMES_PATH}")
    if not os.path.exists(JOBS_PATH):
        raise SystemExit(f"Missing file: {JOBS_PATH}")

    load_start = time.perf_counter()
    resumes_raw = ensure_list(load_json(RESUMES_PATH))
    jobs_raw = []
    for j in ensure_list(load_json(JOBS_PATH)):
        jobs_raw.append(j if _job_is_pre_enriched(j) else enrich_job_record(j))
    log_stage("load_inputs", load_start)

    if not resumes_raw:
        raise SystemExit("No resumes found in scanned_resumes.json")
    if not jobs_raw:
        raise SystemExit("No jobs found in scraped_jobs.json")

    shortlist_start = time.perf_counter()
    shortlisted_per_resume: Dict[str, List[Dict[str, Any]]] = {}
    shortlist_meta: Dict[str, Dict[str, Any]] = {}
    union_jobs: Dict[str, Dict[str, Any]] = {}
    resume_profiles: Dict[str, Dict[str, Any]] = {}
    ordered_resumes: List[Dict[str, Any]] = []

    for i, resume in enumerate(resumes_raw):
        resume_id = str(resume.get("resume_id") or f"RES-{i:04d}")
        resume["resume_id"] = resume_id
        if not str(resume.get("candidate_category") or "").strip():
            inferred = infer_resume_profile(
                str(resume.get("resume_text") or ""),
                {
                    "summary": str(resume.get("summary") or ""),
                    "education": str(resume.get("education") or ""),
                    "skills": str(resume.get("skills") or ""),
                    "experience": str(resume.get("experience") or ""),
                    "projects": str(resume.get("projects") or ""),
                },
            )
            resume.update({k: v for k, v in inferred.items() if resume.get(k) in (None, "", [], {})})
        profile = {
            "candidate_country": resume.get("candidate_country"),
            "candidate_experience_years": resume.get("candidate_experience_years"),
            "candidate_degree_level": resume.get("candidate_degree_level"),
            "candidate_degree_family": resume.get("candidate_degree_family"),
            "candidate_category": resume.get("candidate_category"),
            "candidate_category_confidence": resume.get("candidate_category_confidence"),
            "candidate_function": resume.get("candidate_function"),
            "candidate_function_scores": resume.get("candidate_function_scores") or {},
            "candidate_domain": resume.get("candidate_domain"),
            "candidate_domain_scores": resume.get("candidate_domain_scores") or {},
            "candidate_category_key": resume.get("candidate_category_key"),
            "candidate_category_key_confidence": resume.get("candidate_category_key_confidence"),
        }
        resume_profiles[resume_id] = profile
        short, meta = shortlist_jobs_for_resume(
            jobs_raw,
            profile,
            location_mode=LOCATION_MODE,
            selected_countries=SELECTED_COUNTRIES,
            max_jobs=MAX_JOBS_PER_RESUME,
        )
        shortlisted_per_resume[resume_id] = short
        shortlist_meta[resume_id] = meta
        ordered_resumes.append(resume)
        for idx, job in enumerate(short):
            union_jobs.setdefault(_canonical_job_id(job, idx), job)
        print(
            f"Resume {resume_id}: key={profile.get('candidate_category_key')} legacy={profile.get('candidate_category')} degree={profile.get('candidate_degree_level')}/{profile.get('candidate_degree_family')} "
            f"country={profile.get('candidate_country') or 'unknown'} shortlisted {meta.get('jobs_shortlisted')} of {meta.get('jobs_seen')} jobs"
        )
    log_stage("shortlist_jobs", shortlist_start)

    union_payloads = [(idx, job, _job_text(job)) for idx, job in enumerate(union_jobs.values())]
    if not union_payloads:
        raise SystemExit("No jobs remained after country filtering and prefiltering.")

    model = None
    if LOW_MEMORY_MODE:
        print("Low-memory mode enabled. Using lighter matching settings.")
    if HAVE_SENTENCE_TRANSFORMERS and SentenceTransformer is not None:
        model_start = time.perf_counter()
        print(f"Using SentenceTransformer model: {MODEL_NAME}")
        model = get_sentence_transformer(MODEL_NAME)
        log_stage("load_model", model_start)
    else:
        print("sentence_transformers not found or disabled. Falling back to TF-IDF cosine similarity.")

    lexical_start = time.perf_counter()
    vectorizer, job_tfidf = prebuild_job_tfidf(union_payloads)
    union_job_ids = [_canonical_job_id(job, idx) for idx, job, _ in union_payloads]
    union_job_index = {jid: idx for idx, jid in enumerate(union_job_ids)}
    log_stage("build_union_job_tfidf", lexical_start)

    semantic_job_embeddings: Dict[str, np.ndarray] = {}
    semantic_resume_embeddings: Dict[str, np.ndarray] = {}
    if model is not None:
        semantic_job_start = time.perf_counter()
        semantic_job_embeddings = _semantic_encode_jobs(model, union_payloads)
        semantic_resume_embeddings = _semantic_encode_resumes(model, ordered_resumes)
        log_stage("encode_semantic_vectors", semantic_job_start)

    build_rows_start = time.perf_counter()
    all_results: List[Dict[str, Any]] = []
    for resume in ordered_resumes:
        resume_id = str(resume["resume_id"])
        profile = resume_profiles[resume_id]
        resume_text = " ".join([
            str(resume.get("summary") or ""),
            str(resume.get("education") or ""),
            str(resume.get("skills") or ""),
            str(resume.get("experience") or ""),
            str(resume.get("projects") or ""),
            str(resume.get("resume_text") or ""),
        ]).strip()
        lexical_scores = lexical_scores_for_resume(vectorizer, job_tfidf, resume_text or "resume")
        semantic_scores: Dict[str, float] = {}
        if semantic_job_embeddings and resume_id in semantic_resume_embeddings:
            candidate_ids = [_canonical_job_id(job, idx) for idx, job in enumerate(shortlisted_per_resume[resume_id])]
            candidate_ids = [jid for jid in candidate_ids if jid in semantic_job_embeddings]
            pending_vecs = np.asarray([semantic_job_embeddings[jid] for jid in candidate_ids], dtype=np.float32)
            if pending_vecs.size > 0:
                raw_scores = parallel_vector_similarity(semantic_resume_embeddings[resume_id], pending_vecs)
                semantic_scores = {jid: float(score) for jid, score in zip(candidate_ids, raw_scores)}

        score_map: Dict[str, float] = {}
        shortlisted_jobs = shortlisted_per_resume[resume_id]
        for idx, job in enumerate(shortlisted_jobs):
            jid = _canonical_job_id(job, idx)
            raw_sim = semantic_scores.get(jid)
            if raw_sim is None:
                raw_sim = float(lexical_scores[union_job_index[jid]]) if jid in union_job_index else 0.0
            score_map[jid] = float(raw_sim)

        rows: List[Dict[str, Any]] = []
        if PARALLEL_ROW_WORKERS > 1 and len(shortlisted_jobs) >= 72:
            chunk_size = max(24, math.ceil(len(shortlisted_jobs) / PARALLEL_ROW_WORKERS))
            chunks = [shortlisted_jobs[i:i + chunk_size] for i in range(0, len(shortlisted_jobs), chunk_size)]
            args = [(resume_id, profile, chunk, score_map) for chunk in chunks]
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(PARALLEL_ROW_WORKERS, len(chunks))) as ex:
                    for part in ex.map(_build_rows_chunk, args):
                        rows.extend(part)
            except Exception:
                rows = []

        if not rows:
            for idx, job in enumerate(shortlisted_jobs):
                jid = _canonical_job_id(job, idx)
                rows.append(build_row(resume_id, profile, job, score_map.get(jid, 0.0)))

        for row in rows:
            row["shortlist_jobs_considered"] = shortlist_meta[resume_id].get("jobs_shortlisted")
            row["location_mode"] = shortlist_meta[resume_id].get("location_mode")
            row["selected_countries"] = shortlist_meta[resume_id].get("selected_countries")

        rows.sort(key=lambda x: (-float(x.get("final_match_percent", 0.0) or 0.0), -float(x.get("raw_match_percent", 0.0) or 0.0)))
        for rank, row in enumerate(rows, start=1):
            row["rank"] = rank
        print_top_jobs_for_resume(resume_id, rows, TOP_N_PRINT)
        all_results.extend(rows)
    log_stage("build_rows", build_rows_start)

    save_start = time.perf_counter()
    json_dump(OUTPUT_PATH, all_results)
    write_results_sqlite(all_results)
    log_stage("save_outputs", save_start)
    print(f"\nSaved {len(all_results)} rows total to: {OUTPUT_PATH} (top {MAX_JOBS_PER_RESUME} shortlisted jobs per resume)")
    print(f"Saved SQLite database to: {OUTPUT_DB_PATH}")
    log_stage("total", overall_start)


if __name__ == '__main__':
    run()
