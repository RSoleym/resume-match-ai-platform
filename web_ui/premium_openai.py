from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Sequence
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
}

COUNTRY_TO_ISO2 = {
    "canada": "CA",
    "united states": "US",
    "usa": "US",
    "us": "US",
    "united kingdom": "GB",
    "uk": "GB",
    "great britain": "GB",
    "ireland": "IE",
    "poland": "PL",
    "germany": "DE",
    "france": "FR",
    "netherlands": "NL",
    "israel": "IL",
    "india": "IN",
    "china": "CN",
    "japan": "JP",
    "south korea": "KR",
    "korea": "KR",
    "singapore": "SG",
    "australia": "AU",
    "new zealand": "NZ",
    "mexico": "MX",
    "brazil": "BR",
    "spain": "ES",
    "italy": "IT",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "finland": "FI",
    "switzerland": "CH",
    "austria": "AT",
    "belgium": "BE",
    "portugal": "PT",
    "taiwan": "TW",
    "hong kong": "HK",
    "malaysia": "MY",
    "costa rica": "CR",
    "czech republic": "CZ",
    "czechia": "CZ",
}


CATEGORY_ROLE_HINTS = {
    "Hardware / RTL / Verification": [
        "Design Verification Engineer",
        "RTL Design Engineer",
        "ASIC Verification Engineer",
        "FPGA Engineer",
        "Digital Design Engineer",
        "Hardware Verification Engineer",
    ],
    "Embedded / Firmware": [
        "Embedded Firmware Engineer",
        "Embedded Software Engineer",
        "Firmware Engineer",
        "Platform Software Engineer",
        "Board Bring-Up Engineer",
    ],
    "Software Engineering": ["Software Engineer", "Backend Engineer", "C++ Software Engineer", "Python Engineer"],
    "Data / AI / ML": ["Machine Learning Engineer", "AI Engineer", "Data Scientist", "NLP Engineer"],
    "Electrical / Power / Controls": ["Electrical Engineer", "Controls Engineer", "Power Systems Engineer"],
}

CATEGORY_CORE_TERMS = {
    "Hardware / RTL / Verification": [
        "systemverilog", "verilog", "rtl", "design verification", "uvm", "asic", "fpga", "digital design",
        "hardware verification", "silicon", "semiconductor", "cpu", "gpu", "timing", "formal", "sva",
    ],
    "Embedded / Firmware": [
        "embedded", "firmware", "cortex-m", "microcontroller", "board bring-up", "bare metal", "device driver", "c", "c++",
    ],
}

CATEGORY_AVOID_TERMS = {
    "Hardware / RTL / Verification": [
        "electromechanical", "electro mechanical", "mechanical", "civil", "construction", "hvac", "plc", "scada",
        "field service", "technician", "sales", "electrical designer", "power systems",
    ],
    "Embedded / Firmware": ["mechanical", "civil", "sales", "recruiter", "accounting"],
}

GENERIC_SEARCH_PAGE_PATTERNS = [
    "search - job bank",
    "jobs and work opportunities",
    "discover ",
    "browse jobs",
    "job search",
    "search results",
    "all jobs",
]

SEARCH_RESULT_HOST_PATTERNS = {
    "indeed": ["/jobs", "/q-", "jobs?", "/cmp/"],
    "linkedin": ["/jobs/search", "/jobs/collections/"],
    "glassdoor": ["job-listing", "jobs.htm"],
    "jobbank": ["jobsearch", "findajob"],
}

DIRECT_JOB_HOST_ALLOWLIST_HINTS = [
    "greenhouse.io", "lever.co", "workdayjobs.com", "myworkdayjobs.com", "ashbyhq.com", "smartrecruiters.com",
]

CRITICAL_SKILL_KEYWORDS = [
    "systemverilog", "verilog", "rtl", "uvm", "asic", "fpga", "embedded", "firmware", "cortex-m", "python", "c++", "vivado",
]

class OpenAIConfigError(RuntimeError):
    pass


def _clean_text(value: Any, max_chars: int) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text[:max_chars]


def _extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text, re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}
    return {}


def _chunked(rows: Sequence[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    size = max(1, int(size or 1))
    return [list(rows[i : i + size]) for i in range(0, len(rows), size)]


def _build_resume_payload(resume_context: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "country": _clean_text(resume_context.get("candidate_country"), 120),
        "experience_years": resume_context.get("candidate_experience_years"),
        "degree_level": _clean_text(resume_context.get("candidate_degree_level"), 80),
        "degree_family": _clean_text(resume_context.get("candidate_degree_family"), 120),
        "degree_fields": resume_context.get("candidate_degree_fields") or [],
        "category": _clean_text(resume_context.get("candidate_category") or resume_context.get("candidate_category_key"), 120),
        "function": _clean_text(resume_context.get("candidate_function"), 120),
        "function_scores": resume_context.get("candidate_function_scores") or {},
        "domain": _clean_text(resume_context.get("candidate_domain"), 120),
        "domain_scores": resume_context.get("candidate_domain_scores") or {},
        "summary": _clean_text(resume_context.get("summary"), 1400),
        "skills": _clean_text(resume_context.get("skills"), 1600),
        "experience": _clean_text(resume_context.get("experience"), 1800),
        "projects": _clean_text(resume_context.get("projects"), 1200),
        "education": _clean_text(resume_context.get("education"), 1000),
        "resume_text_excerpt": _clean_text(resume_context.get("resume_text"), 2600),
    }


def _build_job_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "job_id": str(job.get("job_id") or ""),
        "title": _clean_text(job.get("title"), 180),
        "company": _clean_text(job.get("company"), 140),
        "location": _clean_text(job.get("location"), 140),
        "country": _clean_text(job.get("country"), 80),
        "work_mode": _clean_text(job.get("work_mode"), 40),
        "posted_date": _clean_text(job.get("posted_date"), 40),
        "job_function": _clean_text(job.get("job_function"), 120),
        "job_domain": _clean_text(job.get("job_domain"), 120),
        "job_category": _clean_text(job.get("job_category"), 120),
        "degree_level_min": _clean_text(job.get("degree_level_min"), 60),
        "degree_family": _clean_text(job.get("degree_family"), 120),
        "experience_needed_years": job.get("experience_needed_years"),
        "description_excerpt": _clean_text(job.get("description_text") or job.get("description"), 1200),
    }


def _call_openai_batch(
    *,
    api_key: str,
    model: str,
    resume_context: Dict[str, Any],
    jobs_batch: Sequence[Dict[str, Any]],
    user_identifier: str = "",
) -> List[Dict[str, Any]]:
    system = (
        "You are a strict resume-to-job scoring assistant. "
        "Score every provided job from 0 to 100 for overall fit to the candidate. "
        "Use the resume and the job fields together, especially skills, domain, seniority, degree, experience, and work context. "
        "Do not invent jobs. Do not omit any provided job_id. Return only JSON."
    )
    user = {
        "resume": _build_resume_payload(resume_context),
        "jobs": [_build_job_payload(j) for j in jobs_batch],
        "rules": {
            "output_schema": {
                "scores": [
                    {"job_id": "string", "match_percentage": "number 0-100", "reason": "short string <= 18 words"}
                ]
            },
            "must_return_all_job_ids": True,
            "sort_not_required": True,
            "json_only": True,
        },
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "max_completion_tokens": 2200,
    }
    resp = requests.post(OPENAI_CHAT_URL, headers=headers, json=payload, timeout=180)
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    choices = data.get("choices") if isinstance(data, dict) else []
    content = ""
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else {}
        if isinstance(msg, dict):
            content = str(msg.get("content") or "")
    parsed = _extract_json(content)
    scores = parsed.get("scores") if isinstance(parsed, dict) else []
    out: List[Dict[str, Any]] = []
    if isinstance(scores, list):
        for item in scores:
            if not isinstance(item, dict):
                continue
            jid = str(item.get("job_id") or "").strip()
            if not jid:
                continue
            try:
                pct = float(item.get("match_percentage"))
            except Exception:
                pct = 0.0
            pct = max(0.0, min(100.0, pct))
            out.append({"job_id": jid, "match_percentage": pct, "reason": _clean_text(item.get("reason"), 120)})
    return out


def score_jobs_with_openai(
    resume_context: Dict[str, Any],
    jobs: Sequence[Dict[str, Any]],
    *,
    api_key: str,
    model: str = "gpt-4o-mini",
    batch_size: int = 25,
    user_identifier: str = "",
) -> List[Dict[str, Any]]:
    api_key = (api_key or "").strip()
    if not api_key or api_key == "PASTE_FRIEND_OPENAI_API_KEY_HERE":
        raise OpenAIConfigError(
            "OpenAI API key is not configured yet. Put it in the project root .env as OPENAI_API_KEY=... or add it in Render Environment."
        )
    jobs_list = [dict(j) for j in jobs if isinstance(j, dict)]
    batches = _chunked(jobs_list, batch_size)
    merged: Dict[str, Dict[str, Any]] = {}
    for batch in batches:
        scored = _call_openai_batch(
            api_key=api_key,
            model=model,
            resume_context=resume_context,
            jobs_batch=batch,
            user_identifier=user_identifier,
        )
        seen = {str(x.get("job_id") or "").strip() for x in scored if isinstance(x, dict)}
        for item in scored:
            jid = str(item.get("job_id") or "").strip()
            if jid:
                merged[jid] = item
        for job in batch:
            jid = str(job.get("job_id") or "").strip()
            if jid and jid not in seen and jid not in merged:
                merged[jid] = {"job_id": jid, "match_percentage": 0.0, "reason": "No score returned"}
    ordered: List[Dict[str, Any]] = []
    for job in jobs_list:
        jid = str(job.get("job_id") or "").strip()
        if jid:
            ordered.append(merged.get(jid, {"job_id": jid, "match_percentage": 0.0, "reason": "No score returned"}))
    return ordered


# -----------------------------
# Live web search support
# -----------------------------

def _extract_json_array(text: str) -> List[Dict[str, Any]]:
    text = (text or "").strip()
    if not text:
        return []
    try:
        value = json.loads(text)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            jobs = value.get("jobs") or value.get("results") or value.get("suitable_jobs")
            if isinstance(jobs, list):
                return [x for x in jobs if isinstance(x, dict)]
    except Exception:
        pass
    m = re.search(r"\[(?:.|\n|\r)*\]", text)
    if m:
        try:
            value = json.loads(m.group(0))
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        except Exception:
            pass
    obj = _extract_json(text)
    jobs = obj.get("jobs") if isinstance(obj, dict) else None
    if isinstance(jobs, list):
        return [x for x in jobs if isinstance(x, dict)]
    return []


def _clean_skill_terms(skills_text: Any, limit: int = 14) -> List[str]:
    raw = _clean_text(skills_text, 1000)
    if not raw:
        return []
    items: List[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,;/|\n]", raw):
        token = re.sub(r"\s+", " ", part).strip(" -")
        if len(token) < 2:
            continue
        low = token.lower()
        if low in seen:
            continue
        seen.add(low)
        items.append(token)
        if len(items) >= limit:
            break
    return items


def _unique_keep_order(values: Sequence[str], *, limit: int = 12) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in values:
        item = _clean_text(raw, 120)
        low = item.lower()
        if not item or low in seen:
            continue
        seen.add(low)
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _derive_resume_search_profile(resume_context: Dict[str, Any]) -> Dict[str, Any]:
    category = _clean_text(resume_context.get("candidate_category") or resume_context.get("candidate_category_key") or "General", 120)
    function_name = _clean_text(resume_context.get("candidate_function"), 120)
    domain_name = _clean_text(resume_context.get("candidate_domain"), 120)
    text_blob = " ".join(
        [
            _clean_text(resume_context.get("summary"), 1200),
            _clean_text(resume_context.get("skills"), 1400),
            _clean_text(resume_context.get("experience"), 1400),
            _clean_text(resume_context.get("projects"), 1200),
            _clean_text(resume_context.get("education"), 800),
            _clean_text(resume_context.get("resume_text"), 2400),
        ]
    ).lower()

    role_titles: List[str] = []
    role_titles.extend(CATEGORY_ROLE_HINTS.get(category, []))

    keyword_title_pairs = [
        ("design verification", "Design Verification Engineer"),
        ("uvm", "ASIC Verification Engineer"),
        ("systemverilog", "Design Verification Engineer"),
        ("rtl", "RTL Design Engineer"),
        ("verilog", "RTL Design Engineer"),
        ("fpga", "FPGA Engineer"),
        ("embedded", "Embedded Firmware Engineer"),
        ("firmware", "Firmware Engineer"),
        ("cortex-m", "Embedded Firmware Engineer"),
        ("board bring-up", "Embedded Firmware Engineer"),
    ]
    for token, title in keyword_title_pairs:
        if token in text_blob:
            if category == "Hardware / RTL / Verification" and title in {"Embedded Firmware Engineer", "Firmware Engineer"}:
                role_titles.append(title)
            else:
                role_titles.insert(0, title)

    if function_name and function_name.lower() not in {x.lower() for x in role_titles}:
        role_titles.insert(0, function_name)
    if domain_name and domain_name.lower() not in {x.lower() for x in role_titles}:
        role_titles.append(domain_name)
    role_titles = _unique_keep_order(role_titles, limit=8)

    keywords = _clean_skill_terms(resume_context.get("skills"), limit=18)
    for token in CRITICAL_SKILL_KEYWORDS:
        if token in text_blob:
            keywords.append(token)
    for token in CATEGORY_CORE_TERMS.get(category, []):
        if token in text_blob:
            keywords.append(token)
    keywords = _unique_keep_order(keywords, limit=18)

    negative_terms = _unique_keep_order(CATEGORY_AVOID_TERMS.get(category, []), limit=10)
    preferred_hosts: List[str] = []
    if category == "Hardware / RTL / Verification":
        preferred_hosts.extend(["amd.com", "nvidia.com", "qualcomm.com", "amazon.jobs", "stathera.com", "generac.com", "arm.com"])
    return {
        "category": category,
        "function": function_name,
        "domain": domain_name,
        "role_titles": role_titles,
        "keywords": keywords,
        "negative_terms": negative_terms,
        "preferred_hosts": _unique_keep_order(preferred_hosts, limit=10),
    }


def _relevance_blob_for_job(title: str, description_text: str, page_text: str) -> str:
    return " ".join([_clean_text(title, 220), _clean_text(description_text, 2000), _clean_text(page_text, 2500)]).lower()


def _job_relevance_score(profile: Dict[str, Any], *, title: str, description_text: str, page_text: str) -> float:
    blob = _relevance_blob_for_job(title, description_text, page_text)
    score = 0.0
    for role in profile.get("role_titles") or []:
        role_low = str(role).lower()
        if role_low and role_low in blob:
            score += 4.5
    for keyword in profile.get("keywords") or []:
        kw = str(keyword).lower().strip()
        if kw and kw in blob:
            score += 1.4 if len(kw.split()) == 1 else 2.2
    for neg in profile.get("negative_terms") or []:
        bad = str(neg).lower().strip()
        if bad and bad in blob:
            score -= 3.2
    category = str(profile.get("category") or "")
    if category == "Hardware / RTL / Verification":
        if any(term in blob for term in ["systemverilog", "verilog", "rtl", "design verification", "uvm", "asic", "fpga", "digital design"]):
            score += 5.0
        if any(term in blob for term in ["electrical designer", "electromechanical", "mechanical engineer", "controls engineer", "power systems"]):
            score -= 5.5
    if category == "Embedded / Firmware":
        if any(term in blob for term in ["embedded", "firmware", "cortex-m", "microcontroller", "bare metal"]):
            score += 4.5
    return score


def _guess_country_from_text(location_text: str) -> str:
    low = _clean_text(location_text, 220).lower()
    if not low:
        return ""
    for key, iso2 in COUNTRY_TO_ISO2.items():
        if key in low:
            return key.title() if len(key) > 2 else iso2
    if ", on" in low or "toronto" in low or "markham" in low or "ontario" in low:
        return "Canada"
    if any(x in low for x in [", ca", ", tx", ", ny", "united states", "usa"]):
        return "United States"
    return ""


def _parse_relative_posted_date(value: str) -> str:
    raw = _clean_text(value, 80).lower()
    if not raw:
        return ""
    now = datetime.now(timezone.utc)
    m = re.search(r"(\d+)\s+(day|days|hour|hours|week|weeks)", raw)
    if m:
        amount = int(m.group(1))
        unit = m.group(2)
        if unit.startswith("hour"):
            dt = now - timedelta(hours=amount)
        elif unit.startswith("week"):
            dt = now - timedelta(days=amount * 7)
        else:
            dt = now - timedelta(days=amount)
        return dt.date().isoformat()
    if "today" in raw or "just posted" in raw:
        return now.date().isoformat()
    if "yesterday" in raw:
        return (now - timedelta(days=1)).date().isoformat()
    return _clean_text(value, 80)


def _looks_like_search_result_page(url: str, title: str, page_text: str) -> bool:
    low_url = (url or "").lower()
    low_title = _clean_text(title, 220).lower()
    low_page = _clean_text(page_text, 1200).lower()
    host = urlparse(low_url).netloc
    for host_hint, path_patterns in SEARCH_RESULT_HOST_PATTERNS.items():
        if host_hint in host and any(pattern in low_url for pattern in path_patterns):
            return True
    if any(pattern in low_title for pattern in GENERIC_SEARCH_PAGE_PATTERNS):
        return True
    generic_page_terms = [
        "create job alert", "browse jobs", "salary guide", "search thousands of jobs", "find jobs", "work opportunities",
        "job search results", "search jobs", "sign in to view your jobs", "upload your resume",
    ]
    hits = sum(1 for term in generic_page_terms if term in low_page)
    return hits >= 2


def _extract_jobposting_fields_from_jsonld(soup: BeautifulSoup) -> Dict[str, str]:
    def _walk(obj, out):
        if isinstance(obj, dict):
            typ = obj.get("@type")
            if typ == "JobPosting" or (isinstance(typ, list) and "JobPosting" in typ):
                out.append(obj)
            for value in obj.values():
                _walk(value, out)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item, out)

    candidates = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(" ", strip=True) or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        _walk(payload, candidates)

    for item in candidates:
        title = _clean_text(item.get("title") or item.get("name"), 220)
        org = item.get("hiringOrganization") if isinstance(item.get("hiringOrganization"), dict) else {}
        company = _clean_text(org.get("name"), 180)
        desc = _clean_text(item.get("description"), 2000)
        emp_type = item.get("employmentType")
        if isinstance(emp_type, list):
            emp_type = " / ".join([str(x) for x in emp_type if x])
        work_mode = "Remote" if str(item.get("jobLocationType") or "").lower() == "telecommute" else _clean_text(emp_type, 80)
        job_loc = item.get("jobLocation")
        location_parts: List[str] = []
        country = ""
        if isinstance(job_loc, list):
            job_loc = job_loc[0] if job_loc else {}
        if isinstance(job_loc, dict):
            address = job_loc.get("address") if isinstance(job_loc.get("address"), dict) else {}
            locality = _clean_text(address.get("addressLocality"), 120)
            region = _clean_text(address.get("addressRegion"), 120)
            country = _clean_text(address.get("addressCountry"), 80)
            location_parts = [x for x in [locality, region, country] if x]
        posted_date = _clean_text(item.get("datePosted"), 80)
        return {
            "title": title,
            "company": company,
            "description_text": desc,
            "work_mode": work_mode,
            "location": ", ".join(location_parts),
            "country": country,
            "posted_date": posted_date,
        }
    return {}


def _country_to_iso2(value: str) -> str:
    text = (value or "").strip().lower()
    if not text:
        return ""
    if len(text) == 2 and text.isalpha():
        return text.upper()
    return COUNTRY_TO_ISO2.get(text, "")


def _build_user_location(country_filter: str, city_filter: str) -> Dict[str, Any] | None:
    iso2 = _country_to_iso2(country_filter)
    city = _clean_text(city_filter, 80)
    if not iso2 and not city:
        return None
    return {"type": "approximate", "country": iso2 or "US", "city": city or None, "region": city or None}


def _build_chat_user_location(country_filter: str, city_filter: str) -> Dict[str, Any] | None:
    iso2 = _country_to_iso2(country_filter)
    city = _clean_text(city_filter, 80)
    if not iso2 and not city:
        return None
    return {
        "type": "approximate",
        "approximate": {
            "country": iso2 or "US",
            "city": city or None,
            "region": city or None,
        },
    }


def _make_live_search_prompt(
    *,
    resume_context: Dict[str, Any],
    requested_count: int,
    country_filter: str,
    city_filter: str,
    work_mode_filter: str,
    posted_range: str,
    exclude_urls: Sequence[str],
    focus_titles: Sequence[str] | None = None,
) -> str:
    resume_payload = _build_resume_payload(resume_context)
    profile = _derive_resume_search_profile(resume_context)
    titles = _unique_keep_order(list(focus_titles or []) + list(profile.get("role_titles") or []), limit=6)
    keywords = _unique_keep_order(profile.get("keywords") or [], limit=14)
    avoid_terms = _unique_keep_order(profile.get("negative_terms") or [], limit=10)
    filters = {
        "country": country_filter or "any",
        "city": city_filter or "any",
        "work_mode": work_mode_filter or "any",
        "posted_range": posted_range or "all",
    }
    return (
        "Use live web search to find CURRENT direct job-posting detail pages that fit this resume. "
        "Use only the candidate features and the user-selected filters below. "
        "Do not return generic category pages, search results pages, or broad directory pages. "
        f"Target up to {requested_count} jobs. "
        f"Resume-derived role titles: {json.dumps(titles, ensure_ascii=False)}. "
        f"Strong resume keywords: {json.dumps(keywords, ensure_ascii=False)}. "
        f"Avoid unrelated job families: {json.dumps(avoid_terms, ensure_ascii=False)}. "
        f"User filters: {json.dumps(filters, ensure_ascii=False)}. "
        f"Already seen URLs to avoid: {json.dumps(list(exclude_urls)[-120:], ensure_ascii=False)}. "
        f"Candidate summary: {json.dumps(resume_payload, ensure_ascii=False)}. "
        "Return only valid JSON in this exact shape: "
        '{"jobs":[{"job_id":"string","title":"string","company":"string","url":"string",'
        '"location":"string","country":"string","work_mode":"string","posted_date":"string",'
        '"job_function":"string","job_domain":"string","job_category":"string",'
        '"match_percentage":0,"reason":"short string"}]}. '
        "Never invent links. Omit closed jobs."
    )


def _make_live_sources_prompt(
    *,
    resume_context: Dict[str, Any],
    requested_count: int,
    country_filter: str,
    city_filter: str,
    work_mode_filter: str,
    posted_range: str,
    exclude_urls: Sequence[str],
    focus_titles: Sequence[str] | None = None,
) -> str:
    resume_payload = _build_resume_payload(resume_context)
    profile = _derive_resume_search_profile(resume_context)
    titles = _unique_keep_order(list(focus_titles or []) + list(profile.get("role_titles") or []), limit=6)
    keywords = _unique_keep_order(profile.get("keywords") or [], limit=12)
    avoid_terms = _unique_keep_order(profile.get("negative_terms") or [], limit=10)
    filters = {
        "country": country_filter or "any",
        "city": city_filter or "any",
        "work_mode": work_mode_filter or "any",
        "posted_range": posted_range or "all",
    }
    return (
        f"Find up to {requested_count} current direct job-posting pages using live web search. "
        "Only use the resume-derived role titles and technical signals below. "
        "Do not return generic electrical, electromechanical, mechanical, or list pages unless the page clearly matches the resume keywords. "
        "Do not use search-result pages, category pages, or directory pages. The URLs must be job-detail pages. "
        f"Resume-derived role titles: {json.dumps(titles, ensure_ascii=False)}. "
        f"Strong resume keywords: {json.dumps(keywords, ensure_ascii=False)}. "
        f"Avoid unrelated families: {json.dumps(avoid_terms, ensure_ascii=False)}. "
        f"User filters: {json.dumps(filters, ensure_ascii=False)}. "
        f"Avoid URLs already seen: {json.dumps(list(exclude_urls)[-120:], ensure_ascii=False)}. "
        f"Candidate summary: {json.dumps(resume_payload, ensure_ascii=False)}. "
        "Reply briefly if needed, but the cited job-detail URLs are the main output."
    )


def _http_error_detail(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if response is None:
        return repr(exc)
    try:
        body = response.text
    except Exception:
        body = ""
    body = _clean_text(body, 1200)
    status = getattr(response, "status_code", "?")
    return f"HTTP {status}: {body}" if body else repr(exc)


def _extract_text_from_responses_payload(data: Dict[str, Any]) -> str:
    if not isinstance(data, dict):
        return ""
    value = data.get("output_text")
    if isinstance(value, str) and value.strip():
        return value.strip()
    parts: List[str] = []
    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "message":
                for part in item.get("content", []) if isinstance(item.get("content"), list) else []:
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        parts.append(part.get("text", ""))
    return "\n".join([p for p in parts if p]).strip()


def _collect_source_urls(obj: Any) -> List[Dict[str, str]]:
    found: List[Dict[str, str]] = []
    seen: set[str] = set()

    def add(url: str, title: str = "") -> None:
        url = _clean_text(url, 500)
        title = _clean_text(title, 240)
        if not url or url in seen:
            return
        seen.add(url)
        found.append({"url": url, "title": title})

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if isinstance(node.get("sources"), list):
                for item in node.get("sources"):
                    if isinstance(item, dict):
                        add(item.get("url") or item.get("link") or "", item.get("title") or item.get("name") or "")
            if node.get("type") == "url_citation":
                uc = node.get("url_citation") if isinstance(node.get("url_citation"), dict) else node
                add(uc.get("url") or "", uc.get("title") or "")
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(obj)
    return found


def _extract_urls_from_text(text: str) -> List[Dict[str, str]]:
    found: List[Dict[str, str]] = []
    seen: set[str] = set()
    for match in re.findall(r"https?://[^\s)\]>\"']+", text or ""):
        url = _clean_text(match.rstrip('.,;'), 500)
        if not url or url in seen:
            continue
        seen.add(url)
        found.append({"url": url, "title": ""})
    return found


def _is_generic_careers_url(url: str) -> bool:
    low = (url or "").lower().strip()
    if not low:
        return True
    bad_patterns = [
        "/careers/",
        "/jobs/",
        "/job-search",
        "/search-jobs",
        "/careers$",
    ]
    good_patterns = [
        "/job/",
        "jobid=",
        "job_id=",
        "gh_jid=",
        "requisition",
        "req=",
        "reqid=",
        "/positions/",
        "/jobs/view/",
        "/vacancy/",
        "/posting/",
        "/opportunity/",
    ]
    if any(x in low for x in good_patterns):
        return False
    if any(x.rstrip('$') in low for x in bad_patterns):
        parsed = urlparse(low)
        path = parsed.path.rstrip('/')
        if path.endswith('/careers') or path.endswith('/jobs') or 'search' in path:
            return True
    return False


def _fetch_job_page_metadata(url: str) -> Dict[str, str]:
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        resp.raise_for_status()
    except Exception:
        return {}
    html = resp.text or ""
    soup = BeautifulSoup(html, "html.parser")

    ld_json = _extract_jobposting_fields_from_jsonld(soup)

    title = _clean_text(ld_json.get("title"), 220)
    if not title and soup.title and soup.title.string:
        title = _clean_text(soup.title.string, 220)
    if not title:
        og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "og:title"})
        if og and og.get("content"):
            title = _clean_text(og.get("content"), 220)

    desc = _clean_text(ld_json.get("description_text"), 2000)
    if not desc:
        md = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if md and md.get("content"):
            desc = _clean_text(md.get("content"), 1600)

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    location = _clean_text(ld_json.get("location"), 200)
    country = _clean_text(ld_json.get("country"), 120)
    if not country:
        country = _guess_country_from_text(location or text[:300])

    return {
        "title": title,
        "company": _clean_text(ld_json.get("company"), 180),
        "location": location,
        "country": country,
        "work_mode": _clean_text(ld_json.get("work_mode"), 80),
        "posted_date": _parse_relative_posted_date(ld_json.get("posted_date") or ""),
        "description_text": _clean_text(desc or text, 1800),
        "page_text": _clean_text(text, 2600),
    }


def _split_page_title(raw_title: str) -> tuple[str, str]:
    title = _clean_text(raw_title, 220)
    if not title:
        return "", ""
    low = title.lower()
    for token in (" job details", " careers", " career", " jobs", " job opening", " application"):
        if low.endswith(token):
            title = title[: -len(token)].strip(" -|—–")
            low = title.lower()
    for sep in (" | ", " — ", " – ", " - "):
        if sep in title:
            left, right = title.split(sep, 1)
            left = _clean_text(left, 180)
            right = _clean_text(right, 180)
            if left and right:
                return left, right
    m = re.match(r"(.+?)\s+at\s+(.+)$", title, flags=re.I)
    if m:
        return _clean_text(m.group(1), 180), _clean_text(m.group(2), 180)
    return title, ""


def _source_rows_to_jobs(
    sources: Sequence[Dict[str, str]],
    country_filter: str,
    city_filter: str,
    *,
    search_model: str = "",
    resume_context: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    profile = _derive_resume_search_profile(resume_context or {})
    for idx, src in enumerate(sources, start=1):
        url = _clean_text(src.get("url"), 500)
        if not url or url in seen or _is_generic_careers_url(url):
            continue
        seen.add(url)
        citation_title = _clean_text(src.get("title"), 220)
        meta = _fetch_job_page_metadata(url)
        meta_title = _clean_text(meta.get("title"), 220)
        split_title, split_company = _split_page_title(meta_title or citation_title)
        title = split_title or citation_title or meta_title
        host = (urlparse(url).netloc or "").replace("www.", "")
        company = _clean_text(meta.get("company") or split_company or host.split(".")[0].replace("-", " ").title(), 180)
        description_text = _clean_text(meta.get("description_text") or meta.get("page_text") or "", 1800)
        page_text = _clean_text(meta.get("page_text") or description_text, 2400)
        if _looks_like_search_result_page(url, title, page_text):
            continue
        relevance = _job_relevance_score(profile, title=title, description_text=description_text, page_text=page_text)
        if profile.get("category") and relevance < 2.8:
            continue
        location = _clean_text(meta.get("location") or (f"{city_filter}, {country_filter}" if city_filter and country_filter else country_filter), 200)
        country = _clean_text(meta.get("country") or country_filter or _guess_country_from_text(location), 120)
        rows.append(
            {
                "job_id": f"WEB-{idx:05d}-{abs(hash(url)) % 1000000}",
                "title": _clean_text(title, 220),
                "company": company,
                "url": url,
                "source_url": url,
                "location": location,
                "country": country,
                "work_mode": _clean_text(meta.get("work_mode"), 60),
                "posted_date": _clean_text(meta.get("posted_date"), 80),
                "job_function": _clean_text(profile.get("function"), 120),
                "job_domain": _clean_text(profile.get("domain"), 120),
                "job_category": _clean_text(profile.get("category"), 120),
                "description_text": description_text,
                "page_text": page_text,
                "match_percentage": 0.0,
                "reason": "Found from live web search",
                "search_model": _clean_text(search_model, 120),
                "relevance_score": round(relevance, 3),
            }
        )
    rows.sort(key=lambda x: (-float(x.get("relevance_score") or 0.0), str(x.get("title") or "").lower()))
    return rows


def _extract_chat_content_and_annotations(data: Dict[str, Any]) -> tuple[str, List[Dict[str, str]]]:
    content = ""
    sources: List[Dict[str, str]] = []
    choices = data.get("choices") if isinstance(data, dict) else []
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else {}
        if isinstance(msg, dict):
            if isinstance(msg.get("content"), str):
                content = msg.get("content") or ""
            ann = msg.get("annotations")
            if isinstance(ann, list):
                for item in ann:
                    if isinstance(item, dict):
                        uc = item.get("url_citation") if isinstance(item.get("url_citation"), dict) else item
                        url = uc.get("url") if isinstance(uc, dict) else ""
                        title = uc.get("title") if isinstance(uc, dict) else ""
                        if url:
                            sources.append({"url": url, "title": title or ""})
    return content, sources


def _request_json_with_retries(
    *,
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    timeout_s: int,
    attempts: int,
) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt >= max(1, attempts):
                raise
            time.sleep(min(4.0, float(attempt)))
        except Exception:
            raise
    if last_exc is not None:
        raise last_exc
    return {}


def _pick_chat_search_model(model: str) -> str:
    requested = (model or "").strip()
    if requested in {"gpt-5-search-api", "gpt-4o-search-preview", "gpt-4o-mini-search-preview"}:
        return requested
    env_value = (os.environ.get("OPENAI_WEB_CHAT_MODEL") or "").strip()
    if env_value:
        return env_value
    return "gpt-4o-search-preview"


def _chat_search_for_source_rows(
    *,
    api_key: str,
    resume_context: Dict[str, Any],
    requested_count: int,
    country_filter: str,
    city_filter: str,
    work_mode_filter: str,
    posted_range: str,
    exclude_urls: Sequence[str],
    model_hint: str,
    focus_titles: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    chat_model = _pick_chat_search_model(model_hint)
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    prompt = _make_live_sources_prompt(
        resume_context=resume_context,
        requested_count=requested_count,
        country_filter=country_filter,
        city_filter=city_filter,
        work_mode_filter=work_mode_filter,
        posted_range=posted_range,
        exclude_urls=exclude_urls,
        focus_titles=focus_titles,
    )
    chat_payload: Dict[str, Any] = {
        "model": chat_model,
        "messages": [
            {
                "role": "developer",
                "content": "Find direct live job-detail pages only. Use the resume-derived titles and keywords. Ignore generic list pages.",
            },
            {"role": "user", "content": prompt},
        ],
        "web_search_options": {"search_context_size": "low"},
        "max_completion_tokens": 1400,
    }
    chat_location = _build_chat_user_location(country_filter, city_filter)
    if chat_location:
        chat_payload["web_search_options"]["user_location"] = chat_location
    data = _request_json_with_retries(
        url=OPENAI_CHAT_URL,
        headers=headers,
        payload=chat_payload,
        timeout_s=max(45, int(os.environ.get("OPENAI_WEB_CHAT_TIMEOUT_S", "60") or "60")),
        attempts=max(1, int(os.environ.get("OPENAI_WEB_CHAT_RETRIES", "2") or "2")),
    )
    content, annotation_sources = _extract_chat_content_and_annotations(data)
    source_candidates = annotation_sources + _extract_urls_from_text(content)
    rows = _source_rows_to_jobs(
        source_candidates,
        country_filter,
        city_filter,
        search_model=chat_model,
        resume_context=resume_context,
    )
    if rows:
        return rows
    jobs = _extract_json_array(content)
    if jobs:
        for item in jobs:
            if isinstance(item, dict) and not item.get("search_model"):
                item["search_model"] = chat_model
        return jobs
    return []


def _call_openai_live_search_once(
    *,
    api_key: str,
    model: str,
    resume_context: Dict[str, Any],
    requested_count: int,
    country_filter: str,
    city_filter: str,
    work_mode_filter: str,
    posted_range: str,
    exclude_urls: Sequence[str],
    user_identifier: str = "",
    focus_titles: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    primary_error = ""
    try:
        rows = _chat_search_for_source_rows(
            api_key=api_key,
            resume_context=resume_context,
            requested_count=requested_count,
            country_filter=country_filter,
            city_filter=city_filter,
            work_mode_filter=work_mode_filter,
            posted_range=posted_range,
            exclude_urls=exclude_urls,
            model_hint=model,
            focus_titles=focus_titles,
        )
        if rows:
            return rows
        primary_error = "Chat search returned no direct job sources."
    except Exception as e:
        primary_error = _http_error_detail(e)

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    prompt = _make_live_search_prompt(
        resume_context=resume_context,
        requested_count=requested_count,
        country_filter=country_filter,
        city_filter=city_filter,
        work_mode_filter=work_mode_filter,
        posted_range=posted_range,
        exclude_urls=exclude_urls,
        focus_titles=focus_titles,
    )

    tool_entry: Dict[str, Any] = {"type": "web_search"}
    user_location = _build_user_location(country_filter, city_filter)
    if user_location:
        tool_entry["user_location"] = user_location
    responses_payload: Dict[str, Any] = {
        "model": model,
        "tools": [tool_entry],
        "tool_choice": "auto",
        "include": ["web_search_call.action.sources"],
        "input": prompt,
        "max_output_tokens": 2600,
    }
    if model in {"gpt-5", "o4-mini", "o3"}:
        responses_payload["reasoning"] = {"effort": "low"}

    responses_timeout = max(75, int(os.environ.get("OPENAI_WEB_TIMEOUT_S", "120") or "120"))
    responses_attempts = max(1, int(os.environ.get("OPENAI_WEB_RETRIES", "2") or "2"))
    last_error = "Responses API returned no jobs."
    try:
        data = _request_json_with_retries(
            url=OPENAI_RESPONSES_URL,
            headers=headers,
            payload=responses_payload,
            timeout_s=responses_timeout,
            attempts=responses_attempts,
        )
        text_payload = _extract_text_from_responses_payload(data)
        jobs = _extract_json_array(text_payload)
        if jobs:
            for item in jobs:
                if isinstance(item, dict) and not item.get("search_model"):
                    item["search_model"] = model
            return jobs
        source_rows = _source_rows_to_jobs(
            _collect_source_urls(data),
            country_filter,
            city_filter,
            search_model=model,
            resume_context=resume_context,
        )
        if source_rows:
            return source_rows
        last_error = "Responses API returned no jobs or usable sources."
    except Exception as e:
        last_error = _http_error_detail(e)

    raise RuntimeError(f"Live web search failed. Primary error: {primary_error}. Fallback error: {last_error}")


def _normalize_live_job_row(job: Dict[str, Any], idx: int) -> Dict[str, Any]:
    url = _clean_text(job.get("url") or job.get("link") or job.get("source_url"), 500)
    if not url:
        return {}
    title = _clean_text(job.get("title") or job.get("job_title"), 220)
    company = _clean_text(job.get("company"), 180)
    location = _clean_text(job.get("location"), 180)
    country = _clean_text(job.get("country"), 120)
    try:
        match_pct = float(job.get("match_percentage"))
    except Exception:
        match_pct = 0.0
    return {
        "job_id": _clean_text(job.get("job_id") or url or f"WEB-{idx:05d}", 260),
        "title": title,
        "company": company,
        "url": url,
        "source_url": url,
        "location": location,
        "country": country,
        "work_mode": _clean_text(job.get("work_mode"), 60),
        "posted_date": _clean_text(job.get("posted_date") or job.get("days_posted"), 80),
        "job_function": _clean_text(job.get("job_function"), 120),
        "job_domain": _clean_text(job.get("job_domain"), 120),
        "job_category": _clean_text(job.get("job_category"), 120),
        "description_text": _clean_text(job.get("description_text") or job.get("page_text") or "", 1400),
        "match_percentage": max(0.0, min(100.0, match_pct)),
        "reason": _clean_text(job.get("reason"), 160),
        "search_model": _clean_text(job.get("search_model"), 120),
    }


def _broadening_plan(country_filter: str, city_filter: str, work_mode_filter: str, posted_range: str) -> List[Dict[str, str]]:
    plan = [
        {"country": country_filter, "city": city_filter, "work_mode": work_mode_filter, "posted": posted_range},
        {"country": country_filter, "city": "", "work_mode": work_mode_filter, "posted": posted_range},
        {"country": country_filter, "city": "", "work_mode": "", "posted": posted_range or "all"},
        {"country": country_filter, "city": "", "work_mode": "", "posted": "all"},
    ]
    uniq: List[Dict[str, str]] = []
    seen = set()
    for item in plan:
        key = (item["country"], item["city"], item["work_mode"], item["posted"])
        if key not in seen:
            seen.add(key)
            uniq.append(item)
    return uniq


def _make_focus_title_batches(resume_context: Dict[str, Any]) -> List[List[str]]:
    profile = _derive_resume_search_profile(resume_context)
    titles = list(profile.get("role_titles") or [])
    if not titles:
        return [[""]]
    batches: List[List[str]] = []
    for i in range(0, len(titles), 2):
        batches.append([x for x in titles[i : i + 2] if x])
    if titles:
        batches.append([titles[0]])
    return batches[:5] or [[""]]


def search_live_jobs_with_openai(
    resume_context: Dict[str, Any],
    *,
    api_key: str,
    model: str = "gpt-5",
    country_filter: str = "",
    city_filter: str = "",
    work_mode_filter: str = "",
    posted_range: str = "all",
    max_results: int = 250,
    progress_cb=None,
    user_identifier: str = "",
) -> List[Dict[str, Any]]:
    api_key = (api_key or "").strip()
    if not api_key or api_key == "PASTE_FRIEND_OPENAI_API_KEY_HERE":
        raise OpenAIConfigError(
            "OpenAI API key is not configured yet. Put it in the project root .env as OPENAI_API_KEY=... or add it in Render Environment."
        )

    target = max(5, min(int(max_results or 25), 60))
    merged: Dict[str, Dict[str, Any]] = {}
    seen_urls: List[str] = []
    seen_urls_lower: set[str] = set()
    attempts = _broadening_plan(country_filter, city_filter, work_mode_filter, posted_range)
    focus_batches = _make_focus_title_batches(resume_context)
    total_steps = max(1, len(attempts) * len(focus_batches))
    step_idx = 0

    for attempt in attempts:
        if len(merged) >= target:
            break
        for focus_titles in focus_batches:
            if len(merged) >= target:
                break
            step_idx += 1
            remaining = min(10, target - len(merged))
            label_focus = ", ".join([x for x in focus_titles if x]) or "resume-matched jobs"
            if callable(progress_cb):
                progress_cb(f"Searching live jobs: {label_focus}", step_idx, total_steps)
            try:
                jobs = _call_openai_live_search_once(
                    api_key=api_key,
                    model=model,
                    resume_context=resume_context,
                    requested_count=max(5, remaining),
                    country_filter=attempt.get("country", ""),
                    city_filter=attempt.get("city", ""),
                    work_mode_filter=attempt.get("work_mode", ""),
                    posted_range=attempt.get("posted", "all"),
                    exclude_urls=seen_urls,
                    user_identifier=user_identifier,
                    focus_titles=focus_titles,
                )
            except Exception:
                continue
            for idx, item in enumerate(jobs, start=1):
                if not isinstance(item, dict):
                    continue
                row = _normalize_live_job_row(item, len(merged) + idx)
                if not row:
                    continue
                url = str(row.get("url") or "").strip()
                low = url.lower()
                key = url or str(row.get("job_id") or "").strip()
                if not key:
                    continue
                if low and low in seen_urls_lower:
                    continue
                if key in merged:
                    continue
                merged[key] = row
                if low:
                    seen_urls_lower.add(low)
                    seen_urls.append(url)
                if len(merged) >= target:
                    break

    rows = list(merged.values())
    if not rows:
        return []

    rows.sort(key=lambda x: (-float(x.get("relevance_score") or 0.0), str(x.get("title") or "").lower()))
    rows = rows[: max(10, min(len(rows), 30))]

    if callable(progress_cb):
        progress_cb("Scoring the best matches", 1, 1)
    scored = score_jobs_with_openai(
        resume_context,
        rows,
        api_key=api_key,
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        batch_size=20,
        user_identifier=user_identifier,
    )
    score_map = {str(x.get("job_id") or "").strip(): x for x in scored if isinstance(x, dict)}
    for row in rows:
        jid = str(row.get("job_id") or "").strip()
        item = score_map.get(jid)
        if item:
            try:
                row["match_percentage"] = float(item.get("match_percentage") or 0.0)
            except Exception:
                row["match_percentage"] = 0.0
            row["reason"] = _clean_text(item.get("reason"), 160)

    rows.sort(
        key=lambda x: (
            -float(x.get("match_percentage") or 0.0),
            -float(x.get("relevance_score") or 0.0),
            str(x.get("title") or "").lower(),
        )
    )
    return rows[:target]
