#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import re
from datetime import date

import requests
from bs4 import BeautifulSoup

# ======================
# Paths / Settings
# ======================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PATH_TO_URLS = os.path.join(BASE_DIR, "job_urls.txt")
ARCHIVE_JSON = os.path.join(BASE_DIR, "job_url_archive.json")
OUTPUT_JSON  = os.path.join(BASE_DIR, "scraped_jobs.json")
OUTPUT_CSV   = os.path.join(BASE_DIR, "scraped_jobs.csv")

HEADERS = {"User-Agent": "Mozilla/5.0"}
MIN_DESC_LEN = 200
REQUEST_TIMEOUT = 30

# ======================
# Load archive
# ======================

archived_urls = set()
if os.path.exists(ARCHIVE_JSON):
    try:
        with open(ARCHIVE_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            archived_urls = set(data)
    except Exception:
        archived_urls = set()

# ======================
# Load existing jobs (NEVER DELETE)
# ======================

existing_jobs = []
jobs_by_url = {}

if os.path.exists(OUTPUT_JSON):
    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            existing_jobs = json.load(f) or []
        if isinstance(existing_jobs, list):
            jobs_by_url = {
                j.get("source_url", ""): j
                for j in existing_jobs
                if j.get("source_url")
            }
        else:
            existing_jobs = []
            jobs_by_url = {}
    except Exception:
        existing_jobs = []
        jobs_by_url = {}

# ======================
# Load URL queue
# ======================

with open(PATH_TO_URLS, "r", encoding="utf-8") as f:
    URLS = [ln.strip() for ln in f if ln.strip()]

# ======================
# Helpers
# ======================

def norm_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_jsonld(soup: BeautifulSoup):
    """
    Try to extract JobPosting data from JSON-LD.
    Returns: title, company, location, description_text, posted_date
    """
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            raw = tag.string or ""
            if not raw.strip():
                continue

            data = json.loads(raw)
            items = data if isinstance(data, list) else [data]

            for it in items:
                if it.get("@type") != "JobPosting":
                    continue

                title = it.get("title", "") or ""
                company = (it.get("hiringOrganization", {}) or {}).get("name", "") or ""

                addr = (it.get("jobLocation") or {}).get("address", {}) or {}
                location = " ".join(filter(None, [
                    addr.get("addressLocality"),
                    addr.get("addressRegion"),
                    addr.get("addressCountry"),
                ]))

                desc_html = it.get("description", "") or ""
                desc_text = BeautifulSoup(desc_html, "html.parser").get_text(" ", strip=True)

                posted = it.get("datePosted", "") or ""

                return title, company, location, norm_space(desc_text), posted

        except Exception:
            # ignore bad JSON-LD blocks
            pass

    return "", "", "", "", ""


def extract_generic(html: str):
    """
    Fallback extractor if JSON-LD is missing.
    Returns: title, company, location, description_text, posted_date
    """
    soup = BeautifulSoup(html, "html.parser")

    title, company, location, desc, posted = extract_jsonld(soup)

    if not desc:
        main = soup.find("main") or soup.find("article") or soup
        desc = norm_space(main.get_text(" ", strip=True))

    if not title:
        title = soup.title.get_text(strip=True) if soup.title else ""

    return title, company, location, desc, posted


# ======================
# Run scraper
# ======================

new_jobs = []
processed_urls = []

for url in URLS:
    print(f"\nProcessing: {url}")

    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.encoding = r.apparent_encoding
        html = r.text
    except Exception as e:
        print("❌ Fetch failed:", e)
        continue  # fetch failed → keep in queue

    processed_urls.append(url)  # ✅ ALWAYS mark as processed after successful fetch

    title, company, loc, desc, posted = extract_generic(html)

    if len(desc) < MIN_DESC_LEN:
        print("⚠ Description too short → NOT archived")
        continue  # but WILL be deleted from queue

    today = str(date.today())

    # ======================
    # DUPLICATE LOGIC
    # ======================

    if url in jobs_by_url:
        existing = jobs_by_url[url]

        if posted and posted != existing.get("posted_date"):
            print("🔁 Posting updated → refreshing job")

            existing.update({
                "title": title,
                "company": company,
                "location": loc,
                "description_text": desc,
                "posted_date": posted,
                "collected_date": today,
            })

            archived_urls.add(url)
        else:
            print("Duplicate unchanged → skip archive")

        continue

    # ======================
    # NEW JOB
    # ======================

    job = {
        "job_id": f"URL-{len(existing_jobs) + len(new_jobs) + 1:05d}",
        "title": title,
        "company": company,
        "location": loc,
        "description_text": desc,
        "source_url": url,
        "posted_date": posted,
        "collected_date": today,
    }

    new_jobs.append(job)
    jobs_by_url[url] = job
    archived_urls.add(url)

# ======================
# Persist data
# ======================

all_jobs = existing_jobs + new_jobs

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(all_jobs, f, indent=2, ensure_ascii=False)

CSV_FIELDS = [
    "job_id",
    "title",
    "company",
    "location",
    "description_text",
    "source_url",
    "posted_date",
    "collected_date",
]

if all_jobs:
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_jobs)

with open(ARCHIVE_JSON, "w", encoding="utf-8") as f:
    json.dump(sorted(archived_urls), f, indent=2)

# ======================
# FINAL CLEANUP (GUARANTEED)
# ======================

remaining = [u for u in URLS if u not in processed_urls]

with open(PATH_TO_URLS, "w", encoding="utf-8") as f:
    for u in remaining:
        f.write(u + "\n")

print("\n========== SUMMARY ==========")
print(f"Processed URLs: {len(processed_urls)}")
print(f"New jobs added: {len(new_jobs)}")
print(f"Remaining in queue: {len(remaining)}")
