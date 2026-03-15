import json
import os
import time
import requests
from datetime import datetime

# ================= CONFIG =================

COMPANY_FILE = "companies.json"

# Read-only archive (maintained by your OTHER program)
ARCHIVE_FILE = "job_url_archive.json"

# Output stream of newly discovered URLs for THIS run
OUTPUT_URL_FILE = "job_urls.txt"

MAX_NEW_URLS_PER_COMPANY = 250
PAGE_SIZE = 20
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 0.6

# ================= ARCHIVE (READ ONLY) =================

def load_archive_readonly():
    """
    Loads archive as a SET for fast membership checks.
    Supports archive formats:
      1) list of urls: ["https://...", ...]
      2) dict fallback: {"urls": ["https://...", ...]}
    If missing/invalid -> returns empty set.
    """
    if not os.path.exists(ARCHIVE_FILE):
        return set()

    try:
        with open(ARCHIVE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return set(data)

        if isinstance(data, dict):
            return set(data.get("urls", []))

        return set()

    except json.JSONDecodeError:
        print("[WARN] Archive file exists but is invalid JSON. Treating archive as empty.")
        return set()

# ================= OUTPUT =================

def append_new_urls(urls):
    if not urls:
        return
    with open(OUTPUT_URL_FILE, "a", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")

# ================= WORKDAY FETCH =================

def fetch_company_new_urls(company, archive_set):
    """
    Returns up to 100 NEW URLs for ONE company.
    Stops scanning that company immediately if an archived URL is encountered.
    Does NOT write to archive (read-only).
    """
    if (company["type"] == "workday"):
        host = company["host"].rstrip("/")          # e.g. https://walmart.wd5.myworkdayjobs.com
        tenant = company["name"]                    # e.g. walmart
        site = company["site"]                      # e.g. WalmartExternal
        api = f"{host}/wday/cxs/{tenant}/{site}/jobs"
    

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": host,
        "Referer": f"{host}/{site}",
    }

    offset = 0
    new_urls = []

    print(f"\n=== {company['name']} ===")

    while True:
        payload = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "searchText": "",
            "appliedFacets": {}
        }

        r = requests.post(api, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)

        if r.status_code != 200:
            print(f"[WARN] API returned {r.status_code} for {company['name']}")
            break

        data = r.json()
        jobs = data.get("jobPostings", [])

        if not jobs:
            print("No more jobs returned")
            break

        for job in jobs:
            path = job.get("externalPath")
            if not path:
                continue

            url = f"{host}/{site}{path}"

            # STOP CONDITION #1: archived URL hit → stop THIS company, move on
            if url in archive_set:
                print("Archived URL hit → stopping company scan")
                return new_urls

            # Collect as "new"
            new_urls.append(url)

            print(f"[NEW] {url}")

            # STOP CONDITION #2: per-company cap
            if len(new_urls) >= MAX_NEW_URLS_PER_COMPANY:
                print("Reached 100 new URLs → stopping company scan")
                return new_urls

        offset += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return new_urls

# ================= MAIN =================

def main():
    if not os.path.exists(COMPANY_FILE):
        raise SystemExit("companies.json not found")

    with open(COMPANY_FILE, "r", encoding="utf-8") as f:
        companies = json.load(f)

    archive_set = load_archive_readonly()

    all_new = []
    for company in companies:
        new_urls = fetch_company_new_urls(company, archive_set)
        append_new_urls(new_urls)   # write as we go
        all_new.extend(new_urls)
        time.sleep(1.0)

    print("\n=== DONE ===")
    print(f"New URLs written to {OUTPUT_URL_FILE}: {len(all_new)}")
    print(f"Archive (read-only) size used for checks: {len(archive_set)}")
    print(f"Last run: {datetime.now().isoformat(timespec='seconds')}")

if __name__ == "__main__":
    main()