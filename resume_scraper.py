#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, hashlib, time
from datetime import date
from concurrent.futures import ThreadPoolExecutor

import pytesseract
from pdf2image import convert_from_path
from PIL import Image

# ======================
# Settings
# ======================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "resumes")
OUTPUT_JSON = os.path.join(BASE_DIR, "scanned_resumes.json")
MANIFEST_JSON = os.path.join(BASE_DIR, "resumes_manifest.json")

TESSERACT_CONFIG = r"--oem 3 --psm 6"

# ======================
# Helpers
# ======================
def file_sha1(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> dict:
    try:
        with open(MANIFEST_JSON, "r", encoding="utf-8") as f:
            rows = json.load(f)
        if isinstance(rows, list):
            return {str(r.get("stored_filename", "")): r for r in rows if isinstance(r, dict)}
    except Exception:
        pass
    return {}

def scrub_contacts(text: str) -> str:
    if not text:
        return ""
    patterns = [
        r"\b[\w\.-]+@[\w\.-]+\.\w+\b",
        r"\b(?:\+?\d{1,3}[\s\-\.]?)?(?:\(?\d{3}\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4}\b",
        r"\bhttps?://\S+\b",
        r"\bwww\.\S+\b",
        r"\blinkedin\.com/\S+\b",
        r"\bgithub\.com/\S+\b",
    ]
    out = text
    for p in patterns:
        out = re.sub(p, " ", out, flags=re.I)
    out = re.sub(r"[ \t]+", " ", out)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out

def normalize_ocr_text(text: str) -> str:
    """
    Make OCR text easier to parse:
    - unify bullets
    - clean weird symbols
    - normalize newlines
    - keep line structure (important!)
    """
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\x0c", "\n")  # page breaks
    # common OCR bullet-ish symbols → bullet
    text = text.replace("¢", "•").replace("«", "•").replace("»", "•").replace("·", "•")
    # normalize multiple spaces
    text = re.sub(r"[ \t]+", " ", text)
    # trim each line
    text = "\n".join(line.strip() for line in text.splitlines())
    # collapse excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()

def is_contactish_line(line: str) -> bool:
    if not line:
        return False
    low = line.lower()
    if "linkedin" in low or "github" in low or "www." in low or "http" in low or "@" in low:
        return True
    if re.search(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", line):
        return True
    # often the name line is short letters/spaces
    if len(line) <= 40 and re.fullmatch(r"[A-Za-z .'-]+", line.strip()):
        return True
    return False

def drop_top_contact_block(lines: list[str]) -> list[str]:
    i = 0
    while i < len(lines) and is_contactish_line(lines[i]):
        i += 1
    return lines[i:]

def is_header_line(line: str, header_variants: list[str]) -> bool:
    """
    Robust header match:
    - allows extra spaces/punctuation
    - case-insensitive
    - matches whole line or line that is basically just the header
    """
    if not line:
        return False

    # remove punctuation except spaces
    norm = re.sub(r"[^A-Za-z\s]", " ", line)
    norm = re.sub(r"\s+", " ", norm).strip().lower()

    # headers are usually short
    if len(norm.split()) > 5:
        return False

    for hv in header_variants:
        hvn = hv.lower()
        if norm == hvn:
            return True
        # allow "technical skills" split as "technical" or "skills" is NOT enough → we keep full variants
    return False

def wrap_bullets(lines: list[str]) -> list[str]:
    """
    Join wrapped OCR lines so bullets become one logical bullet each.
    Rule:
    - If a line starts with '•' or '*' treat as new bullet
    - Otherwise it continues previous line (if previous exists)
    """
    out = []
    for line in lines:
        if not line:
            continue

        # treat section headers as standalone (don't merge into bullets)
        if line.isupper() and len(line.split()) <= 3:
            out.append(line)
            continue

        if line.startswith(("•", "*")):
            out.append(line)
        else:
            if out and not out[-1].isupper():
                out[-1] = out[-1].rstrip() + " " + line
            else:
                out.append(line)
    return out

# ======================
# OCR
# ======================
def ocr_pdf(path: str) -> str:
    images = convert_from_path(path, dpi=250)
    def ocr_one(img):
        gray = img.convert("L")
        return pytesseract.image_to_string(gray, config=TESSERACT_CONFIG)
    max_workers = max(1, min(4, len(images)))
    if max_workers == 1:
        pages = [ocr_one(img) for img in images]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            pages = list(ex.map(ocr_one, images))
    return "\n".join(pages)

# ======================
# Header-driven slicing parser
# ======================
HEADER_ALIASES = {
    "summary": ["professional summary", "summary", "profile", "objective"],
    "education": ["education"],
    "skills": ["technical skills", "skills"],
    "experience": ["experience", "work experience", "employment"],
    "projects": ["projects", "project experience", "selected projects"],
}

TARGET_SECTIONS = ["summary", "education", "skills", "experience", "projects"]

def find_header_positions(lines: list[str]) -> list[tuple[int, str]]:
    """
    Returns a sorted list of (index, section) where headers occur.
    """
    hits = []
    for idx, line in enumerate(lines):
        for sec, variants in HEADER_ALIASES.items():
            if is_header_line(line, variants):
                hits.append((idx, sec))
                break
    # If multiple same-section headers appear, keep the first occurrence
    seen = set()
    dedup = []
    for i, sec in sorted(hits, key=lambda x: x[0]):
        if sec not in seen:
            dedup.append((i, sec))
            seen.add(sec)
    return dedup

def slice_by_headers(lines: list[str]) -> dict:
    """
    Core idea:
    1) Find header anchors in the document.
    2) Slice content between anchors.
    3) Remove header lines themselves.
    4) If no SUMMARY header exists, treat the text before EDUCATION as summary.
    """
    sections = {k: [] for k in TARGET_SECTIONS}

    header_hits = find_header_positions(lines)

    # If we have NO headers at all, fallback: everything -> summary
    if not header_hits:
        sections["summary"] = lines[:]
        return sections

    # Build ranges
    # Example: (idx_EDU, 'education') to next header idx => education content
    for h_i, (start_idx, sec) in enumerate(header_hits):
        end_idx = header_hits[h_i + 1][0] if h_i + 1 < len(header_hits) else len(lines)
        # content after header line
        content = lines[start_idx + 1 : end_idx]
        sections[sec].extend(content)

    # If summary header was not found but education exists, take pre-education as summary
    found_secs = {sec for _, sec in header_hits}
    if "summary" not in found_secs:
        # find earliest header (often EDUCATION)
        first_idx, first_sec = header_hits[0]
        # if first header isn't projects/skills etc, still treat pre-first as summary
        pre = lines[:first_idx]
        if pre:
            sections["summary"].extend(pre)

    return sections

def post_clean_section_text(section_lines: list[str]) -> str:
    """
    Final cleanup:
    - scrub contacts
    - remove stray header words that got OCR'd inside content
    - normalize spacing
    """
    # drop empty + obvious leftovers
    cleaned = []
    for l in section_lines:
        if not l:
            continue
        # sometimes OCR repeats headers inside blocks
        if is_header_line(l, sum(HEADER_ALIASES.values(), [])):
            continue
        cleaned.append(l)

    # wrap bullets properly
    cleaned = wrap_bullets(cleaned)

    txt = "\n".join(cleaned).strip()
    txt = scrub_contacts(txt)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt

def split_sections(text: str) -> dict:
    text = normalize_ocr_text(text)
    lines = [l.strip() for l in text.splitlines()]

    # remove leading empties
    while lines and not lines[0]:
        lines.pop(0)

    # drop contact block at top
    lines = drop_top_contact_block(lines)

    # remove blank-only lines but keep single blank lines as paragraph separators
    # we'll keep blank lines by converting multiple blanks to one
    normalized_lines = []
    prev_blank = False
    for l in lines:
        if not l:
            if not prev_blank:
                normalized_lines.append("")
            prev_blank = True
        else:
            normalized_lines.append(l)
            prev_blank = False
    lines = normalized_lines

    raw_sections = slice_by_headers(lines)

    out = {}
    for sec in TARGET_SECTIONS:
        out[sec] = post_clean_section_text(raw_sections.get(sec, []))

    # Extra: if skills accidentally swallowed EXPERIENCE/PROJECTS due to missing headers,
    # we at least remove those header tokens and keep formatting clean (already done above).

    return out

# ======================
# Main
# ======================
def scan_file(path: str) -> dict:
    raw = ocr_pdf(path)
    parsed = split_sections(raw)

    resume_text = " ".join(
        parsed[k].replace("\n", " ")
        for k in TARGET_SECTIONS
        if parsed.get(k)
    )
    resume_text = re.sub(r"\s+", " ", resume_text).strip()

    return {
        "resume_id": "RES-" + file_sha1(path)[:12],
        "file_name": os.path.basename(path),
        "summary": parsed["summary"],
        "education": parsed["education"],
        "skills": parsed["skills"],
        "experience": parsed["experience"],
        "projects": parsed["projects"],
        "resume_text": resume_text,
        "collected_date": str(date.today()),
    }

def main():
    start_ts = time.perf_counter()
    results = []

    if not os.path.isdir(INPUT_DIR):
        print("Missing resumes folder:", INPUT_DIR)
        return

    for root, _, files in os.walk(INPUT_DIR):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                path = os.path.join(root, fn)
                print("OCR scanning:", path)
                file_start = time.perf_counter()
                try:
                    results.append(scan_file(path))
                    print(f"  done in {time.perf_counter() - file_start:.2f}s")
                except Exception as e:
                    print("  ERROR:", e)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(results)} resumes → {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
