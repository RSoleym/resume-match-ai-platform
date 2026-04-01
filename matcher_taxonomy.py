from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    import pycountry  # type: ignore
except Exception:  # pragma: no cover
    pycountry = None

try:
    import numpy as np  # type: ignore
except Exception:  # pragma: no cover
    np = None  # type: ignore

try:
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:  # pragma: no cover
    SentenceTransformer = None  # type: ignore

from semantic_role_classifier import classify_role_profile, role_profile_similarity
from shared_model_registry import get_sentence_transformer

DEGREE_LEVEL_ORDER = {
    "none": 0,
    "certificate": 1,
    "associate": 2,
    "bachelors": 3,
    "masters": 4,
    "phd": 5,
}

DEGREE_LEVEL_LABELS = {v: k for k, v in DEGREE_LEVEL_ORDER.items()}

COUNTRY_ALIASES = {
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

CITY_HINTS = {
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
    "dublin": "Ireland",
    "london": "United Kingdom",
    "cambridge": "United Kingdom",
    "amsterdam": "Netherlands",
    "eindhoven": "Netherlands",
    "munich": "Germany",
    "berlin": "Germany",
}

WORK_MODE_OPTIONS = ["Remote", "Hybrid", "On-site"]



COUNTRY_REGION = {
    # North America
    "Canada": "North America", "United States": "North America", "Mexico": "North America",
    # Europe
    "Ireland": "Europe", "United Kingdom": "Europe", "Netherlands": "Europe", "Germany": "Europe",
    "France": "Europe", "Spain": "Europe", "Portugal": "Europe", "Italy": "Europe", "Poland": "Europe",
    "Romania": "Europe", "Sweden": "Europe", "Norway": "Europe", "Finland": "Europe", "Denmark": "Europe",
    "Belgium": "Europe", "Austria": "Europe", "Switzerland": "Europe", "Czechia": "Europe",
    "Czech Republic": "Europe", "Hungary": "Europe", "Slovakia": "Europe", "Serbia": "Europe", "Greece": "Europe",
    "Luxembourg": "Europe",
    # Asia / Pacific
    "India": "Asia", "Japan": "Asia", "China": "Asia", "Taiwan": "Asia", "Singapore": "Asia",
    "Malaysia": "Asia", "Korea, Republic of": "Asia", "South Korea": "Asia", "Vietnam": "Asia",
    "Thailand": "Asia", "Indonesia": "Asia", "Philippines": "Asia", "Hong Kong": "Asia",
    # Middle East
    "Israel": "Middle East", "United Arab Emirates": "Middle East", "Saudi Arabia": "Middle East",
    "Qatar": "Middle East", "Jordan": "Middle East", "Turkey": "Middle East",
    # Oceania
    "Australia": "Oceania", "New Zealand": "Oceania",
    # South America
    "Brazil": "South America", "Argentina": "South America", "Chile": "South America", "Colombia": "South America",
    "Peru": "South America",
    # Africa
    "South Africa": "Africa", "Egypt": "Africa", "Morocco": "Africa", "Nigeria": "Africa", "Kenya": "Africa",
}

CATEGORY_PROTOTYPES: Dict[str, List[str]] = {
    "Hardware / RTL / Verification": [
        "rtl design verification systemverilog uvm asic soc fpga silicon cpu gpu pcie serdes timing closure physical design verification engineer",
        "digital hardware design for semiconductor chips including verilog, formal verification, dv, emulation and post silicon validation",
    ],
    "Embedded / Firmware": [
        "embedded firmware development for microcontrollers and boards using c c++, rtos, bare metal, spi, i2c, uart and bring-up",
        "device driver, bootloader and low level embedded software for hardware platforms and peripherals",
    ],
    "Software Engineering": [
        "software engineering backend frontend full stack web api python java c++ react node flask django mobile ios android",
        "building software applications, services, platforms and developer tooling",
    ],
    "Data / AI / ML": [
        "machine learning data engineering data science artificial intelligence nlp computer vision analytics mlops llm neural network",
        "training models, data pipelines, feature engineering, inference systems and business intelligence",
    ],
    "IT / Cloud / DevOps / Security": [
        "devops sre cloud aws azure gcp kubernetes docker infrastructure linux cybersecurity site reliability security engineer",
        "managing cloud platforms, infrastructure automation, monitoring, deployment, networking and security",
    ],
    "Electrical / Power / Controls": [
        "electrical engineer power systems controls plc scada high voltage protection substation motor control renewable solar grid",
        "electrical design, controls automation, power electronics and industrial systems",
    ],
    "Mechanical / Manufacturing": [
        "mechanical engineering manufacturing cad solidworks thermodynamics hvac tooling product design reliability process engineering",
        "mechanical systems, manufacturing process, tooling, product development and test",
    ],
    "Civil / Construction": [
        "civil engineering structural geotechnical transportation municipal construction revit site engineer",
        "construction, infrastructure, buildings, municipal works and structural design",
    ],
    "Biomedical / Life Sciences / Chemistry": [
        "biomedical engineering biology chemistry biotech pharmaceutical clinical laboratory medical device life sciences",
        "medical device development, biotech research, chemistry labs and life science work",
    ],
    "Finance / Accounting": [
        "finance accounting accounts payable accounts receivable audit tax controller bookkeeping quickbooks fp&a financial analyst",
        "financial reporting, accounting operations, audit, reconciliation and budgeting",
    ],
    "Business / Operations / Supply Chain": [
        "operations supply chain procurement logistics program manager project manager product manager business analyst planner",
        "business operations, supply chain planning, procurement, logistics and project execution",
    ],
    "Sales / Marketing / Customer Support": [
        "sales marketing customer success account executive business development recruiter human resources support specialist social media",
        "go to market, customer relationships, campaigns, recruiting and support",
    ],
    "Retail / Warehouse / Logistics": [
        "retail cashier store associate warehouse fulfillment picker packer forklift merchandising distribution center",
        "retail stores, warehouses, fulfillment centers and logistics operations",
    ],
    "Design / UX / Creative": [
        "ux ui graphic design product designer figma adobe illustrator animation creative",
        "user experience, interface design, graphic design and creative production",
    ],
    "Research / Academic": [
        "research assistant research engineer phd postdoctoral professor teaching assistant publication journal academic",
        "scientific research, academic work, lab work and publications",
    ],
}

_CATEGORY_MODEL = None
_CATEGORY_MODEL_EMB = None
_CATEGORY_TEXT_ORDER: List[str] = []
_CATEGORY_SEMANTIC_ENABLED = (os.environ.get("ROLEMATCHER_ENABLE_CATEGORY_ST", "1").strip().lower() not in {"0", "false", "no", "off"})

CATEGORY_RELATED_SCORES: Dict[str, Dict[str, float]] = {
    "Hardware / RTL / Verification": {"Embedded / Firmware": 0.84, "Electrical / Power / Controls": 0.68, "Software Engineering": 0.48, "Data / AI / ML": 0.34, "Research / Academic": 0.52},
    "Embedded / Firmware": {"Hardware / RTL / Verification": 0.84, "Software Engineering": 0.76, "Electrical / Power / Controls": 0.62, "Data / AI / ML": 0.42},
    "Software Engineering": {"Embedded / Firmware": 0.76, "Data / AI / ML": 0.66, "IT / Cloud / DevOps / Security": 0.72, "Hardware / RTL / Verification": 0.48, "Research / Academic": 0.42},
    "Data / AI / ML": {"Software Engineering": 0.66, "IT / Cloud / DevOps / Security": 0.58, "Hardware / RTL / Verification": 0.45, "Research / Academic": 0.62},
    "IT / Cloud / DevOps / Security": {"Software Engineering": 0.72, "Data / AI / ML": 0.58},
    "Electrical / Power / Controls": {"Hardware / RTL / Verification": 0.68, "Embedded / Firmware": 0.62, "Mechanical / Manufacturing": 0.54},
    "Mechanical / Manufacturing": {"Electrical / Power / Controls": 0.54, "Civil / Construction": 0.45},
    "Civil / Construction": {"Mechanical / Manufacturing": 0.45},
    "Biomedical / Life Sciences / Chemistry": {"Research / Academic": 0.58},
    "Finance / Accounting": {"Business / Operations / Supply Chain": 0.66},
    "Business / Operations / Supply Chain": {"Finance / Accounting": 0.66, "Sales / Marketing / Customer Support": 0.45, "Retail / Warehouse / Logistics": 0.54},
    "Sales / Marketing / Customer Support": {"Business / Operations / Supply Chain": 0.45, "Retail / Warehouse / Logistics": 0.35},
    "Retail / Warehouse / Logistics": {"Business / Operations / Supply Chain": 0.54, "Sales / Marketing / Customer Support": 0.35},
    "Design / UX / Creative": {"Software Engineering": 0.34},
    "Research / Academic": {"Data / AI / ML": 0.62, "Biomedical / Life Sciences / Chemistry": 0.58, "Hardware / RTL / Verification": 0.52},
}


def canonicalize_work_mode(value: str) -> str:
    low = norm_space(value).lower().replace("—", "-").replace("–", "-")
    if not low:
        return "On-site"
    if "hybrid" in low or "partially remote" in low or "partially on-site" in low:
        return "Hybrid"
    if any(tok in low for tok in ["remote", "work from home", "wfh", "virtual", "telecommut", "home-based", "distributed", "anywhere"]):
        return "Remote"
    if any(tok in low for tok in ["on-site", "onsite", "on site", "in-office", "in office", "office-based", "office based"]):
        return "On-site"
    return "On-site"

CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "Hardware / RTL / Verification": [
        "rtl", "systemverilog", "verilog", "uvm", "asic", "soc", "vlsi", "dft", "formal verification",
        "physical design", "timing closure", "fpga", "validation engineer", "design verification", "gpu", "cpu",
        "silicon", "semiconductor", "pcie", "serdes", "emulation", "post-silicon",
    ],
    "Embedded / Firmware": [
        "firmware", "embedded", "microcontroller", "rtos", "bare metal", "device driver", "bootloader",
        "cortex-m", "spi", "i2c", "uart", "can bus", "board bring-up", "bring-up",
    ],
    "Software Engineering": [
        "software engineer", "backend", "frontend", "full stack", "full-stack", "web development", "api",
        "python developer", "java developer", "c++ developer", "react", "node", "django", "flask",
        "mobile app", "ios", "android", "swe",
    ],
    "Data / AI / ML": [
        "machine learning", "deep learning", "artificial intelligence", "data scientist", "data engineer",
        "nlp", "computer vision", "analytics", "business intelligence", "mlops", "llm", "neural network",
        "dataflow", "spark", "hadoop", "pandas", "sql analytics",
    ],
    "IT / Cloud / DevOps / Security": [
        "devops", "sre", "cloud", "aws", "azure", "gcp", "kubernetes", "docker", "linux admin",
        "infrastructure", "cybersecurity", "security engineer", "iam", "network security", "site reliability",
    ],
    "Electrical / Power / Controls": [
        "electrical engineer", "power systems", "controls", "scada", "plc", "substation", "protection",
        "high voltage", "motor control", "power electronics", "renewable", "solar", "grid", "automation",
    ],
    "Mechanical / Manufacturing": [
        "mechanical engineer", "cad", "solidworks", "manufacturing", "process engineer", "thermodynamics",
        "hvac", "tooling", "product design", "packaging design", "reliability engineer", "aerospace",
    ],
    "Civil / Construction": [
        "civil engineer", "structural", "construction", "site engineer", "autocad civil", "geotechnical",
        "transportation engineer", "municipal", "building systems", "revit",
    ],
    "Biomedical / Life Sciences / Chemistry": [
        "biomedical", "biotech", "biology", "chemist", "chemistry", "pharmaceutical", "laboratory",
        "clinical", "medical device", "life science", "bioinformatics",
    ],
    "Finance / Accounting": [
        "accounting", "finance", "ap/ar", "accounts payable", "accounts receivable", "financial analyst",
        "tax", "audit", "controller", "bookkeeper", "quickbooks", "fp&a",
    ],
    "Business / Operations / Supply Chain": [
        "operations", "supply chain", "procurement", "buyer", "logistics analyst", "program manager",
        "project manager", "product manager", "business analyst", "operations manager", "planner",
    ],
    "Sales / Marketing / Customer Support": [
        "sales", "marketing", "customer success", "account executive", "business development", "recruiter",
        "human resources", "hr", "support specialist", "customer support", "social media",
    ],
    "Retail / Warehouse / Logistics": [
        "retail", "cashier", "store associate", "warehouse", "fulfillment", "picker", "packer", "forklift",
        "merchandising", "walmart", "target", "amazon warehouse",
    ],
    "Design / UX / Creative": [
        "ux", "ui", "graphic design", "designer", "figma", "adobe", "illustrator", "creative", "animation",
    ],
    "Research / Academic": [
        "research assistant", "research engineer", "phd candidate", "postdoctoral", "professor", "teaching assistant",
        "publication", "journal", "academic",
    ],
}

CATEGORY_FAMILY = {
    "Hardware / RTL / Verification": "Engineering / Computing",
    "Embedded / Firmware": "Engineering / Computing",
    "Software Engineering": "Engineering / Computing",
    "Data / AI / ML": "Engineering / Computing",
    "IT / Cloud / DevOps / Security": "Engineering / Computing",
    "Electrical / Power / Controls": "Engineering",
    "Mechanical / Manufacturing": "Engineering",
    "Civil / Construction": "Engineering",
    "Biomedical / Life Sciences / Chemistry": "Science / Biomedical",
    "Finance / Accounting": "Business",
    "Business / Operations / Supply Chain": "Business",
    "Sales / Marketing / Customer Support": "Business",
    "Retail / Warehouse / Logistics": "Operations",
    "Design / UX / Creative": "Creative",
    "Research / Academic": "Research",
    "General": "General",
}

DEGREE_FIELD_KEYWORDS: Dict[str, Dict[str, Any]] = {
    "Electrical Engineering": {"family": "ECE", "aliases": ["electrical engineering", "electronics engineering", "electronics and communication", "ece", "electrical"]},
    "Computer Engineering": {"family": "ECE", "aliases": ["computer engineering", "computer engineer", "computer hardware", "computer architecture"]},
    "Software Engineering": {"family": "ECE", "aliases": ["software engineering", "software engineer"]},
    "Computer Science": {"family": "ECE", "aliases": ["computer science", "cs degree", "computing science", "informatics"]},
    "Biomedical Engineering": {"family": "ECE", "aliases": ["biomedical engineering", "bioengineering", "medical device engineering"]},
    "Mechanical Engineering": {"family": "ECE", "aliases": ["mechanical engineering", "aerospace engineering", "industrial engineering", "mechatronics", "robotics"]},
    "Civil Engineering": {"family": "Civil / Construction", "aliases": ["civil engineering", "structural engineering", "construction engineering"]},
    "Chemical / Life Sciences": {"family": "Life Sciences", "aliases": ["chemical engineering", "chemistry", "biochemistry", "biology", "biotechnology", "life sciences", "pharmaceutical sciences"]},
    "Math / Physics / Statistics": {"family": "ECE", "aliases": ["physics", "mathematics", "math", "statistics", "applied math"]},
    "Finance / Accounting": {"family": "Finance / Accounting", "aliases": ["finance", "accounting", "commerce", "economics", "actuarial"]},
    "Business / Operations": {"family": "Business", "aliases": ["business administration", "operations management", "supply chain", "supply chain management", "business analytics", "mba"]},
    "Design / UX": {"family": "Design", "aliases": ["graphic design", "interaction design", "ux design", "user experience", "human computer interaction", "hci"]},
}

RELATED_DEGREE_FAMILIES = {
    "ECE": {"ECE", "Life Sciences"},
    "Life Sciences": {"Life Sciences", "ECE"},
    "Civil / Construction": {"Civil / Construction", "ECE"},
    "Finance / Accounting": {"Finance / Accounting", "Business"},
    "Business": {"Business", "Finance / Accounting"},
    "Design": {"Design"},
    "General": {"General", "ECE", "Business", "Finance / Accounting", "Life Sciences", "Civil / Construction", "Design"},
}

_EXPERIENCE_PATTERNS = [
    r"(?P<num>\d+(?:\.\d+)?)\s*(?:\+|plus)?\s*(?:years|year|yrs|yr)\s*(?:of\s*)?experience",
    r"(?:minimum\s+of|at\s+least|min\.?|minimum)\s*(?P<num>\d+(?:\.\d+)?)\s*(?:years|year|yrs|yr)",
    r"(?P<num>\d+(?:\.\d+)?)\s*(?:\+|plus)\s*(?:years|year|yrs|yr)",
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
WORK_KEYWORDS = {"engineer", "developer", "intern", "co-op", "coop", "analyst", "specialist", "designer", "manager", "consultant", "scientist", "technician", "assistant"}


def norm_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_country_name(value: str) -> str:
    text = norm_space(value)
    if not text:
        return ""
    low = text.lower()
    if low in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[low]
    if pycountry is not None:
        try:
            match = pycountry.countries.lookup(text)
            return match.name
        except Exception:
            pass
    return text


def _country_patterns() -> List[Tuple[str, re.Pattern[str]]]:
    names: List[str] = []
    if pycountry is not None:
        try:
            names.extend([c.name for c in pycountry.countries])
        except Exception:
            pass
    names.extend(["Taiwan", "Remote"])
    out: List[Tuple[str, re.Pattern[str]]] = []
    seen: Set[str] = set()
    for name in names:
        if not name or name in seen or name == "Remote":
            continue
        seen.add(name)
        out.append((name, re.compile(rf"(?i)(^|[\s,(/-]){re.escape(name)}($|[\s,)/-])")))
    return out


_COUNTRY_PATTERNS = _country_patterns()


def infer_country(text: str) -> str:
    s = norm_space(text)
    if not s:
        return ""
    low = s.lower()
    for alias, full in COUNTRY_ALIASES.items():
        if re.search(rf"(?i)(^|[\s,(/-]){re.escape(alias)}($|[\s,)/-])", s):
            return full
    for name, pat in _COUNTRY_PATTERNS:
        if pat.search(s):
            return name
    for city, country in CITY_HINTS.items():
        if city in low:
            return country
    parts = [part.strip() for part in re.split(r"[,/|()\-]+", s) if part.strip()]
    for part in reversed(parts):
        p = part.upper()
        if len(p) == 2 and p in REGION_CODES:
            return REGION_CODES[p]
        if pycountry is not None:
            try:
                if len(p) == 2:
                    c = pycountry.countries.get(alpha_2=p)
                    if c:
                        return c.name
                if len(p) == 3:
                    c = pycountry.countries.get(alpha_3=p)
                    if c:
                        return c.name
            except Exception:
                pass
    if "remote" in low:
        return "Remote"
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
        r"\bin[- ]office\b",
        r"\bin office\b",
        r"\bon campus\b",
    ]
    if any(re.search(p, text) for p in hybrid_patterns):
        return "Hybrid"
    if any(re.search(p, text) for p in remote_patterns):
        return "Remote"
    if any(re.search(p, text) for p in onsite_patterns):
        return "On-site"
    return canonicalize_work_mode(text)


def extract_required_years(text: str) -> Optional[float]:
    s = norm_space(text)
    if not s:
        return None
    vals: List[float] = []
    for pat in _EXPERIENCE_PATTERNS:
        for m in re.finditer(pat, s, flags=re.IGNORECASE):
            try:
                vals.append(float(m.group("num")))
            except Exception:
                pass
    return max(vals) if vals else None


def _parse_month_year(s: str) -> Optional[date]:
    t = norm_space(s).lower().rstrip(",.)")
    if not t:
        return None
    if t in {"present", "current", "now", "today"}:
        today = date.today()
        return date(today.year, today.month, 1)
    m = re.match(r"^(?P<m>\d{1,2})\s*[-/]\s*(?P<y>\d{4})$", t)
    if m:
        return date(int(m.group("y")), max(1, min(12, int(m.group("m")))), 1)
    m = re.match(r"^(?P<y>\d{4})\s*[-/]\s*(?P<m>\d{1,2})$", t)
    if m:
        return date(int(m.group("y")), max(1, min(12, int(m.group("m")))), 1)
    m = re.match(r"^(?P<mon>[a-z]{3,9})\.?\s+(?P<y>\d{4})$", t)
    if m:
        mm = MONTHS.get(m.group("mon")[:3])
        if mm:
            return date(int(m.group("y")), mm, 1)
    m = re.match(r"^(?P<season>spring|summer|fall|autumn|winter)\s+(?P<y>\d{4})$", t)
    if m:
        mm = SEASONS.get(m.group("season"))
        if mm:
            return date(int(m.group("y")), mm, 1)
    m = re.match(r"^(?P<y>\d{4})$", t)
    if m:
        return date(int(m.group("y")), 1, 1)
    return None


def _months_between(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + (b.month - a.month)


def _extract_date_ranges_from_text(text: str) -> List[Tuple[date, date, int, int]]:
    if not text:
        return []
    t = text.replace("–", "-").replace("—", "-")
    token = r"(?:[A-Za-z]{3,9}\.?\s+\d{4}|\d{1,2}[-/]\d{4}|\d{4}[-/]\d{1,2}|\d{4}|present|current|spring\s+\d{4}|summer\s+\d{4}|fall\s+\d{4}|autumn\s+\d{4}|winter\s+\d{4})"
    pattern = re.compile(rf"(?P<start>{token})\s*(?:-|to)\s*(?P<end>{token})", flags=re.IGNORECASE)
    out: List[Tuple[date, date, int, int]] = []
    for m in pattern.finditer(t):
        s = _parse_month_year(m.group("start"))
        e = _parse_month_year(m.group("end"))
        if s and e:
            if e < s:
                s, e = e, s
            out.append((s, e, m.start(), m.end()))
    return out


def _merge_intervals(intervals: Sequence[Tuple[date, date]]) -> List[Tuple[date, date]]:
    if not intervals:
        return []
    rows = sorted(intervals, key=lambda x: (x[0].year, x[0].month, x[1].year, x[1].month))
    merged = [rows[0]]
    for s, e in rows[1:]:
        ls, le = merged[-1]
        if _months_between(le, s) <= 1:
            merged[-1] = (ls, max(le, e))
        else:
            merged.append((s, e))
    return merged


def _extract_years_literal(text: str) -> Optional[float]:
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:\+|plus)?\s*(?:years|year|yrs|yr)\b", text or "", flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def derive_resume_years_experience(experience_text: str, resume_text: str = "") -> Optional[float]:
    combined = "\n".join([experience_text or "", resume_text or ""]).strip()
    if not combined:
        return None
    ranges = _extract_date_ranges_from_text(combined)
    if ranges:
        intervals = _merge_intervals([(s, e) for s, e, _, _ in ranges])
        total_months = sum(max(0, _months_between(s, e)) for s, e in intervals)
        years = round(total_months / 12.0, 2)
        if years > 0:
            return years
    return _extract_years_literal(combined)



def _normalize_score_map(raw_scores: Dict[str, float]) -> Dict[str, float]:
    if not raw_scores:
        return {}
    cleaned = {k: max(0.0, float(v)) for k, v in raw_scores.items()}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {k: round(v / total, 6) for k, v in cleaned.items()}


def _softmax(values: Sequence[float], temp: float = 0.22) -> List[float]:
    if not values:
        return []
    if temp <= 0:
        temp = 0.22
    vmax = max(values)
    exp_vals = [math.exp((float(v) - vmax) / temp) for v in values]
    total = sum(exp_vals) or 1.0
    return [v / total for v in exp_vals]


def _get_category_semantic_state() -> Tuple[Any, Optional[Dict[str, Any]]]:
    global _CATEGORY_MODEL, _CATEGORY_MODEL_EMB, _CATEGORY_TEXT_ORDER
    if not _CATEGORY_SEMANTIC_ENABLED or SentenceTransformer is None or np is None:
        return None, None
    if _CATEGORY_MODEL is not None and _CATEGORY_MODEL_EMB is not None:
        return _CATEGORY_MODEL, _CATEGORY_MODEL_EMB
    try:
        _CATEGORY_MODEL = get_sentence_transformer("all-MiniLM-L6-v2")
        _CATEGORY_TEXT_ORDER = list(CATEGORY_PROTOTYPES.keys())
        cat_texts = [" ".join(CATEGORY_PROTOTYPES.get(cat, []) + CATEGORY_KEYWORDS.get(cat, [])) for cat in _CATEGORY_TEXT_ORDER]
        emb = _CATEGORY_MODEL.encode(cat_texts, normalize_embeddings=True, show_progress_bar=False)
        _CATEGORY_MODEL_EMB = {cat: emb[i] for i, cat in enumerate(_CATEGORY_TEXT_ORDER)}
        return _CATEGORY_MODEL, _CATEGORY_MODEL_EMB
    except Exception:
        _CATEGORY_MODEL = None
        _CATEGORY_MODEL_EMB = None
        return None, None


def _semantic_category_scores(text: str) -> Dict[str, float]:
    model, emb_map = _get_category_semantic_state()
    if model is None or emb_map is None or not norm_space(text):
        return {}
    try:
        vec = model.encode([norm_space(text)[:4000]], normalize_embeddings=True, show_progress_bar=False)[0]
        cats = list(emb_map.keys())
        sims = [float(np.dot(vec, emb_map[cat])) for cat in cats]
        weights = _softmax(sims, temp=0.16)
        return {cat: round(max(0.0, w), 6) for cat, w in zip(cats, weights)}
    except Exception:
        return {}


def _score_categories(text: str) -> Dict[str, float]:
    low = norm_space(text).lower()
    if not low:
        return {}
    title_part = low.split(" ", 40)[:20]
    title_bias_text = " ".join(title_part)
    scores: Dict[str, float] = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        score = 0.0
        for kw in keywords:
            kl = kw.lower()
            hits = low.count(kl)
            if not hits:
                continue
            phrase_bonus = 1.55 if len(kl.split()) >= 2 else 1.0
            title_bonus = 1.25 if kl in title_bias_text else 1.0
            score += hits * phrase_bonus * title_bonus
            if hits > 1:
                score += 0.2 * (hits - 1)
        if score:
            scores[cat] = round(score, 4)
    return scores



def classify_category(title: str = "", description: str = "", *, fallback: str = "General") -> Dict[str, Any]:
    title_clean = norm_space(title)
    desc_clean = norm_space(description)
    weighted_text = " ".join([
        title_clean,
        title_clean,
        title_clean,
        desc_clean[:5000],
    ]).strip()
    lexical_norm = _normalize_score_map(_score_categories(weighted_text))
    semantic_norm = _semantic_category_scores(weighted_text)

    all_cats = list(dict.fromkeys(list(CATEGORY_KEYWORDS.keys()) + list(lexical_norm.keys()) + list(semantic_norm.keys())))
    combined: Dict[str, float] = {}
    for cat in all_cats:
        lex = lexical_norm.get(cat, 0.0)
        sem = semantic_norm.get(cat, 0.0)
        combo = (0.62 * lex) + (0.38 * sem)
        if title_clean:
            title_low = title_clean.lower()
            if any(kw.lower() in title_low for kw in CATEGORY_KEYWORDS.get(cat, [])):
                combo += 0.08
        if combo > 0:
            combined[cat] = combo

    if not combined:
        return {
            "category": fallback,
            "category_family": CATEGORY_FAMILY.get(fallback, "General"),
            "category_scores": {},
            "category_confidence": 0.0,
            "category_runner_up": "",
            "category_method": "fallback",
        }

    norm_combined = _normalize_score_map(combined)
    ranked = sorted(norm_combined.items(), key=lambda x: (-x[1], x[0]))
    top_cat, top_score = ranked[0]
    runner_up_cat, runner_up_score = ranked[1] if len(ranked) > 1 else ("", 0.0)
    margin = max(0.0, float(top_score) - float(runner_up_score))
    conf = round(min(0.995, (0.78 * float(top_score)) + (0.22 * margin)), 3)
    return {
        "category": top_cat,
        "category_family": CATEGORY_FAMILY.get(top_cat, "General"),
        "category_scores": {k: round(v, 4) for k, v in ranked[:6]},
        "category_confidence": conf,
        "category_runner_up": runner_up_cat,
        "category_method": "lexical+semantic" if semantic_norm else "lexical",
    }


def _choose_level(levels: Iterable[str], *, highest: bool) -> str:
    picked = [lvl for lvl in levels if lvl in DEGREE_LEVEL_ORDER]
    if not picked:
        return "none"
    ranked = sorted(picked, key=lambda lvl: DEGREE_LEVEL_ORDER[lvl], reverse=highest)
    return ranked[0]


def _detect_degree_levels(text: str) -> List[str]:
    low = norm_space(text).lower()
    found: List[str] = []
    level_patterns = {
        "phd": [r"\bph\.?d\b", r"doctorate", r"doctoral"],
        "masters": [r"master'?s", r"master of", r"\bm\.?s\b", r"\bm\.?eng\b", r"mba", r"graduate degree"],
        "bachelors": [r"bachelor'?s", r"bachelor of", r"\bb\.?s\b", r"\bb\.?eng\b", r"undergraduate degree", r"college degree"],
        "associate": [r"associate degree", r"community college"],
        "certificate": [r"certificate", r"diploma"],
    }
    for lvl, patterns in level_patterns.items():
        if any(re.search(p, low) for p in patterns):
            found.append(lvl)
    return found


def _extract_degree_fields(text: str) -> Tuple[List[str], str]:
    low = norm_space(text).lower()
    found_fields: List[str] = []
    families: List[str] = []
    for label, meta in DEGREE_FIELD_KEYWORDS.items():
        aliases = meta.get("aliases", [])
        matched = False
        for alias in aliases:
            pattern = rf"(?<![a-z]){re.escape(alias.lower())}(?![a-z])"
            if re.search(pattern, low):
                matched = True
                break
        if matched:
            found_fields.append(label)
            families.append(str(meta.get("family") or "General"))
    if not found_fields:
        return [], "General"
    counter = Counter(families)
    family = counter.most_common(1)[0][0]
    return sorted(set(found_fields)), family


def infer_degree_family_from_category(category: str) -> str:
    mapping = {
        "Hardware / RTL / Verification": "ECE",
        "Embedded / Firmware": "ECE",
        "Software Engineering": "ECE",
        "Data / AI / ML": "ECE",
        "IT / Cloud / DevOps / Security": "ECE",
        "Electrical / Power / Controls": "ECE",
        "Mechanical / Manufacturing": "ECE",
        "Civil / Construction": "Civil / Construction",
        "Biomedical / Life Sciences / Chemistry": "Life Sciences",
        "Finance / Accounting": "Finance / Accounting",
        "Business / Operations / Supply Chain": "Business",
        "Sales / Marketing / Customer Support": "Business",
        "Retail / Warehouse / Logistics": "General",
        "Design / UX / Creative": "Design",
        "Research / Academic": "ECE",
    }
    return mapping.get(category, "General")


def extract_degree_requirement(text: str, *, fallback_category: str = "General") -> Dict[str, Any]:
    body = norm_space(text)
    parts = []
    for sentence in re.split(r"[\n\.;]+", body):
        low_sentence = sentence.lower()
        if any(tok in low_sentence for tok in ["degree", "bachelor", "master", "phd", "ph.d", "doctorate", "related field", "relevant field", "discipline"]):
            parts.append(sentence)
    context = norm_space(" ".join(parts)) or body
    levels = _detect_degree_levels(context)
    fields, family = _extract_degree_fields(context)
    low = context.lower()
    if ("related field" in low or "relevant field" in low or "related discipline" in low) and family == "General":
        family = infer_degree_family_from_category(fallback_category)
    if not levels and ("degree" not in low and "bachelor" not in low and "master" not in low and "phd" not in low):
        level = "none"
    else:
        level = _choose_level(levels, highest=False)
    return {
        "degree_level_min": level,
        "degree_family": family,
        "degree_fields": fields,
    }


def infer_resume_degree(education_text: str, resume_text: str = "", *, fallback_category: str = "General") -> Dict[str, Any]:
    body = norm_space(" ".join([education_text or "", resume_text or ""]))
    levels = _detect_degree_levels(body)
    fields, family = _extract_degree_fields(body)
    if family == "General" and ("related field" in body.lower() or "relevant field" in body.lower()):
        family = infer_degree_family_from_category(fallback_category)
    level = _choose_level(levels, highest=True)
    return {
        "degree_level": level,
        "degree_family": family,
        "degree_fields": fields,
    }



def category_similarity(a: str, b: str) -> float:
    aa = a or "General"
    bb = b or "General"
    if aa == bb:
        return 1.0
    direct = CATEGORY_RELATED_SCORES.get(aa, {}).get(bb)
    if direct is not None:
        return round(float(direct), 4)
    reverse = CATEGORY_RELATED_SCORES.get(bb, {}).get(aa)
    if reverse is not None:
        return round(float(reverse), 4)
    fa = CATEGORY_FAMILY.get(aa, "General")
    fb = CATEGORY_FAMILY.get(bb, "General")
    if fa == fb and fa != "General":
        return 0.55
    if fa == "Engineering / Computing" and fb == "Engineering":
        return 0.62
    if fb == "Engineering / Computing" and fa == "Engineering":
        return 0.62
    if fa == "Business" and fb in {"Operations", "General"}:
        return 0.38
    if fb == "Business" and fa in {"Operations", "General"}:
        return 0.38
    if fa == "General" or fb == "General":
        return 0.42
    return 0.12


def degree_fit_score(candidate_level: str, candidate_family: str, job_level: str, job_family: str) -> float:
    if (job_level or "none") == "none" and (job_family or "General") == "General":
        return 1.0
    cand_level_num = DEGREE_LEVEL_ORDER.get(candidate_level or "none", 0)
    job_level_num = DEGREE_LEVEL_ORDER.get(job_level or "none", 0)
    if job_level_num <= 0:
        level_score = 1.0
    elif cand_level_num >= job_level_num:
        level_score = 1.0
    elif cand_level_num == 0:
        level_score = 0.2
    else:
        gap = job_level_num - cand_level_num
        level_score = max(0.2, 1.0 - 0.22 * gap)

    cf = candidate_family or "General"
    jf = job_family or "General"
    if jf == "General":
        family_score = 1.0
    elif cf == jf:
        family_score = 1.0
    elif jf in RELATED_DEGREE_FAMILIES.get(cf, set()) or cf in RELATED_DEGREE_FAMILIES.get(jf, set()):
        family_score = 0.82
    elif cf == "General":
        family_score = 0.45
    else:
        family_score = 0.2
    return round((0.7 * family_score) + (0.3 * level_score), 4)



def experience_fit_score(candidate_years: Optional[float], job_years: Optional[float]) -> float:
    if job_years is None:
        return 1.0
    if candidate_years is None:
        return 0.48
    if candidate_years >= job_years:
        return 1.0
    gap = max(0.0, float(job_years) - float(candidate_years))
    if gap <= 0.5:
        return 0.9
    if gap <= 1.0:
        return 0.8
    if gap <= 2.0:
        return 0.62
    if gap <= 3.0:
        return 0.45
    if gap <= 5.0:
        return 0.28
    return 0.14


def enrich_job_record(job: Dict[str, Any]) -> Dict[str, Any]:
    j = dict(job)
    title = str(j.get("title") or "")
    location = str(j.get("location") or "")
    desc = str(j.get("description_text") or j.get("description") or "")

    role_profile = classify_role_profile(title, desc)
    legacy_category_info = classify_category(title, desc, fallback=role_profile["legacy_category"])
    degree_info = extract_degree_requirement(f"{title}\n{desc}", fallback_category=role_profile["legacy_category"])

    experience_needed_years = j.get("experience_needed_years")
    if experience_needed_years in (None, ""):
        experience_needed_years = extract_required_years(desc)
    else:
        try:
            experience_needed_years = float(experience_needed_years)
        except Exception:
            experience_needed_years = extract_required_years(desc)

    degree_fields = j.get("degree_fields")
    if isinstance(degree_fields, str):
        try:
            loaded = json.loads(degree_fields)
            if isinstance(loaded, list):
                degree_fields = loaded
        except Exception:
            degree_fields = [x.strip() for x in degree_fields.split("|") if x.strip()]
    if not isinstance(degree_fields, list) or not degree_fields:
        degree_fields = degree_info["degree_fields"]

    j["country"] = str(j.get("country") or infer_country(location))
    j["work_mode"] = canonicalize_work_mode(str(j.get("work_mode") or infer_work_mode(title, location, desc)))
    j["experience_needed_years"] = experience_needed_years
    j["degree_level_min"] = str(j.get("degree_level_min") or degree_info["degree_level_min"])
    j["degree_family"] = str(j.get("degree_family") or degree_info["degree_family"])
    j["degree_fields"] = degree_fields

    j["job_function"] = str(j.get("job_function") or role_profile["primary_function"])
    j["job_function_confidence"] = float(j.get("job_function_confidence") or role_profile["primary_function_confidence"] or 0.0)
    j["job_function_runner_up"] = str(j.get("job_function_runner_up") or role_profile.get("primary_function_runner_up") or "")
    j["job_function_scores"] = j.get("job_function_scores") or role_profile.get("primary_function_scores") or {}
    j["job_domain"] = str(j.get("job_domain") or role_profile["primary_domain"])
    j["job_domain_confidence"] = float(j.get("job_domain_confidence") or role_profile["primary_domain_confidence"] or 0.0)
    j["job_domain_runner_up"] = str(j.get("job_domain_runner_up") or role_profile.get("primary_domain_runner_up") or "")
    j["job_domain_scores"] = j.get("job_domain_scores") or role_profile.get("primary_domain_scores") or {}
    j["job_category_key"] = str(j.get("job_category_key") or role_profile["category_key"])
    j["job_category_key_confidence"] = float(j.get("job_category_key_confidence") or role_profile.get("category_key_confidence") or 0.0)

    j["job_category"] = str(j.get("job_category") or role_profile["legacy_category"] or legacy_category_info["category"])
    j["job_category_family"] = str(j.get("job_category_family") or CATEGORY_FAMILY.get(j["job_category"], "General"))
    j["job_category_confidence"] = float(j.get("job_category_confidence") or legacy_category_info.get("category_confidence") or 0.0)
    return j


def infer_resume_country(raw_text: str, parsed_sections: Optional[Dict[str, str]] = None) -> str:
    raw = raw_text or ""
    sections = parsed_sections or {}

    top_lines = [ln.strip() for ln in raw.splitlines()[:18] if ln.strip()]
    prioritized_chunks: List[str] = []
    if top_lines:
        prioritized_chunks.append("\n".join(top_lines[:8]))
        prioritized_chunks.extend(top_lines[:18])

    for key in ["summary", "education", "experience", "projects"]:
        value = norm_space(sections.get(key) or "")
        if value:
            prioritized_chunks.append(value)

    seen: Set[str] = set()
    ordered_chunks: List[str] = []
    for chunk in prioritized_chunks:
        norm = norm_space(chunk)
        if norm and norm not in seen:
            ordered_chunks.append(norm)
            seen.add(norm)

    for candidate in ordered_chunks:
        country = infer_country(candidate)
        if country and country != "Remote":
            return country

    joined_sections = " ".join(norm_space(v) for v in sections.values() if norm_space(v))
    if joined_sections:
        country = infer_country(joined_sections)
        if country and country != "Remote":
            return country

    return ""

def infer_resume_profile(raw_text: str, parsed_sections: Dict[str, str]) -> Dict[str, Any]:
    summary = parsed_sections.get("summary") or ""
    education = parsed_sections.get("education") or ""
    skills = parsed_sections.get("skills") or ""
    experience = parsed_sections.get("experience") or ""
    projects = parsed_sections.get("projects") or ""
    resume_text = " ".join(x for x in [summary, education, skills, experience, projects] if x).strip()

    role_profile = classify_role_profile(
        " ".join(x for x in [summary, skills] if x),
        " ".join(x for x in [experience, projects, education, resume_text] if x),
    )
    legacy_category_info = classify_category(" ".join([summary, skills]), " ".join([experience, projects, education]), fallback=role_profile["legacy_category"])
    degree_info = infer_resume_degree(education, resume_text, fallback_category=role_profile["legacy_category"])
    candidate_country = infer_resume_country(raw_text, parsed_sections)
    candidate_experience_years = derive_resume_years_experience(experience, resume_text)
    return {
        "candidate_country": candidate_country,
        "candidate_experience_years": candidate_experience_years,
        "candidate_degree_level": degree_info["degree_level"],
        "candidate_degree_family": degree_info["degree_family"],
        "candidate_degree_fields": degree_info["degree_fields"],
        "candidate_category": role_profile["legacy_category"] or legacy_category_info["category"],
        "candidate_category_family": CATEGORY_FAMILY.get(role_profile["legacy_category"] or legacy_category_info["category"], "General"),
        "candidate_category_confidence": max(float(role_profile.get("category_key_confidence") or 0.0), float(legacy_category_info.get("category_confidence") or 0.0)),
        "candidate_category_runner_up": legacy_category_info.get("category_runner_up") or "",
        "candidate_function": role_profile["primary_function"],
        "candidate_function_confidence": role_profile["primary_function_confidence"],
        "candidate_function_runner_up": role_profile.get("primary_function_runner_up") or "",
        "candidate_function_scores": role_profile.get("primary_function_scores") or {},
        "candidate_domain": role_profile["primary_domain"],
        "candidate_domain_confidence": role_profile["primary_domain_confidence"],
        "candidate_domain_runner_up": role_profile.get("primary_domain_runner_up") or "",
        "candidate_domain_scores": role_profile.get("primary_domain_scores") or {},
        "candidate_category_key": role_profile["category_key"],
        "candidate_category_key_confidence": role_profile.get("category_key_confidence") or 0.0,
    }


def shortlist_jobs_for_resume(
    jobs: Sequence[Dict[str, Any]],
    resume_profile: Dict[str, Any],
    *,
    location_mode: str = "current",
    selected_countries: Optional[Sequence[str]] = None,
    max_jobs: int = 250,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    selected = [normalize_country_name(c) for c in (selected_countries or []) if norm_space(c)]
    selected = [c for c in selected if c]
    mode = (location_mode or "current").strip().lower()
    candidate_country = normalize_country_name(str(resume_profile.get("candidate_country") or ""))
    candidate_region = COUNTRY_REGION.get(candidate_country, "")
    allowed: Optional[Set[str]] = None
    if mode == "selected" and selected:
        allowed = set(selected)
    elif mode == "current" and candidate_country:
        allowed = {candidate_country}
    elif mode == "all":
        allowed = None

    candidate_degree_level = str(resume_profile.get("candidate_degree_level") or "none")
    candidate_degree_family = str(resume_profile.get("candidate_degree_family") or "General")
    candidate_years = resume_profile.get("candidate_experience_years")
    key_conf = float(resume_profile.get("candidate_category_key_confidence") or resume_profile.get("candidate_category_confidence") or 0.0)
    primary_threshold = 0.62 if key_conf >= 0.72 else 0.58
    secondary_threshold = 0.50 if key_conf >= 0.72 else 0.46

    def location_score_for(job_country: str, job_mode: str) -> float:
        if job_mode == "Remote":
            return 0.92
        if candidate_country and job_country == candidate_country:
            return 1.0
        if candidate_region and COUNTRY_REGION.get(job_country, "") == candidate_region and candidate_region:
            return 0.82
        return 0.24

    evaluated: List[Dict[str, Any]] = []
    filtered_out_location = 0
    for raw_job in jobs:
        job = enrich_job_record(raw_job)
        job_country = normalize_country_name(str(job.get("country") or ""))
        job_mode = canonicalize_work_mode(str(job.get("work_mode") or "On-site"))
        remote_cross_border = (job_mode == "Remote")
        if allowed is not None and job_country not in allowed and not remote_cross_border:
            filtered_out_location += 1
            continue

        key_score, key_parts = role_profile_similarity(resume_profile, job)
        degree_score = degree_fit_score(candidate_degree_level, candidate_degree_family, str(job.get("degree_level_min") or "none"), str(job.get("degree_family") or "General"))
        exp_score = experience_fit_score(candidate_years, job.get("experience_needed_years"))
        loc_score = location_score_for(job_country, job_mode)

        stage_rank = 2
        if key_score >= primary_threshold:
            stage_rank = 0
        elif key_score >= secondary_threshold:
            stage_rank = 1

        job["work_mode"] = job_mode
        job["prefilter_function_score"] = round(float(key_parts.get("function_score") or 0.0), 3)
        job["prefilter_domain_score"] = round(float(key_parts.get("domain_score") or 0.0), 3)
        job["prefilter_category_score"] = round(float(key_score), 3)
        job["prefilter_degree_score"] = round(float(degree_score), 3)
        job["prefilter_experience_score"] = round(float(exp_score), 3)
        job["prefilter_location_score"] = round(float(loc_score), 3)
        job["prefilter_stage_rank"] = stage_rank
        job["prefilter_score"] = round((160.0 * key_score) + (46.0 * degree_score) + (34.0 * exp_score) + (18.0 * loc_score), 3)
        evaluated.append(job)

    def sort_jobs(rows: List[Dict[str, Any]], order: str) -> List[Dict[str, Any]]:
        if order == "category":
            return sorted(rows, key=lambda j: (float(j.get("prefilter_category_score", 0.0)), float(j.get("prefilter_domain_score", 0.0)), float(j.get("prefilter_function_score", 0.0)), float(j.get("prefilter_degree_score", 0.0)), float(j.get("prefilter_experience_score", 0.0)), float(j.get("prefilter_location_score", 0.0))), reverse=True)
        if order == "degree":
            return sorted(rows, key=lambda j: (float(j.get("prefilter_degree_score", 0.0)), float(j.get("prefilter_category_score", 0.0)), float(j.get("prefilter_experience_score", 0.0)), float(j.get("prefilter_location_score", 0.0))), reverse=True)
        if order == "experience":
            return sorted(rows, key=lambda j: (float(j.get("prefilter_experience_score", 0.0)), float(j.get("prefilter_category_score", 0.0)), float(j.get("prefilter_degree_score", 0.0)), float(j.get("prefilter_location_score", 0.0))), reverse=True)
        return sorted(rows, key=lambda j: (float(j.get("prefilter_location_score", 0.0)), float(j.get("prefilter_category_score", 0.0)), float(j.get("prefilter_degree_score", 0.0)), float(j.get("prefilter_experience_score", 0.0))), reverse=True)

    primary_pool = [j for j in evaluated if int(j.get("prefilter_stage_rank", 3)) == 0]
    secondary_pool = [j for j in evaluated if int(j.get("prefilter_stage_rank", 3)) == 1]
    tertiary_pool = [j for j in evaluated if int(j.get("prefilter_stage_rank", 3)) >= 2]

    if len(primary_pool) >= max_jobs:
        shortlist = sort_jobs(primary_pool, "category")
        if len(shortlist) > max_jobs:
            shortlist = sort_jobs(shortlist, "degree")[:max_jobs]
        if len(shortlist) > max_jobs:
            shortlist = sort_jobs(shortlist, "experience")[:max_jobs]
        if len(shortlist) > max_jobs:
            shortlist = sort_jobs(shortlist, "location")[:max_jobs]
        shortlist = shortlist[:max_jobs]
    else:
        shortlist = sort_jobs(primary_pool, "category")
        seen_ids: Set[str] = {str(j.get("job_id") or f"AUTO-{i:05d}") for i, j in enumerate(shortlist)}

        def append_from(rows: List[Dict[str, Any]], order: str) -> None:
            nonlocal shortlist
            for job in sort_jobs(rows, order):
                jid = str(job.get("job_id") or f"AUTO-{len(seen_ids)+1:05d}")
                if jid in seen_ids:
                    continue
                shortlist.append(job)
                seen_ids.add(jid)
                if len(shortlist) >= max_jobs:
                    return

        if len(shortlist) < max_jobs:
            append_from(secondary_pool, "category")
        if len(shortlist) < max_jobs:
            remaining = [j for j in evaluated if str(j.get("job_id") or "") not in seen_ids]
            append_from([j for j in remaining if float(j.get("prefilter_degree_score", 0.0)) >= 0.82], "degree")
        if len(shortlist) < max_jobs:
            remaining = [j for j in evaluated if str(j.get("job_id") or "") not in seen_ids]
            append_from([j for j in remaining if float(j.get("prefilter_degree_score", 0.0)) >= 0.58], "degree")
        if len(shortlist) < max_jobs:
            remaining = [j for j in evaluated if str(j.get("job_id") or "") not in seen_ids]
            append_from([j for j in remaining if float(j.get("prefilter_experience_score", 0.0)) >= 0.74], "experience")
        if len(shortlist) < max_jobs:
            remaining = [j for j in evaluated if str(j.get("job_id") or "") not in seen_ids]
            append_from([j for j in remaining if float(j.get("prefilter_experience_score", 0.0)) >= 0.46], "experience")
        if len(shortlist) < max_jobs:
            remaining = [j for j in evaluated if str(j.get("job_id") or "") not in seen_ids]
            append_from(remaining, "location")
        if len(shortlist) < max_jobs:
            remaining = [j for j in tertiary_pool if str(j.get("job_id") or "") not in seen_ids]
            append_from(remaining, "category")
        shortlist = shortlist[:max_jobs]

    meta = {
        "candidate_country": candidate_country,
        "candidate_region": candidate_region,
        "location_mode": mode,
        "selected_countries": selected,
        "allowed_countries": sorted(allowed) if allowed is not None else [],
        "jobs_seen": len(jobs),
        "jobs_after_location": len(evaluated),
        "jobs_filtered_out_by_location": filtered_out_location,
        "jobs_shortlisted": len(shortlist),
        "jobs_primary_category": len(primary_pool),
        "jobs_secondary_category": len(secondary_pool),
        "category_threshold": round(primary_threshold, 3),
    }
    return shortlist, meta
