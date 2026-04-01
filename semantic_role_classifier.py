from __future__ import annotations

import math
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import numpy as np  # type: ignore
except Exception:  # pragma: no cover
    np = None  # type: ignore

try:
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:  # pragma: no cover
    SentenceTransformer = None  # type: ignore

from shared_model_registry import get_sentence_transformer

MODEL_NAME = os.environ.get("ROLEMATCHER_CATEGORY_MODEL", "all-MiniLM-L6-v2").strip() or "all-MiniLM-L6-v2"
USE_ST = (os.environ.get("ROLEMATCHER_ENABLE_CATEGORY_ST", "1").strip().lower() not in {"0", "false", "no", "off"})

FUNCTION_PROTOTYPES: Dict[str, List[str]] = {
    "Hardware / RTL / Verification": [
        "digital design verification engineer working on rtl, systemverilog, verilog, uvm, asic, soc, fpga, post-silicon and semiconductor validation",
        "hardware architecture, cpu gpu silicon, pcie, serdes, formal verification, timing closure and dv for complex chips",
    ],
    "Embedded / Firmware": [
        "embedded firmware engineer building bootloaders, drivers, board support packages and low level software for devices and peripherals",
        "microcontroller, bare metal, rtos, bring-up, c, c++, spi, i2c, uart and embedded systems development",
    ],
    "Software Engineering": [
        "software engineer building backend, frontend, full stack, systems, platform, web or application software and developer tooling",
        "programming in python java c++ javascript typescript for services, apis, platforms, operating systems and software products",
    ],
    "Data / AI / ML": [
        "machine learning, data science, data engineering, analytics, nlp, llm, computer vision, model training and mlops",
        "artificial intelligence systems, feature engineering, inference, experimentation, statistics and data pipelines",
    ],
    "IT / Cloud / DevOps / Security": [
        "cloud infrastructure, sre, devops, platform operations, kubernetes, docker, aws, azure, gcp and security engineering",
        "linux systems, networking, observability, incident response, identity, reliability, infrastructure automation and cybersecurity",
    ],
    "Electrical / Power / Controls": [
        "electrical engineer working on controls, plc, scada, motor control, high voltage, protection, substation, power electronics and grid systems",
        "power systems, industrial automation, electrical design, renewable energy, solar, utilities and controls engineering",
    ],
    "Mechanical / Manufacturing": [
        "mechanical engineering, manufacturing, cad, solidworks, tooling, process engineering, reliability, hvac, robotics and industrial systems",
        "product development, test, packaging, aerospace, mechanical design and manufacturing operations",
    ],
    "Civil / Construction": [
        "civil engineering, structural, transportation, construction, municipal infrastructure, geotechnical, revit and building systems",
        "site engineering, construction management, roads, buildings, infrastructure and public works",
    ],
    "Biomedical / Life Sciences / Chemistry": [
        "biomedical engineering, biotech, laboratory, clinical, chemistry, biology, medical devices, pharmaceutical and life sciences work",
        "research or engineering for healthcare, diagnostics, chemistry labs and biomedical systems",
    ],
    "Finance / Accounting": [
        "finance and accounting work including accounts payable, accounts receivable, reconciliation, audit, tax, financial reporting and fp&a",
        "controller, bookkeeping, general ledger, close process, budgeting and accounting operations",
    ],
    "Program / Product / Project Management": [
        "technical program manager, product manager or project manager coordinating roadmaps, releases, delivery, stakeholders, execution and cross-functional teams",
        "program management for hardware, software or platform teams, release management, schedule tracking, risks, dependencies and go-to-market execution",
    ],
    "Sales / Marketing / Customer Support": [
        "sales, marketing, account management, customer success, business development, recruiting, support and go-to-market work",
        "campaigns, pipeline, lead generation, customer relationships, outreach and customer support operations",
    ],
    "Retail / Warehouse / Logistics": [
        "retail store work, cashier, warehouse, fulfillment, logistics, merchandising, inventory and distribution operations",
        "warehouse associate, picker packer, forklift, stocking, shipping, receiving and logistics execution",
    ],
    "Design / UX / Creative": [
        "ux designer, ui designer, product designer, graphic designer, visual designer, interaction designer and creative design work",
        "figma, user research, wireframes, prototypes, visual systems, branding, adobe creative suite and design production",
    ],
    "Research / Academic": [
        "research engineer, research assistant, academic scientist, teaching assistant, postdoctoral scholar and publication-focused work",
        "experiments, papers, labs, grant work, scientific research, university and academic environments",
    ],
}

DOMAIN_PROTOTYPES: Dict[str, List[str]] = {
    "Semiconductors / Silicon / ASIC / FPGA": [
        "semiconductor, silicon, asic, soc, rtl, vlsi, fpga, cpu, gpu, pcie, serdes, post-silicon validation and chip development",
        "hardware platforms, silicon products, board-level systems, firmware-hardware co-design and low level platform enablement",
    ],
    "Datacenter / Cloud / Infrastructure": [
        "datacenter, cloud platforms, servers, distributed infrastructure, systems software, developer infrastructure and enterprise platforms",
        "backend services, cloud environments, infrastructure tooling, deployment systems and large scale platforms",
    ],
    "Web / Mobile / Product Applications": [
        "consumer or enterprise software applications, web products, mobile apps, user-facing features, frontend and full-stack products",
        "product engineering for websites, mobile apps, web platforms and software user experiences",
    ],
    "AI / ML / Data": [
        "machine learning, analytics, data engineering, nlp, llm, recommendation, experimentation and data science",
        "ai systems, model training, inference, feature stores, metrics and data platforms",
    ],
    "Power / Energy / Industrial Controls": [
        "power systems, electrical utilities, renewable energy, controls, automation, plc, scada and industrial systems",
        "substations, motor control, energy infrastructure, industrial automation and controls engineering",
    ],
    "Mechanical / Manufacturing / Industrial": [
        "mechanical products, manufacturing lines, process engineering, tooling, cad, product reliability and industrial equipment",
        "factory operations, manufacturing engineering, robotics, hvac, aerospace and mechanical systems",
    ],
    "Construction / Civil / Buildings": [
        "construction projects, civil infrastructure, structural systems, transportation, municipal works and buildings",
        "building systems, sites, roads, bridges, construction management and civil design",
    ],
    "Healthcare / Biotech / Life Sciences": [
        "healthcare, biotech, clinical, chemistry, biology, laboratory, pharmaceutical, diagnostics and biomedical products",
        "medical devices, healthcare research, life science labs and bioengineering",
    ],
    "Finance / Accounting / ERP": [
        "accounting operations, finance systems, audit, payroll, tax, sap, erp and financial reporting",
        "bookkeeping, general ledger, financial analytics and accounting process execution",
    ],
    "Business / GTM / Operations": [
        "business operations, go-to-market, supply chain, procurement, project delivery, customer operations and business systems",
        "operations planning, product launches, stakeholder management, commercial operations and process execution",
    ],
    "Retail / Warehouse / Logistics": [
        "retail stores, warehousing, shipping, receiving, fulfillment, logistics and distribution centers",
        "inventory, stocking, merchandising, last-mile logistics and warehouse operations",
    ],
    "Design / UX / Creative": [
        "ux, ui, interaction design, user research, visual systems, branding, graphic design and creative production",
        "wireframes, prototypes, user journeys, figma, adobe and creative design work",
    ],
    "Academic / Research": [
        "research labs, publications, scientific work, academic institutions, experiments and research programs",
        "university research, teaching, papers, grant work and lab-based discovery",
    ],
    "General": [
        "general professional work",
    ],
}

FUNCTION_KEYWORDS: Dict[str, List[str]] = {
    "Hardware / RTL / Verification": ["rtl", "systemverilog", "verilog", "uvm", "design verification", "asic", "soc", "fpga", "semiconductor", "silicon", "pcie", "serdes", "timing closure", "post-silicon", "validation engineer"],
    "Embedded / Firmware": ["firmware", "embedded", "microcontroller", "rtos", "bare metal", "device driver", "bootloader", "board bring-up", "bsp", "spi", "i2c", "uart"],
    "Software Engineering": ["software engineer", "software developer", "backend", "frontend", "full stack", "full-stack", "platform engineer", "systems engineer", "developer", "api", "distributed systems", "linux programming", "c++", "python", "java", "typescript", "javascript"],
    "Data / AI / ML": ["machine learning", "data science", "data engineer", "nlp", "llm", "computer vision", "mlops", "analytics", "data pipeline", "feature engineering"],
    "IT / Cloud / DevOps / Security": ["devops", "site reliability", "sre", "cloud", "aws", "azure", "gcp", "kubernetes", "docker", "cybersecurity", "security engineer", "infrastructure"],
    "Electrical / Power / Controls": ["electrical engineer", "power systems", "controls", "plc", "scada", "substation", "high voltage", "motor control", "power electronics", "grid"],
    "Mechanical / Manufacturing": ["mechanical engineer", "manufacturing engineer", "solidworks", "cad", "process engineer", "tooling", "hvac", "reliability engineer"],
    "Civil / Construction": ["civil engineer", "structural", "construction", "site engineer", "geotechnical", "transportation engineer", "revit", "municipal"],
    "Biomedical / Life Sciences / Chemistry": ["biomedical", "biology", "chemistry", "biotech", "clinical", "laboratory", "pharmaceutical", "medical device"],
    "Finance / Accounting": ["accounts payable", "accounts receivable", "audit", "tax", "bookkeeping", "controller", "fp&a", "financial analyst", "accounting"],
    "Program / Product / Project Management": ["program manager", "technical program manager", "project manager", "product manager", "release manager", "release planning", "roadmap", "stakeholder", "cross-functional", "program management", "delivery"],
    "Sales / Marketing / Customer Support": ["sales", "marketing", "customer success", "account executive", "business development", "support specialist", "customer support", "recruiter", "hr"],
    "Retail / Warehouse / Logistics": ["retail", "cashier", "warehouse", "fulfillment", "picker", "packer", "forklift", "store associate", "merchandising"],
    "Design / UX / Creative": ["ux designer", "ui designer", "product designer", "graphic designer", "visual designer", "interaction designer", "figma", "wireframe", "prototype", "user research", "illustrator", "photoshop"],
    "Research / Academic": ["research assistant", "research engineer", "research scientist", "postdoctoral", "teaching assistant", "publication", "journal", "academic"],
}

DOMAIN_KEYWORDS: Dict[str, List[str]] = {
    "Semiconductors / Silicon / ASIC / FPGA": ["semiconductor", "silicon", "asic", "soc", "fpga", "vlsi", "cpu", "gpu", "pcie", "serdes", "board support package", "firmware release"],
    "Datacenter / Cloud / Infrastructure": ["datacenter", "cloud", "server", "distributed systems", "infrastructure", "platform", "linux", "observability", "deployment", "cluster"],
    "Web / Mobile / Product Applications": ["web", "frontend", "backend", "full stack", "mobile", "ios", "android", "react", "node", "product application", "user-facing"],
    "AI / ML / Data": ["machine learning", "data science", "data engineer", "analytics", "llm", "nlp", "computer vision", "mlops", "feature store"],
    "Power / Energy / Industrial Controls": ["power systems", "controls", "plc", "scada", "grid", "substation", "high voltage", "renewable", "solar", "industrial automation"],
    "Mechanical / Manufacturing / Industrial": ["manufacturing", "mechanical", "tooling", "cad", "hvac", "aerospace", "robotics", "factory", "industrial"],
    "Construction / Civil / Buildings": ["construction", "civil", "structural", "transportation", "municipal", "building", "infrastructure", "site"],
    "Healthcare / Biotech / Life Sciences": ["biomedical", "biotech", "clinical", "laboratory", "pharmaceutical", "medical device", "biology", "chemistry", "healthcare"],
    "Finance / Accounting / ERP": ["accounting", "finance", "audit", "tax", "payroll", "erp", "sap", "general ledger", "quickbooks"],
    "Business / GTM / Operations": ["operations", "supply chain", "procurement", "go-to-market", "program", "delivery", "business systems", "customer operations"],
    "Retail / Warehouse / Logistics": ["retail", "warehouse", "fulfillment", "inventory", "distribution", "logistics", "merchandising"],
    "Design / UX / Creative": ["ux", "ui", "figma", "wireframe", "prototype", "visual design", "branding", "adobe"],
    "Academic / Research": ["research", "publication", "lab", "academic", "university", "experiments"],
    "General": [],
}

LEGACY_CATEGORY_FROM_FUNCTION = {
    "Hardware / RTL / Verification": "Hardware / RTL / Verification",
    "Embedded / Firmware": "Embedded / Firmware",
    "Software Engineering": "Software Engineering",
    "Data / AI / ML": "Data / AI / ML",
    "IT / Cloud / DevOps / Security": "IT / Cloud / DevOps / Security",
    "Electrical / Power / Controls": "Electrical / Power / Controls",
    "Mechanical / Manufacturing": "Mechanical / Manufacturing",
    "Civil / Construction": "Civil / Construction",
    "Biomedical / Life Sciences / Chemistry": "Biomedical / Life Sciences / Chemistry",
    "Finance / Accounting": "Finance / Accounting",
    "Program / Product / Project Management": "Business / Operations / Supply Chain",
    "Sales / Marketing / Customer Support": "Sales / Marketing / Customer Support",
    "Retail / Warehouse / Logistics": "Retail / Warehouse / Logistics",
    "Design / UX / Creative": "Design / UX / Creative",
    "Research / Academic": "Research / Academic",
}

FUNCTION_RELATED: Dict[str, Dict[str, float]] = {
    "Software Engineering": {"Program / Product / Project Management": 0.68, "IT / Cloud / DevOps / Security": 0.72, "Embedded / Firmware": 0.76, "Data / AI / ML": 0.66},
    "Hardware / RTL / Verification": {"Program / Product / Project Management": 0.66, "Embedded / Firmware": 0.84, "Electrical / Power / Controls": 0.68},
    "Embedded / Firmware": {"Program / Product / Project Management": 0.64, "Hardware / RTL / Verification": 0.84, "Software Engineering": 0.76},
    "Data / AI / ML": {"Software Engineering": 0.66, "Program / Product / Project Management": 0.58},
    "IT / Cloud / DevOps / Security": {"Software Engineering": 0.72, "Program / Product / Project Management": 0.54},
    "Electrical / Power / Controls": {"Program / Product / Project Management": 0.56, "Hardware / RTL / Verification": 0.68},
    "Mechanical / Manufacturing": {"Program / Product / Project Management": 0.5},
    "Biomedical / Life Sciences / Chemistry": {"Research / Academic": 0.62},
    "Program / Product / Project Management": {"Software Engineering": 0.68, "Hardware / RTL / Verification": 0.66, "Embedded / Firmware": 0.64, "Data / AI / ML": 0.58, "Business / Operations / Supply Chain": 0.56},
    "Research / Academic": {"Data / AI / ML": 0.62, "Biomedical / Life Sciences / Chemistry": 0.62, "Hardware / RTL / Verification": 0.54},
}

DOMAIN_RELATED: Dict[str, Dict[str, float]] = {
    "Semiconductors / Silicon / ASIC / FPGA": {"Datacenter / Cloud / Infrastructure": 0.62, "Web / Mobile / Product Applications": 0.36},
    "Datacenter / Cloud / Infrastructure": {"Semiconductors / Silicon / ASIC / FPGA": 0.62, "AI / ML / Data": 0.58, "Web / Mobile / Product Applications": 0.62},
    "Web / Mobile / Product Applications": {"Datacenter / Cloud / Infrastructure": 0.62, "AI / ML / Data": 0.44, "Design / UX / Creative": 0.52},
    "AI / ML / Data": {"Datacenter / Cloud / Infrastructure": 0.58, "Web / Mobile / Product Applications": 0.44, "Academic / Research": 0.54},
    "Power / Energy / Industrial Controls": {"Mechanical / Manufacturing / Industrial": 0.46},
    "Mechanical / Manufacturing / Industrial": {"Power / Energy / Industrial Controls": 0.46, "Construction / Civil / Buildings": 0.36},
    "Construction / Civil / Buildings": {"Mechanical / Manufacturing / Industrial": 0.36},
    "Healthcare / Biotech / Life Sciences": {"Academic / Research": 0.56},
    "Academic / Research": {"AI / ML / Data": 0.54, "Healthcare / Biotech / Life Sciences": 0.56},
    "Business / GTM / Operations": {"Finance / Accounting / ERP": 0.44, "Retail / Warehouse / Logistics": 0.42},
    "Finance / Accounting / ERP": {"Business / GTM / Operations": 0.44},
    "Retail / Warehouse / Logistics": {"Business / GTM / Operations": 0.42},
}

_MODEL = None
_EMBED_CACHE: Dict[str, Tuple[List[str], Dict[str, Any]]] = {}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _normalize_score_map(raw_scores: Dict[str, float]) -> Dict[str, float]:
    cleaned = {k: max(0.0, float(v)) for k, v in raw_scores.items() if float(v) > 0.0}
    total = sum(cleaned.values())
    if total <= 0.0:
        return {}
    return {k: round(v / total, 6) for k, v in cleaned.items()}


def _softmax(values: Sequence[float], temp: float = 0.16) -> List[float]:
    if not values:
        return []
    vmax = max(float(v) for v in values)
    exps = [math.exp((float(v) - vmax) / max(0.05, temp)) for v in values]
    total = sum(exps) or 1.0
    return [v / total for v in exps]


def _get_model() -> Any:
    global _MODEL
    if not USE_ST or SentenceTransformer is None:
        return None
    if _MODEL is not None:
        return _MODEL
    try:
        _MODEL = get_sentence_transformer(MODEL_NAME)
    except Exception:
        _MODEL = None
    return _MODEL


def _get_label_embeddings(kind: str, prototypes: Dict[str, List[str]]) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    if np is None:
        return list(prototypes.keys()), None
    if kind in _EMBED_CACHE:
        return _EMBED_CACHE[kind]
    model = _get_model()
    labels = list(prototypes.keys())
    if model is None:
        _EMBED_CACHE[kind] = (labels, None)
        return labels, None
    try:
        proto_texts = [" ".join(prototypes.get(label, [])) for label in labels]
        vecs = model.encode(proto_texts, normalize_embeddings=True, show_progress_bar=False)
        emb = {label: vecs[i] for i, label in enumerate(labels)}
        _EMBED_CACHE[kind] = (labels, emb)
        return labels, emb
    except Exception:
        _EMBED_CACHE[kind] = (labels, None)
        return labels, None


def _keyword_scores(title: str, description: str, keywords: Dict[str, List[str]]) -> Dict[str, float]:
    title_low = _norm(title).lower()
    body_low = _norm(f"{title} {title} {title} {description}").lower()
    out: Dict[str, float] = {}
    for label, kws in keywords.items():
        score = 0.0
        for kw in kws:
            kwl = kw.lower()
            body_hits = body_low.count(kwl)
            if not body_hits:
                continue
            body_bonus = 1.0 + (0.25 if " " in kwl else 0.0)
            title_bonus = 1.8 if kwl in title_low else 1.0
            score += body_hits * body_bonus * title_bonus
        if score > 0:
            out[label] = round(score, 4)
    return out


def _semantic_scores(title: str, description: str, kind: str, prototypes: Dict[str, List[str]]) -> Dict[str, float]:
    model = _get_model()
    labels, emb_map = _get_label_embeddings(kind, prototypes)
    if model is None or emb_map is None or np is None:
        return {}
    text = _norm(f"{title} {title} {title} {description}")[:5000]
    if not text:
        return {}
    try:
        vec = model.encode([text], normalize_embeddings=True, show_progress_bar=False)[0]
        sims = [float(np.dot(vec, emb_map[label])) for label in labels]
        probs = _softmax(sims, temp=0.14 if kind == "function" else 0.16)
        return {label: round(max(0.0, p), 6) for label, p in zip(labels, probs)}
    except Exception:
        return {}


def _top_scores(score_map: Dict[str, float], n: int = 5) -> Dict[str, float]:
    return {k: round(v, 4) for k, v in sorted(score_map.items(), key=lambda kv: (-kv[1], kv[0]))[:n]}


def _best_label(score_map: Dict[str, float]) -> Tuple[str, float, str, float]:
    if not score_map:
        return "", 0.0, "", 0.0
    ranked = sorted(score_map.items(), key=lambda kv: (-kv[1], kv[0]))
    top_label, top_score = ranked[0]
    runner_label, runner_score = ranked[1] if len(ranked) > 1 else ("", 0.0)
    return top_label, float(top_score), runner_label, float(runner_score)




def _apply_title_overrides(title: str, function_scores: Dict[str, float], domain_scores: Dict[str, float], description: str) -> Tuple[Dict[str, float], Dict[str, float]]:
    title_low = _norm(title).lower()
    desc_low = _norm(description).lower()

    def boost(scores: Dict[str, float], label: str, amount: float) -> None:
        scores[label] = round(float(scores.get(label, 0.0)) + amount, 6)

    if any(tok in title_low for tok in ["program manager", "project manager", "product manager", "technical program manager", "tpm"]):
        boost(function_scores, "Program / Product / Project Management", 0.42)
    if any(tok in title_low for tok in ["software engineer", "software developer", "backend", "frontend", "full stack", "full-stack"]):
        boost(function_scores, "Software Engineering", 0.38)
    if any(tok in title_low for tok in ["firmware", "embedded"]):
        boost(function_scores, "Embedded / Firmware", 0.36)
    if any(tok in title_low for tok in ["rtl", "verification", "design verification", "asic", "fpga", "silicon"]):
        boost(function_scores, "Hardware / RTL / Verification", 0.36)
    if any(tok in title_low for tok in ["ux designer", "ui designer", "product designer", "graphic designer", "visual designer", "interaction designer"]):
        boost(function_scores, "Design / UX / Creative", 0.42)
        boost(domain_scores, "Design / UX / Creative", 0.34)

    technical_hits = sum(1 for tok in ["software", "firmware", "linux", "datacenter", "server", "gpu", "cpu", "pcie", "semiconductor", "asic", "fpga", "c++", "python"] if tok in desc_low or tok in title_low)
    if technical_hits >= 3:
        boost(domain_scores, "Datacenter / Cloud / Infrastructure", 0.16)
    if technical_hits >= 3 and any(tok in desc_low or tok in title_low for tok in ["gpu", "pcie", "semiconductor", "asic", "fpga", "silicon"]):
        boost(domain_scores, "Semiconductors / Silicon / ASIC / FPGA", 0.14)
    if technical_hits >= 3 and any(tok in title_low for tok in ["software", "platform", "systems"]):
        boost(function_scores, "Software Engineering", 0.12)
    if technical_hits >= 3 and "Design / UX / Creative" in function_scores and not any(tok in title_low for tok in ["designer", "ux", "ui", "graphic"]):
        function_scores["Design / UX / Creative"] *= 0.18
    if technical_hits >= 3 and "Design / UX / Creative" in domain_scores and not any(tok in title_low for tok in ["designer", "ux", "ui", "graphic"]):
        domain_scores["Design / UX / Creative"] *= 0.15

    return function_scores, domain_scores
def classify_role_profile(title: str = "", description: str = "") -> Dict[str, Any]:
    title_clean = _norm(title)
    desc_clean = _norm(description)

    f_lex = _normalize_score_map(_keyword_scores(title_clean, desc_clean, FUNCTION_KEYWORDS))
    f_sem = _semantic_scores(title_clean, desc_clean, "function", FUNCTION_PROTOTYPES)
    d_lex = _normalize_score_map(_keyword_scores(title_clean, desc_clean, DOMAIN_KEYWORDS))
    d_sem = _semantic_scores(title_clean, desc_clean, "domain", DOMAIN_PROTOTYPES)

    function_scores: Dict[str, float] = {}
    for label in FUNCTION_PROTOTYPES:
        score = (0.48 * f_lex.get(label, 0.0)) + (0.52 * f_sem.get(label, 0.0))
        if score > 0:
            function_scores[label] = score
    function_scores = _normalize_score_map(function_scores)

    domain_scores: Dict[str, float] = {}
    for label in DOMAIN_PROTOTYPES:
        score = (0.34 * d_lex.get(label, 0.0)) + (0.66 * d_sem.get(label, 0.0))
        if score > 0:
            domain_scores[label] = score
    domain_scores = _normalize_score_map(domain_scores)

    function_scores, domain_scores = _apply_title_overrides(title_clean, function_scores, domain_scores, desc_clean)
    function_scores = _normalize_score_map(function_scores)
    domain_scores = _normalize_score_map(domain_scores)

    function_label, function_score, function_runner, function_runner_score = _best_label(function_scores)
    domain_label, domain_score, domain_runner, domain_runner_score = _best_label(domain_scores)

    if not function_label:
        function_label = "Software Engineering" if any(tok in desc_clean.lower() for tok in ["software", "developer", "programming", "linux", "api"]) else "Program / Product / Project Management"
        function_score = 0.34
    if not domain_label:
        domain_label = "General"
        domain_score = 0.2

    legacy_category = derive_legacy_category(function_label, domain_label)
    key = make_role_key(function_label, domain_label)
    key_conf = round(min(0.995, (0.46 * function_score) + (0.54 * domain_score) + max(0.0, (domain_score - domain_runner_score)) * 0.12), 3)
    return {
        "primary_function": function_label,
        "primary_function_confidence": round(function_score, 3),
        "primary_function_runner_up": function_runner,
        "primary_function_scores": _top_scores(function_scores, 6),
        "primary_domain": domain_label,
        "primary_domain_confidence": round(domain_score, 3),
        "primary_domain_runner_up": domain_runner,
        "primary_domain_scores": _top_scores(domain_scores, 6),
        "category_key": key,
        "category_key_confidence": key_conf,
        "legacy_category": legacy_category,
    }


def make_role_key(function_label: str, domain_label: str) -> str:
    def slug(x: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", (x or "general").lower()).strip("-") or "general"
    return f"{slug(function_label)}__{slug(domain_label)}"


def derive_legacy_category(function_label: str, domain_label: str) -> str:
    if function_label == "Program / Product / Project Management":
        if domain_label in {"Semiconductors / Silicon / ASIC / FPGA", "Power / Energy / Industrial Controls"}:
            return "Hardware / RTL / Verification"
        if domain_label == "AI / ML / Data":
            return "Data / AI / ML"
        if domain_label in {"Datacenter / Cloud / Infrastructure", "Web / Mobile / Product Applications"}:
            return "Software Engineering"
        if domain_label == "Healthcare / Biotech / Life Sciences":
            return "Biomedical / Life Sciences / Chemistry"
        return "Business / Operations / Supply Chain"
    return LEGACY_CATEGORY_FROM_FUNCTION.get(function_label, "General")


def _label_relation(a: str, b: str, related: Dict[str, Dict[str, float]]) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    direct = related.get(a, {}).get(b)
    if direct is not None:
        return float(direct)
    reverse = related.get(b, {}).get(a)
    if reverse is not None:
        return float(reverse)
    return 0.0


def _score_map_similarity(a_scores: Dict[str, float], b_scores: Dict[str, float], related: Dict[str, Dict[str, float]]) -> float:
    labels = set(a_scores.keys()) | set(b_scores.keys())
    total = 0.0
    for a_label, a_weight in a_scores.items():
        for b_label, b_weight in b_scores.items():
            rel = _label_relation(a_label, b_label, related)
            if rel <= 0.0:
                continue
            total += float(a_weight) * float(b_weight) * rel
    if labels and total <= 0.0:
        # weak fallback: overlapping low-confidence tails
        common = labels & set(labels)
    return max(0.0, min(1.0, total))


def role_profile_similarity(resume_profile: Dict[str, Any], job_profile: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    rf_scores = dict(resume_profile.get("candidate_function_scores") or {})
    jf_scores = dict(job_profile.get("job_function_scores") or {})
    rd_scores = dict(resume_profile.get("candidate_domain_scores") or {})
    jd_scores = dict(job_profile.get("job_domain_scores") or {})

    if not rf_scores and resume_profile.get("candidate_function"):
        rf_scores = {str(resume_profile.get("candidate_function")): 1.0}
    if not jf_scores and job_profile.get("job_function"):
        jf_scores = {str(job_profile.get("job_function")): 1.0}
    if not rd_scores and resume_profile.get("candidate_domain"):
        rd_scores = {str(resume_profile.get("candidate_domain")): 1.0}
    if not jd_scores and job_profile.get("job_domain"):
        jd_scores = {str(job_profile.get("job_domain")): 1.0}

    function_overlap = _score_map_similarity(rf_scores, jf_scores, FUNCTION_RELATED)
    domain_overlap = _score_map_similarity(rd_scores, jd_scores, DOMAIN_RELATED)

    primary_function_rel = _label_relation(str(resume_profile.get("candidate_function") or ""), str(job_profile.get("job_function") or ""), FUNCTION_RELATED)
    primary_domain_rel = _label_relation(str(resume_profile.get("candidate_domain") or ""), str(job_profile.get("job_domain") or ""), DOMAIN_RELATED)

    function_score = max(function_overlap, primary_function_rel)
    domain_score = max(domain_overlap, primary_domain_rel)

    overall = (0.42 * function_score) + (0.58 * domain_score)
    if str(resume_profile.get("candidate_domain") or "") == str(job_profile.get("job_domain") or "") and str(job_profile.get("job_domain") or "") not in {"", "General"}:
        overall += 0.06
    if str(resume_profile.get("candidate_function") or "") == str(job_profile.get("job_function") or "") and str(job_profile.get("job_function") or ""):
        overall += 0.04
    overall = max(0.0, min(1.0, overall))

    return round(overall, 4), {
        "function_score": round(function_score, 4),
        "domain_score": round(domain_score, 4),
        "function_overlap": round(function_overlap, 4),
        "domain_overlap": round(domain_overlap, 4),
    }
