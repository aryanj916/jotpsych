#!/usr/bin/env python3
"""
JotPsych — Clinic Intelligence Scraper
--------------------------------------
Scrape a clinic website (single URL or CSV) and use an LLM to extract:
  specialty, modalities, location, clinic_size

Default provider: Gemini 2.5 Pro via Google GenAI SDK (response schema enforced).

Usage examples:
  # Single URL
  python jotpsych_scraper.py --url https://exampleclinic.com --provider gemini --out results.jsonl

  # Batch via CSV (expects a column named "url")
  python jotpsych_scraper.py --input_csv clinics.csv --provider gemini --out results.jsonl

Environment variables:
  GEMINI_API_KEY   

Install:
  pip install -r requirements.txt
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, ValidationError

# Auto-load environment variables from a .env file if present
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(), override=False)
except Exception:
    # If python-dotenv is not installed, continue; env vars may be set in the shell
    pass

# ----------------------------
# Constants & heuristics
# ----------------------------

DEFAULT_MAX_PAGES = 20
DEFAULT_MAX_DEPTH = 2
DEFAULT_TIMEOUT = 20.0
USER_AGENT = "Mozilla/5.0 (compatible; JotPsychScraper/1.0; +https://www.jotpsych.com/)"
# US state full-name to abbreviation mapping
US_STATE_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
    "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "District of Columbia": "DC"
}
RELEVANT_PATH_TERMS = [
    "about", "team", "our-team", "providers", "clinicians", "staff",
    "physicians", "doctors", "practitioners", "care-team", "medical-staff",
    "services", "specialties", "treatments", "conditions",
    "locations", "location", "contact", "contact-us", "contactus", "directions", "map", "find-us", "findus", "visit", "hours", "address", "addresses", "office", "offices",
    "who-we-are", "practice", "psychiatry",
    "psychology", "therapy", "meet-the-team", "meet-our-team",
    "our-providers", "provider", "physician", "meet-our-providers",
    "meet-our-physicians", "our-physicians", "our-clinicians", "leadership",
    "about-us", "aboutus", "our-staff", "careers", "people"
]
PRIORITY_ORDER = [
    "about", "about-us", "our-team", "team", "meet-the-team", "leadership",
    "providers", "our-providers", "provider", "physicians", "our-physicians", "meet-our-physicians",
    "doctors", "clinicians", "care-team", "medical-staff",
    "contact", "contact-us", "locations", "location", "directions", "map", "address", "hours",
    "services", "specialties", "treatments", "offices"
]
DISALLOWED_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".mp4", ".mov", ".avi", ".mp3", ".wav",
    ".css", ".js", ".json"
}

# ----------------------------
# Data models
# ----------------------------

class ClinicInfo(BaseModel):
    specialty: str = Field(default="unknown")
    modalities: str = Field(default="unknown")
    location: str = Field(default="unknown")
    clinic_size: str = Field(default="unknown")  # "1" | "2-5" | "6-20" | "21+" | "unknown"

class ExtractionResult(BaseModel):
    clinic_info: ClinicInfo

@dataclass
class PagePayload:
    url: str
    text: str
    jsonld: Optional[dict] = None

# ----------------------------
# clinic_size normalization helpers
# ----------------------------

def _extract_exact_provider_count(pages: List[PagePayload]) -> Optional[int]:
    """Try to find an exact provider/clinician count from page text or JSON-LD."""
    candidate_counts: List[int] = []
    patterns = [
        r"\bteam of\s+(\d{1,3})\b",
        r"\b(\d{1,3})\s*\+\s*(?:providers?|clinicians?|physicians?|doctors?|therapists?)\b",
        r"\b(\d{1,3})\s+(?:providers?|clinicians?|physicians?|doctors?|therapists?|practitioners?)\b",
    ]
    for p in pages:
        text = p.text or ""
        for pat in patterns:
            for m in re.finditer(pat, text, flags=re.IGNORECASE):
                try:
                    n = int(m.group(1))
                    if 1 <= n <= 500:
                        candidate_counts.append(n)
                except Exception:
                    continue
        # Try JSON-LD numeric hints
        if p.jsonld and isinstance(p.jsonld, dict):
            for key in ("numberOfEmployees", "employeeCount", "employees", "staffCount"):
                v = p.jsonld.get(key)
                if isinstance(v, int) and 1 <= v <= 500:
                    candidate_counts.append(v)
                elif isinstance(v, str):
                    try:
                        digits = re.sub(r"[^0-9]", "", v)
                        if digits:
                            n = int(digits)
                            if 1 <= n <= 500:
                                candidate_counts.append(n)
                    except Exception:
                        pass
    if candidate_counts:
        return max(candidate_counts)
    return None

def _label_for_count(n: int) -> str:
    if n <= 1:
        return "Solo Practice"
    if n <= 10:
        return "Small Group Practice"
    if n <= 20:
        return "Medium Group Practice"
    return "Large Group Practice"

def normalize_clinic_size_value(raw_value: str, pages: List[PagePayload]) -> str:
    """Normalize clinic_size to include exact count if found, else a labeled range."""
    raw = (raw_value or "").strip()
    # Try multiple strategies for exact estimation
    exact_counts: List[int] = []
    from_text_exact = _extract_exact_provider_count(pages)
    if from_text_exact is not None:
        exact_counts.append(from_text_exact)
    name_count = estimate_provider_count_from_pages(pages)
    if name_count is not None:
        exact_counts.append(name_count)
    exact = max(exact_counts) if exact_counts else None
    if exact is not None:
        if exact == 1:
            return "Solo Practice (1 provider)"
        label = _label_for_count(exact)
        return f"{label} ({exact} providers)"

    # If a range is already present, standardize to our buckets
    m = re.search(r"(\d{1,3})\s*-\s*(\d{1,3})", raw)
    if m:
        low, high = int(m.group(1)), int(m.group(2))
        if high <= 10:
            return "Small Group Practice (2-10 providers)"
        if high <= 20:
            return "Medium Group Practice (11-20 providers)"
        return "Large Group Practice (21+ providers)"

    # Keyword-based mapping
    if re.search(r"\bsolo\b|\b1\b", raw, flags=re.IGNORECASE):
        return "Solo Practice (1 provider)"
    if re.search(r"hospital|medical center|health system|system", raw, flags=re.IGNORECASE):
        return "Hospital System (21+ providers)"
    if re.search(r"small", raw, flags=re.IGNORECASE):
        return "Small Group Practice (2-10 providers)"
    if re.search(r"medium", raw, flags=re.IGNORECASE):
        return "Medium Group Practice (11-20 providers)"
    if re.search(r"large", raw, flags=re.IGNORECASE):
        return "Large Group Practice (21+ providers)"
    if re.search(r"group", raw, flags=re.IGNORECASE):
        return "Group Practice (unknown)"
    if raw.lower() in {"unknown", "not specified", "n/a", ""}:
        return "unknown"
    return raw

def estimate_provider_count_from_pages(pages: List[PagePayload]) -> Optional[int]:
    """Estimate number of providers by scanning names with clinical licenses or "Dr." patterns.
    Returns None if no plausible providers are found.
    """
    if not pages:
        return None
    license_tokens = {
        "MD", "DO", "MBBS", "MBChB", "FRCS", "FRCP", "FACC",
        "PhD", "PsyD", "EdD",
        "NP", "DNP", "FNP", "PMHNP", "APRN", "ARNP", "CNM",
        "PA-C", "PA",
        "LCSW", "LMSW", "MSW", "LICSW",
        "LMFT", "MFT",
        "LPC", "LPCC", "LCPC", "LMHC",
        "BCBA", "LBA",
        "RN", "BSN", "MSN"
    }
    names: set[str] = set()
    # Patterns
    pat_dr = re.compile(r"\bDr\.?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b")
    pat_name_degree = re.compile(
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?:,|\s)\s*(MD|DO|MBBS|MBChB|FRCS|FRCP|FACC|PhD|PsyD|EdD|NP|DNP|FNP|PMHNP|APRN|ARNP|CNM|PA-C|PA|LCSW|LMSW|MSW|LICSW|LMFT|MFT|LPC|LPCC|LCPC|LMHC|BCBA|LBA|RN|BSN|MSN)\b"
    )
    for p in pages:
        text = (p.text or "").strip()
        if not text:
            continue
        # Prefer shorter lines to reduce noise
        for line in text.splitlines():
            s = line.strip()
            if not s or len(s) > 140:
                continue
            for m in pat_name_degree.finditer(s):
                name = m.group(1).lower()
                degree = m.group(2).upper()
                if degree in license_tokens:
                    names.add(name)
            for m in pat_dr.finditer(s):
                names.add(m.group(0).lower())
    if names:
        # Return a conservative count (unique names)
        return min(len(names), 500)
    return None

def collect_provider_name_candidates(pages: List[PagePayload], max_samples: int = 50) -> List[str]:
    """Return a sample list of provider-like names detected in text."""
    if not pages:
        return []
    candidates: List[str] = []
    seen: set[str] = set()
    pat_dr = re.compile(r"\bDr\.?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b")
    pat_name_degree = re.compile(
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?:,|\s)\s*(MD|DO|MBBS|MBChB|FRCS|FRCP|FACC|PhD|PsyD|EdD|NP|DNP|FNP|PMHNP|APRN|ARNP|CNM|PA-C|PA|LCSW|LMSW|MSW|LICSW|LMFT|MFT|LPC|LPCC|LCPC|LMHC|BCBA|LBA|RN|BSN|MSN)\b"
    )
    for p in pages:
        text = (p.text or "")
        for line in text.splitlines():
            s = line.strip()
            if not s or len(s) > 140:
                continue
            for m in pat_name_degree.finditer(s):
                name = m.group(1).strip()
                if name.lower() not in seen:
                    candidates.append(name)
                    seen.add(name.lower())
                    if len(candidates) >= max_samples:
                        return candidates
            for m in pat_dr.finditer(s):
                name = m.group(0).strip()
                if name.lower() not in seen:
                    candidates.append(name)
                    seen.add(name.lower())
                    if len(candidates) >= max_samples:
                        return candidates
    return candidates

def collect_specialty_candidates(pages: List[PagePayload], max_samples: int = 20) -> Dict[str, List[str]]:
    """Collect specialties from JSON-LD and text.
    Returns dict with keys: jsonld, text
    """
    jsonld_specs: List[str] = []
    text_specs: List[str] = []
    # JSON-LD
    for p in pages:
        jd = p.jsonld
        if isinstance(jd, dict):
            val = jd.get("medicalSpecialty") or jd.get("specialty") or jd.get("department")
            if isinstance(val, str):
                jsonld_specs.append(val.strip())
            elif isinstance(val, list):
                for v in val:
                    if isinstance(v, str):
                        jsonld_specs.append(v.strip())
    # Text heuristics
    specialty_keywords = [
        "psychiatry", "psychology", "psychotherapy", "sleep medicine", "cardiology", "podiatry",
        "neurology", "orthopedics", "oncology", "pediatrics", "obstetrics", "gynecology",
        "primary care", "internal medicine", "dermatology", "gastroenterology", "urology",
        "endocrinology", "rheumatology", "nephrology", "pulmonology", "otolaryngology",
        "ophthalmology", "dentistry", "behavioral health", "mental health"
    ]
    kw_pattern = re.compile(r"|".join([re.escape(k) for k in specialty_keywords]), re.IGNORECASE)
    seen: set[str] = set()
    for p in pages:
        for line in (p.text or "").splitlines():
            s = line.strip()
            if not s or len(s) > 140:
                continue
            for m in kw_pattern.finditer(s):
                k = m.group(0).lower()
                if k not in seen:
                    text_specs.append(k)
                    seen.add(k)
                    if len(text_specs) >= max_samples:
                        break
            if len(text_specs) >= max_samples:
                break
    return {
        "jsonld": list(dict.fromkeys(jsonld_specs))[:max_samples],
        "text": list(dict.fromkeys(text_specs))[:max_samples],
    }

def collect_modality_candidates(pages: List[PagePayload], max_samples: int = 30) -> List[str]:
    """Collect modality/treatment keywords from text."""
    modality_keywords = [
        # Behavioral health
        "CBT", "CBT-I", "DBT", "EMDR", "ACT", "ERP", "exposure therapy", "mindfulness",
        "medication management", "group therapy", "family therapy", "couples therapy", "IOP", "PHP",
        "neuropsychological testing", "autism evaluation", "ABA", "biofeedback",
        # Sleep
        "Polysomnogram", "PSG", "MSLT", "MWT", "CPAP", "BiPAP", "Inspire", "dental appliance",
        # General procedures (examples)
        "ablation", "catheterization", "stent", "arthroscopy", "laser therapy", "orthotics"
    ]
    # Build regex with word boundaries where possible
    parts = []
    for kw in modality_keywords:
        if re.fullmatch(r"[A-Za-z\-]+", kw):
            parts.append(r"\b" + re.escape(kw) + r"\b")
        else:
            parts.append(re.escape(kw))
    pat = re.compile("|".join(parts), re.IGNORECASE)
    found: List[str] = []
    seen: set[str] = set()
    for p in pages:
        for line in (p.text or "").splitlines():
            s = line.strip()
            if not s or len(s) > 160:
                continue
            for m in pat.finditer(s):
                k = m.group(0)
                key = k.upper()
                if key not in seen:
                    found.append(k)
                    seen.add(key)
                    if len(found) >= max_samples:
                        return found
    return found

def build_evidence(pages: List[PagePayload]) -> Dict:
    """Build an evidence object to guide the model without post-normalization."""
    jsonld_locs = _collect_locations_from_jsonld(pages)
    text_locs = _collect_locations_from_text(pages)
    exact_count_text = _extract_exact_provider_count(pages)
    provider_names = collect_provider_name_candidates(pages, max_samples=40)
    specialties = collect_specialty_candidates(pages)
    modalities = collect_modality_candidates(pages)
    # JSON-LD numeric hints
    jsonld_counts: List[int] = []
    for p in pages:
        jd = p.jsonld
        if isinstance(jd, dict):
            for key in ("numberOfEmployees", "employeeCount", "employees", "staffCount"):
                v = jd.get(key)
                if isinstance(v, int) and 1 <= v <= 500:
                    jsonld_counts.append(v)
                elif isinstance(v, str):
                    digits = re.sub(r"[^0-9]", "", v)
                    if digits:
                        try:
                            n = int(digits)
                            if 1 <= n <= 500:
                                jsonld_counts.append(n)
                        except Exception:
                            pass
    evidence = {
        "candidate_locations_jsonld": list(dict.fromkeys(jsonld_locs))[:10],
        "candidate_locations_text": list(dict.fromkeys(text_locs))[:10],
        "exact_count_from_text": exact_count_text,
        "jsonld_numeric_counts": list(dict.fromkeys(jsonld_counts))[:10],
        "provider_name_candidates": provider_names,
        "candidate_specialties_jsonld": specialties.get("jsonld", []),
        "candidate_specialties_text": specialties.get("text", []),
        "candidate_modalities_text": modalities,
    }
    return evidence

# ----------------------------
# location normalization
# ----------------------------

def _normalize_state(region: str) -> Optional[str]:
    if not region:
        return None
    r = region.strip()
    if len(r) == 2 and r.isalpha():
        return r.upper()
    abbr = US_STATE_ABBR.get(r)
    if abbr:
        return abbr
    # Handle like "California (CA)"
    m = re.search(r"([A-Za-z ]+?)\s*\((\w{2})\)", r)
    if m:
        return m.group(2).upper()
    return None

def _collect_locations_from_jsonld(pages: List[PagePayload]) -> List[str]:
    found: List[str] = []
    for p in pages:
        jd = p.jsonld
        if not isinstance(jd, dict):
            continue
        addr = jd.get("address")
        addrs = []
        if isinstance(addr, dict):
            addrs = [addr]
        elif isinstance(addr, list):
            addrs = [a for a in addr if isinstance(a, dict)]
        for a in addrs:
            city = a.get("addressLocality") or a.get("locality")
            region = a.get("addressRegion") or a.get("region")
            if city and region:
                st = _normalize_state(str(region))
                if st:
                    found.append(f"{str(city).strip()}, {st}")
    return found

def _collect_locations_from_text(pages: List[PagePayload]) -> List[str]:
    # Match City, ST or City, StateName
    pat_city_state = re.compile(r"\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)\s*,\s*([A-Z]{2}|[A-Za-z][A-Za-z ]+)\b")
    # Provider-like tokens to exclude from location lines
    provider_tokens = re.compile(r"\b(Dr\.?|MD|DO|PhD|PsyD|NP|PA\-C|PA|RN|LCSW|LMFT|LPC|CNM|FNP|DNP|APRN|ARNP|BCBA|MSW|FRCS|FRCP|FACC)\b", re.IGNORECASE)
    found: List[str] = []
    for p in pages:
        text = (p.text or "")
        # Focus on shorter lines to reduce noise
        for line in text.splitlines():
            s = line.strip()
            if not s or len(s) > 120:
                continue
            # Skip lines that likely list providers or credentials
            if provider_tokens.search(s):
                continue
            for m in pat_city_state.finditer(s):
                city = m.group(1).strip()
                region = m.group(2).strip()
                st = _normalize_state(region) if len(region) != 2 else region.upper()
                if st:
                    # Exclude county-level only (e.g., "Marin County, CA")
                    if re.search(r"\bCounty\b", city):
                        continue
                    found.append(f"{city}, {st}")
    return found

def normalize_location_value(raw_value: str, pages: List[PagePayload]) -> str:
    raw = (raw_value or "").strip()
    # Prefer JSON-LD locations
    locs = _collect_locations_from_jsonld(pages)
    # Fallback to text-based city, state pairs
    if not locs:
        locs = _collect_locations_from_text(pages)
    if locs:
        # Deduplicate preserving order
        seen = set()
        ordered = []
        for l in locs:
            if l not in seen:
                seen.add(l)
                ordered.append(l)
        # Limit to a reasonable number to avoid overly long strings
        return "; ".join(ordered[:5])
    # If raw already looks like City, ST keep it; otherwise unknown
    if re.search(r"\b[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*\s*,\s*[A-Z]{2}\b", raw):
        return raw
    return "unknown"

# ----------------------------
# HTML utilities
# ----------------------------

REMOVE_TAGS = {
    "script", "style", "noscript", "svg", "img", "picture", "video",
    "iframe", "form", "button", "input", "select", "label",
    "header", "footer", "nav", "aside"
}

def visible_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # Drop unwanted tags
    for t in list(soup.find_all(REMOVE_TAGS)):
        t.decompose()

    # Remove cookie banners / obvious boilerplate by id/class hints
    hints = ["cookie", "gdpr", "newsletter", "subscribe", "signup", "breadcrumbs"]
    for hint in hints:
        for tag in soup.find_all(attrs={"id": re.compile(hint, re.I)}):
            tag.decompose()
        for tag in soup.find_all(attrs={"class": re.compile(hint, re.I)}):
            tag.decompose()

    # Extract stripped text
    text = "\n".join(s.strip() for s in soup.stripped_strings if s and len(s.strip()) > 1)
    # Collapse excessive newlines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text

def extract_jsonld(html: str) -> Optional[dict]:
    """Return first valid JSON-LD object if present; otherwise None."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    return None

# ----------------------------
# URL helpers
# ----------------------------

def same_domain(u: str, base: str) -> bool:
    a, b = urlparse(u).netloc.lower(), urlparse(base).netloc.lower()
    # strip leading 'www.'
    a = a[4:] if a.startswith("www.") else a
    b = b[4:] if b.startswith("www.") else b
    return a == b or a == ""  # relative URLs have empty netloc

def is_relevant_link(href: str, text: str) -> bool:
    if not href:
        return False
    href_l = href.lower()
    text_l = (text or "").lower()
    for term in RELEVANT_PATH_TERMS:
        if f"/{term}" in href_l or href_l.endswith(term) or term in text_l:
            return True
    return False

def link_priority_score(u: str) -> int:
    path = urlparse(u).path.lower()
    for i, term in enumerate(PRIORITY_ORDER):
        if f"/{term}" in path or path.endswith(term):
            return len(PRIORITY_ORDER) - i
    return 0

def should_visit_url(candidate_url: str, base_url: str) -> bool:
    if not candidate_url:
        return False
    if candidate_url.startswith("mailto:") or candidate_url.startswith("tel:"):
        return False
    parsed = urlparse(candidate_url)
    if parsed.scheme not in {"http", "https", ""}:
        return False
    if not same_domain(candidate_url, base_url):
        return False
    # Disallow assets by extension
    path = parsed.path.lower()
    for ext in DISALLOWED_EXTENSIONS:
        if path.endswith(ext):
            return False
    return True

def rank_links(links: List[str]) -> List[str]:
    return sorted(list(dict.fromkeys(links)), key=link_priority_score, reverse=True)

# ----------------------------
# Networking
# ----------------------------

def make_client(timeout: float = DEFAULT_TIMEOUT) -> httpx.Client:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    return httpx.Client(follow_redirects=True, timeout=timeout, headers=headers)

def make_async_client(timeout: float = DEFAULT_TIMEOUT) -> httpx.AsyncClient:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=10)
    return httpx.AsyncClient(follow_redirects=True, timeout=timeout, headers=headers, limits=limits, trust_env=True)

async def fetch_html(url: str, client: httpx.AsyncClient) -> Optional[str]:
    try:
        r = await client.get(url)
        if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
            return r.text
    except Exception:
        return None
    return None

async def discover_and_fetch(base_url: str, max_pages: int = DEFAULT_MAX_PAGES, max_depth: int = DEFAULT_MAX_DEPTH) -> List[PagePayload]:
    base_url = base_url.strip()
    if not base_url.startswith("http"):
        base_url = "https://" + base_url
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    pages: List[PagePayload] = []
    visited: set[str] = set()
    # (url, depth)
    queue: List[Tuple[str, int]] = [(base_url, 0)]

    async with make_async_client() as client:
        while queue and len(pages) < max_pages:
            current, depth = queue.pop(0)
            current = current.split("#")[0]
            if current in visited:
                continue
            visited.add(current)

            html = await fetch_html(current, client)
            if not html:
                continue

            pages.append(PagePayload(url=current, text=visible_text_from_html(html), jsonld=extract_jsonld(html)))
            if len(pages) >= max_pages:
                break

            if depth >= max_depth:
                continue

            # Discover more links from this page (rank broadly, don't restrict, but prefer relevant)
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                continue
            candidates: List[str] = []
            for a in soup.find_all("a"):
                href = a.get("href")
                if not href:
                    continue
                abs_url = urljoin(current, href)
                if should_visit_url(abs_url, base_url) and abs_url.startswith(root):
                    candidates.append(abs_url)

            ranked = rank_links(candidates)
            for u in ranked:
                u = u.split("#")[0]
                if u not in visited and all(u != q for q, _ in queue):
                    queue.append((u, depth + 1))

    return pages

# ----------------------------
# LLM adapters
# ----------------------------

def gemini_extract(pages: List[PagePayload], model: str = "gemini-2.5-pro") -> ExtractionResult:
    """Use Google GenAI SDK with response schema enforcement."""
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY (or GOOGLE_API_KEY) environment variable.")
    try:
        from google import genai
        from google.genai import types as gtypes
    except Exception as e:
        raise RuntimeError("google-genai SDK not installed. Run: pip install google-genai") from e

    client = genai.Client(api_key=api_key)
    pages_payload = [dataclasses.asdict(p) for p in pages]
    evidence = build_evidence(pages)

    # Load prompt text if available (optional)
    prompt_path = os.path.join(os.path.dirname(__file__), "AI_PROMPT.md")
    system_instruction = None
    if os.path.exists(prompt_path):
        with open(prompt_path, "r") as f:
            system_instruction = f.read().strip()

    schema = {
        "type": "OBJECT",
        "properties": {
            "clinic_info": {
                "type": "OBJECT",
                "properties": {
                    "specialty": {"type": "STRING"},
                    "modalities": {"type": "STRING"},
                    "location": {"type": "STRING"},
                    "clinic_size": {"type": "STRING"},
                },
                "required": ["specialty", "modalities", "location", "clinic_size"],
            }
        },
        "required": ["clinic_info"],
    }

    config = gtypes.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=schema,
        temperature=0,
        system_instruction=system_instruction,
    )

    contents = [
        "Extract the clinic_info JSON from the provided pages.",
        "Only return JSON.",
        json.dumps({"pages": pages_payload, "evidence": evidence})[:700000],  # Keep within context
    ]

    resp = client.models.generate_content(model=model, contents=contents, config=config)
    try:
        data = json.loads(resp.text)
        return ExtractionResult(**data)
    except Exception as e:
        raise RuntimeError(f"Gemini returned non-JSON or schema mismatch: {resp.text[:500]}") from e

def openai_extract(pages: List[PagePayload], model: str = "gpt-4o-mini") -> ExtractionResult:
    """Use OpenAI Responses API. Uses JSON mode (or Structured Outputs if supported)."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY environment variable.")
    try:
        from openai import OpenAI
    except Exception as e:
        raise RuntimeError("openai SDK not installed. Run: pip install openai") from e

    client = OpenAI(api_key=api_key)
    pages_payload = [dataclasses.asdict(p) for p in pages]
    evidence = build_evidence(pages)

    # Load prompt text if available (optional)
    prompt_path = os.path.join(os.path.dirname(__file__), "AI_PROMPT.md")
    system_instruction = None
    if os.path.exists(prompt_path):
        with open(prompt_path, "r") as f:
            system_instruction = f.read().strip()

    # Fallback to plain JSON mode (some snapshots support json_schema; we prefer Gemini for strictness)
    schema = {
        "name": "ClinicExtraction",
        "schema": {
            "type": "object",
            "properties": {
                "clinic_info": {
                    "type": "object",
                    "properties": {
                        "specialty": {"type": "string"},
                        "modalities": {"type": "string"},
                        "location": {"type": "string"},
                        "clinic_size": {"type": "string"},
                    },
                    "required": ["specialty", "modalities", "location", "clinic_size"],
                }
            },
            "required": ["clinic_info"],
            "additionalProperties": False,
        },
        "strict": True,
    }

    instructions = system_instruction or (
        "You are an information extraction engine. "
        "Return ONLY a JSON object that matches the provided schema. "
        "If unknown, use the string 'unknown'."
    )

    # Prefer structured outputs when available
    try:
        response = client.responses.create(
            model=model,
            instructions=instructions,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Extract clinic_info from these pages and return JSON only."},
                        {"type": "input_text", "text": json.dumps({"pages": pages_payload, "evidence": evidence})[:350000]},
                    ],
                }
            ],
            response_format={"type": "json_schema", "json_schema": schema},
        )
        text = response.output_text
    except Exception:
        # Fallback to simple JSON mode
        response = client.responses.create(
            model=model,
            instructions=instructions,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Extract clinic_info from these pages and return JSON only."},
                        {"type": "input_text", "text": json.dumps({"pages": pages_payload, "evidence": evidence})[:350000]},
                    ],
                }
            ],
            response_format={"type": "json_object"},
        )
        text = response.output_text

    try:
        return ExtractionResult(**json.loads(text))
    except Exception as e:
        raise RuntimeError(f"OpenAI returned non-JSON or schema mismatch: {text[:500]}") from e

def anthropic_extract(pages: List[PagePayload], model: str = "claude-3-5-sonnet-latest") -> ExtractionResult:
    """Use Anthropic Messages API. No hard schema guarantee; rely on prompt discipline."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("Missing ANTHROPIC_API_KEY environment variable.")
    try:
        import anthropic
    except Exception as e:
        raise RuntimeError("anthropic SDK not installed. Run: pip install anthropic") from e

    client = anthropic.Anthropic(api_key=api_key)
    pages_payload = [dataclasses.asdict(p) for p in pages]
    evidence = build_evidence(pages)

    # Load prompt text if available (optional)
    prompt_path = os.path.join(os.path.dirname(__file__), "AI_PROMPT.md")
    system_instruction = None
    if os.path.exists(prompt_path):
        with open(prompt_path, "r") as f:
            system_instruction = f.read().strip()

    prompt = system_instruction or (
        "You are an information extraction engine. "
        "Return ONLY valid JSON with this schema: "
        '{"clinic_info":{"specialty":"string","modalities":"string","location":"string","clinic_size":"string"}}. '
        "If unknown, use 'unknown'. No prose."
    )

    msg = client.messages.create(
        model=model,
        max_tokens=800,
        temperature=0,
        messages=[
            {"role": "user", "content": f"{prompt}\n\nPAGES+EVIDENCE:\n{json.dumps({'pages': pages_payload, 'evidence': evidence})[:350000]}"},
        ],
    )
    # Anthropic returns content blocks
    text = "".join([b.text for b in msg.content if b.type == "text"])
    try:
        return ExtractionResult(**json.loads(text))
    except Exception as e:
        raise RuntimeError(f"Anthropic returned non-JSON or schema mismatch: {text[:500]}") from e

# ----------------------------
# Orchestration
# ----------------------------

async def process_one(url: str, provider: str, model: str, max_pages: int, max_depth: int) -> Dict:
    pages = await discover_and_fetch(url, max_pages=max_pages, max_depth=max_depth)
    if not pages:
        raise RuntimeError(f"Could not fetch any HTML from {url}")
    if provider == "gemini":
        result = gemini_extract(pages, model=model)
    elif provider == "openai":
        result = openai_extract(pages, model=model)
    elif provider == "anthropic":
        result = anthropic_extract(pages, model=model)
    else:
        raise ValueError("Unsupported provider; use one of: gemini, openai, anthropic")
    # Let the model decide all fields using provided evidence and prompt
    return result.model_dump()

async def process_with_exhaustion(url: str, provider: str, model: str) -> Dict:
    """Fetch as many same-domain HTML pages as possible (up to a high cap) and extract once."""
    # We perform a wide crawl ignoring max_depth/page heuristics but respecting domain and file type.
    # Safety caps to avoid runaway crawls.
    max_pages_cap = 500
    base_url = url.strip()
    if not base_url.startswith("http"):
        base_url = "https://" + base_url
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    pages: List[PagePayload] = []
    visited: set[str] = set()
    queue: List[str] = [base_url]

    async with make_async_client() as client:
        while queue and len(pages) < max_pages_cap:
            current = queue.pop(0).split("#")[0]
            if current in visited:
                continue
            visited.add(current)
            html = await fetch_html(current, client)
            if not html:
                continue
            pages.append(PagePayload(url=current, text=visible_text_from_html(html), jsonld=extract_jsonld(html)))
            # Discover further links unbounded by depth but still ranked and filtered
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception:
                continue
            candidates: List[str] = []
            for a in soup.find_all("a"):
                href = a.get("href")
                if not href:
                    continue
                abs_url = urljoin(current, href)
                if should_visit_url(abs_url, base_url) and abs_url.startswith(root):
                    candidates.append(abs_url)
            for u in rank_links(candidates):
                u = u.split("#")[0]
                if u not in visited and u not in queue:
                    queue.append(u)

    if not pages:
        raise RuntimeError(f"Could not fetch any HTML from {url} in exhaustive mode")

    if provider == "gemini":
        result = gemini_extract(pages, model=model)
    elif provider == "openai":
        result = openai_extract(pages, model=model)
    elif provider == "anthropic":
        result = anthropic_extract(pages, model=model)
    else:
        raise ValueError("Unsupported provider; use one of: gemini, openai, anthropic")
    return result.model_dump()

def _unknown_fields(data: Dict) -> List[str]:
    try:
        ci = data.get("clinic_info", {})
        unknowns: List[str] = []
        for k in ("specialty", "modalities", "location", "clinic_size"):
            v = str(ci.get(k, "")).strip().lower()
            if (not v) or v == "unknown":
                unknowns.append(k)
        return unknowns
    except Exception:
        return ["specialty", "modalities", "location", "clinic_size"]

def write_jsonl(rows: List[Dict], path: str, pretty: bool = False) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for idx, r in enumerate(rows):
            if pretty:
                f.write(json.dumps(r, ensure_ascii=False, indent=2))
                f.write("\n")
                # Separate records by a newline for readability
                if idx < len(rows) - 1:
                    f.write("\n")
            else:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_csv(rows: List[Dict], path: str) -> None:
    # Flatten clinic_info into columns
    flat = []
    for r in rows:
        ci = r.get("clinic_info", {})
        flat.append({
            "specialty": ci.get("specialty", ""),
            "modalities": ci.get("modalities", ""),
            "location": ci.get("location", ""),
            "clinic_size": ci.get("clinic_size", ""),
        })
    fieldnames = ["specialty", "modalities", "location", "clinic_size"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in flat:
            w.writerow(row)

def write_json(rows: List[Dict], path: str) -> None:
    # Pretty JSON. If multiple rows, write a list; if single, write the single object.
    with open(path, "w", encoding="utf-8") as f:
        if len(rows) == 1:
            json.dump(rows[0], f, ensure_ascii=False, indent=2)
        else:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        f.write("\n")

def read_urls_from_csv(path: str) -> List[str]:
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "url" not in reader.fieldnames:
            raise ValueError("CSV must contain a 'url' column")
        for row in reader:
            u = (row.get("url") or "").strip()
            if u:
                urls.append(u)
    return urls

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="JotPsych — Clinic Intelligence Scraper")
    src = p.add_mutually_exclusive_group(required=False)
    src.add_argument("--url", help="Single clinic URL")
    src.add_argument("--input_csv", help="CSV file with a 'url' column")
    p.add_argument("--provider", default="gemini", choices=["gemini", "openai", "anthropic"], help="LLM provider")
    p.add_argument("--model", default=None, help="Model name (e.g., gemini-2.5-pro, gpt-4o-mini, claude-3-5-sonnet-latest)")
    p.add_argument("--max_pages", type=int, default=DEFAULT_MAX_PAGES, help="Max pages per site to consider")
    p.add_argument("--max_depth", type=int, default=DEFAULT_MAX_DEPTH, help="Depth from home to crawl (e.g., 2)")
    p.add_argument("--max_total_pages", type=int, default=120, help="Upper bound of pages to fetch when resolving unknowns")
    p.add_argument("--max_total_depth", type=int, default=3, help="Upper bound of depth when resolving unknowns")
    p.add_argument("--no_exhaust", action="store_true", help="Disable iterative crawling to resolve unknown fields")
    p.add_argument("--exhaust_all_if_unknown", action="store_true", help="If unknowns remain, crawl all same-domain HTML pages up to a high cap")
    p.add_argument("--out", default="results.jsonl", help="Output path (.jsonl, .json, or .csv)")
    p.add_argument("--pretty", action="store_true", help="Force pretty-print JSON outputs. Defaults to pretty for .json and .jsonl.")
    p.add_argument("--compact", action="store_true", help="Compact JSONL (one line per record). Overrides --pretty for .jsonl.")
    return p.parse_args()

def main() -> None:
    args = parse_args()

    # Interactive fallback if neither URL nor CSV is provided
    if not args.url and not args.input_csv:
        print("JotPsych Scraper — Interactive Mode")
        print("1) Single URL")
        print("2) CSV file of URLs (must have a 'url' column)")
        choice = input("Choose input type [1/2]: ").strip()
        while choice not in {"1", "2"}:
            choice = input("Please enter 1 or 2: ").strip()
        if choice == "1":
            url = input("Enter the clinic URL (e.g., https://example.com): ").strip()
            args.url = url
        else:
            csv_path = input("Enter path to CSV file: ").strip()
            if not os.path.exists(csv_path):
                print(f"[ERROR] File not found: {csv_path}", file=sys.stderr)
                sys.exit(1)
            args.input_csv = csv_path

    provider = args.provider
    if args.model:
        model = args.model
    else:
        model = {
            "gemini": "gemini-2.5-pro",
            "openai": "gpt-4o-mini",
            "anthropic": "claude-3-5-sonnet-latest",
        }[provider]

    urls = [args.url] if args.url else read_urls_from_csv(args.input_csv)

    rows: List[Dict] = []
    start = time.time()
    try:
        for u in urls:
            print(f"[INFO] Processing: {u}", file=sys.stderr)
            # Initial attempt
            data = asyncio.run(process_one(u, provider, model, args.max_pages, args.max_depth))
            unknowns = [] if args.no_exhaust else _unknown_fields(data)
            # Iteratively expand crawl if unknowns remain
            if not args.no_exhaust and unknowns:
                pages_budget = min(max(40, args.max_pages * 2), args.max_total_pages)
                depth_budget = max_depth = min(max(args.max_depth, 3), args.max_total_depth)
                # escalate pages first, then depth
                budgets: List[Tuple[int, int]] = []
                # Pages escalation steps
                p = args.max_pages
                while p < pages_budget:
                    p = min(p + args.max_pages, pages_budget)
                    budgets.append((p, args.max_depth))
                # Depth escalation
                if args.max_depth < depth_budget:
                    budgets.append((pages_budget, depth_budget))
                for pgs, dpt in budgets:
                    print(f"[INFO] Unknown fields {unknowns} — expanding crawl to pages={pgs}, depth={dpt}", file=sys.stderr)
                    data = asyncio.run(process_one(u, provider, model, pgs, dpt))
                    unknowns = _unknown_fields(data)
                    if not unknowns:
                        break
            # Exhaustive fallback if requested and unknowns remain
            if args.exhaust_all_if_unknown and unknowns:
                print(f"[INFO] Unknown fields {unknowns} — running exhaustive crawl", file=sys.stderr)
                data = asyncio.run(process_with_exhaustion(u, provider, model))
            rows.append(data)
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted by user.", file=sys.stderr)
    finally:
        elapsed = time.time() - start
        print(f"[INFO] Done in {elapsed:.1f}s — {len(rows)} result(s).", file=sys.stderr)

    out = args.out
    ol = out.lower()
    if ol.endswith(".csv"):
        write_csv(rows, out)
    elif ol.endswith(".json"):
        write_json(rows, out)
    else:
        pretty = True if not args.compact else False
        if args.pretty:
            pretty = True
        write_jsonl(rows, out, pretty=pretty)

    # Also print the last result to stdout for convenience (single URL UX)
    if len(rows) == 1:
        print(json.dumps(rows[0], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
