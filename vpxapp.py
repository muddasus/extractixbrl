# app.py
import re
import streamlit as st
from bs4 import BeautifulSoup, NavigableString, Tag
from collections import OrderedDict
from urllib.parse import urlparse
import streamlit.components.v1 as components
import requests
from requests.adapters import HTTPAdapter, Retry

# ------------------------------
# Defaults / Tunables (also editable in UI)
# ------------------------------
DEFAULT_GLOSSARY_HEADINGS = ["Special Terms", "Definitions", "Defined Terms", "Glossary"]
DEFAULT_APPX_HINTS = ["Appendix A", "Appendix", "Funds Available", "Available Under the Contract"]
NAV_ROLE_HINTS = ("navigation", "doc-index", "doc-toc")
TOC_CLASS_HINTS = ("toc", "table of contents")
HEADING_TAGS = {"h1","h2","h3","h4","h5","h6"}
SKIP_TAGS = {"a","script","style","nav","header","footer"}
RIDER_BRAND_HINTS = (
    "advantage", "lifetime income", "market select", "managed risk",
    "i4life", "4later", "guaranteed income", "american legacy", "variable annuity", "advisory"
)
PROPER_NOUN_TAILS = ("Fund", "Portfolio", "Series", "Index", "Trust")
EXCLUSION_PATTERNS = [
    r"\bClass\s+[A-Za-z0-9]+\b",      # "Class 4", "Class P2"
    r"\bService\s+Class\b",
    r"\bSeries\s+[A-Za-z0-9]+\b",
    r"\bTicker:\s*[A-Z]{2,6}\b",
    r"\bCUSIP\b",
]

# ------------------------------
# HTTP fetch (SEC-friendly)
# ------------------------------
def fetch_html(source: str, user_agent: str) -> str:
    parsed = urlparse(source)
    if parsed.scheme not in ("http", "https"):
        # Treat as raw HTML text if no scheme
        return source

    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))

    headers = {
        "User-Agent": user_agent or "MyCompany MyApp/1.0 (my.email@example.com)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    resp = s.get(source, headers=headers, timeout=25)
    if resp.status_code == 403:
        raise RuntimeError(
            "SEC returned 403 Forbidden. Provide a descriptive User-Agent with contact info "
            "(e.g., 'Org AppName/1.0 (you@domain.com)'), or download the HTML and use 'Upload HTML'."
        )
    resp.raise_for_status()
    resp.encoding = resp.encoding or resp.apparent_encoding
    return resp.text

# ------------------------------
# Parsing helpers
# ------------------------------
def looks_like_toc_or_nav(el: Tag) -> bool:
    role = (el.get("role") or "").lower()
    if any(h in role for h in NAV_ROLE_HINTS):
        return True
    classes = " ".join(el.get("class", [])).lower()
    if any(h in classes for h in TOC_CLASS_HINTS):
        return True
    return False

def find_headings(soup: BeautifulSoup, names_lower: tuple[str, ...]) -> list[Tag]:
    hits = []
    for h in soup.find_all(HEADING_TAGS):
        if (h.get_text(strip=True) or "").lower() in names_lower:
            hits.append(h)
    return hits

def nodes_until_next_heading(start: Tag) -> list:
    nodes = []
    if not start or start.name not in HEADING_TAGS:
        return nodes
    level = int(start.name[1])
    cur = start.next_sibling
    while cur:
        if isinstance(cur, Tag) and cur.name in HEADING_TAGS and int(cur.name[1]) <= level:
            break
        nodes.append(cur)
        cur = cur.next_sibling
    return nodes

def clean_lines_from_nodes(nodes: list) -> list[str]:
    lines = []
    for n in nodes:
        if not n:
            continue
        if isinstance(n, Tag):
            if looks_like_toc_or_nav(n) or n.name in SKIP_TAGS:
                continue
            txt = n.get_text(" ", strip=True)
        else:
            txt = str(n)
        if not txt:
            continue
        for line in txt.splitlines():
            s = line.strip()
            if s:
                lines.append(s)
    return lines

def extract_glossaries(soup: BeautifulSoup, glossary_headings: list[str]) -> list[dict]:
    results = []
    heads = find_headings(soup, tuple(h.lower() for h in glossary_headings))
    for h in heads:
        nodes = nodes_until_next_heading(h)
        lines = clean_lines_from_nodes(nodes)
        for line in lines:
            m = re.match(r"^(.+?)(?:\s*[—-]\s*)(.+)$", line)
            if not m:
                continue
            term = m.group(1).strip().rstrip(".:;")
            definition = m.group(2).strip()
            if len(term) >= 2 and len(definition) >= 5:
                results.append({"term": term, "definition": definition})
    # de-dup by lowercase term; keep longest definition
    dedup = OrderedDict()
    for item in results:
        k = item["term"].lower()
        if (k not in dedup) or (len(item["definition"]) > len(dedup[k]["definition"])):
            dedup[k] = item
    return list(dedup.values())

def extract_candidate_names_from_tables_and_lists(soup: BeautifulSoup) -> set[str]:
    names = set()
    def consider(text: str):
        t = text.strip()
        if not t:
            return
        if re.search(r"\b(?:Fund|Portfolio|Series|Index|Trust)\b", t) and len(t) <= 180:
            names.add(t)
        m = re.match(r"^(.*?)(?:\s*[–-]\s*Class\s+[A-Za-z0-9]+)\s*$", t)
        if m and len(m.group(1)) > 3:
            names.add(m.group(1).strip())
        m2 = re.match(r"^(.*?)(?:\s+Service\s+Class)\s*$", t)
        if m2 and len(m2.group(1)) > 3:
            names.add(m2.group(1).strip())
    for table in soup.find_all("table"):
        txt = table.get_text(" ", strip=True)
        if not txt: 
            continue
        for line in [x.strip() for x in txt.splitlines() if x.strip()]:
            consider(line)
    for ul in soup.find_all(["ul","ol"]):
        txt = ul.get_text(" ", strip=True)
        if not txt:
