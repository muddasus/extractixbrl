# app_terms_only.py — SEC "Special Terms" extractor (robust & tested)
import re
import io
import csv
import json
import streamlit as st
from bs4 import BeautifulSoup, Tag, NavigableString
from collections import OrderedDict
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter, Retry

# ---------------- Defaults ----------------
DEFAULT_GLOSSARY_HEADINGS = ["Special Terms", "Definitions", "Defined Terms", "Glossary"]
HEADING_TAGS = {"h1","h2","h3","h4","h5","h6"}
SKIP_TAGS = {"script","style"}
NAV_ROLE_HINTS = ("navigation", "doc-index", "doc-toc")
TOC_CLASS_HINTS = ("toc", "table of contents")

MAJOR_HEADING_HINTS = (
    "important information", "overview of the contract", "benefits available",
    "buying the contract", "making withdrawals", "additional information about fees",
    "fee tables", "risks", "taxes", "conflicts of interest", "appendix", "table of contents",
)

# Unicode dashes: em (—), en (–), minus (−), hyphen (‐), non-breaking hyphen (-).
DASH_CHARS = "\u2014\u2013\u2212\u2010\u2011"
DASH_CLASS = f"[{DASH_CHARS}:-]"  # ASCII '-' placed at end to avoid regex range

# ---------------- HTTP (SEC-friendly) ----------------
def fetch_html(source: str, user_agent: str) -> str:
    parsed = urlparse(source)
    if parsed.scheme not in ("http", "https"):
        return source  # treat as raw HTML

    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET", "HEAD", "OPTIONS"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))

    headers = {
        "User-Agent": user_agent or "YourOrg YourApp/1.0 (name@example.com)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    }
    r = s.get(source, headers=headers, timeout=25)
    if r.status_code == 403:
        raise RuntimeError("SEC returned 403 (Forbidden). Use a descriptive User-Agent with contact info, "
                           "or download the HTML and use 'Upload HTML'.")
    r.raise_for_status()
    r.encoding = r.encoding or r.apparent_encoding
    return r.text

# ---------------- Helpers ----------------
def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().rstrip(":").lower()

def looks_like_toc_or_nav(el: Tag) -> bool:
    role = (el.get("role") or "").lower()
    if any(h in role for h in NAV_ROLE_HINTS): return True
    classes = " ".join(el.get("class", [])).lower()
    if any(h in classes for h in TOC_CLASS_HINTS): return True
    return False

def is_heading_like(tag: Tag) -> bool:
    if not isinstance(tag, Tag): return False
    if tag.name in HEADING_TAGS: return True
    txt = (tag.get_text(" ", strip=True) or "")
    tnorm = norm_text(txt)
    if len(tnorm) < 3: return False
    if (tag.find(["b","strong"]) or tag.name in {"center"}) and len(txt) <= 200:
        return True
    if any(h in tnorm for h in MAJOR_HEADING_HINTS):
        return True
    if len(txt) <= 140 and txt == txt.upper():
        return True
    return False

def nodes_until_next_heading(start: Tag) -> list:
    """Collect next siblings after 'start' until heading-like block or separators."""
    nodes = []
    if not isinstance(start, Tag): return nodes
    cur = start.next_sibling
    while cur:
        if isinstance(cur, Tag):
            if cur.name in HEADING_TAGS or is_heading_like(cur) or looks_like_toc_or_nav(cur):
                break
            if cur.name == "hr":
                break
        text = cur.get_text(" ", strip=True) if isinstance(cur, Tag) else str(cur).strip()
        if text in {"* * *", "***"}:
            break
        nodes.append(cur)
        cur = cur.next_sibling
    return nodes

def html_slice_to_text_with_breaks(nodes: list) -> str:
    """Turn a node slice into text with hard breaks at p/br/li/td/th/tr/dt/dd."""
    section_html = "".join(str(n) for n in nodes if n is not None)
    tmp = BeautifulSoup(section_html, "lxml")

    breakers = {"p","div","li","br","tr","td","th","dd","dt"}
    parts = []
    def walk(el):
        if isinstance(el, NavigableString):
            if str(el):
                parts.append(str(el))
            return
        if not isinstance(el, Tag): return
        for c in el.children:
            walk(c)
        if el.name in breakers:
            parts.append("\n")
    walk(tmp)
    txt = "".join(parts)
    txt = re.sub(r"\r", "", txt)
    txt = re.sub(r"[ \t\f\v]+\n", "\n", txt)
    txt = re.sub(r"\n{2,}", "\n", txt)
    return txt.strip()

# ---------------- Parsers ----------------
TERM_SPLIT_RE = re.compile(rf"^\s*(.+?)\s*(?:{DASH_CLASS})\s*(.+?)\s*$")

def parse_inline_block_accum(text_block: str) -> list[dict]:
    """Stateful 'Term — Definition' parser that glues wrapped lines until next term."""
    lines = [l.strip() for l in text_block.splitlines() if l.strip() and l.strip() not in {"* * *","***"}]
    items = []
    cur_term = None
    cur_def = []

    def flush():
        nonlocal cur_term, cur_def
        if cur_term:
            definition = " ".join(cur_def).strip()
            if 2 <= len(cur_term) <= 200 and len(definition) >= 5:
                items.append({"term": cur_term, "definition": definition})
        cur_term, cur_def = None, []

    for line in lines:
        m = TERM_SPLIT_RE.match(line)
        if m:
            flush()
            cur_term = m.group(1).strip().rstrip(".:;")
            cur_def = [m.group(2).strip()]
        else:
            if cur_term:
                cur_def.append(line)
            else:
                # ignore preamble lines
                continue
    flush()
    return items

def parse_table_term_defs(container: Tag) -> list[dict]:
    out = []
    for table in container.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) < 2: continue
            left = tds[0].get_text(" ", strip=True)
            right = " ".join(td.get_text(" ", strip=True) for td in tds[1:])
            term = (left or "").strip()
            definition = (right or "").strip()
            if 2 <= len(term) <= 200 and len(definition) >= 5:
                out.append({"term": term, "definition": definition})
    return out

def parse_dl_term_defs(container: Tag) -> list[dict]:
    out = []
    for dl in container.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            term = (dt.get_text(" ", strip=True) or "").strip()
            definition = (dd.get_text(" ", strip=True) or "").strip()
            if 2 <= len(term) <= 200 and len(definition) >= 5:
                out.append({"term": term, "definition": definition})
    return out

def dedup_terms(items: list[dict]) -> list[dict]:
    dedup = OrderedDict()
    for item in items:
        t = (item.get("term") or "").strip()
        d = (item.get("definition") or "").strip()
        if not t or not d: 
            continue
        k = t.lower()
        if (k not in dedup) or (len(d) > len(dedup[k]["definition"])):
            dedup[k] = {"term": t, "definition": d}
    return list(dedup.values())

# ---------------- Extraction core ----------------
def find_sentinel_anchor(soup: BeautifulSoup) -> Tag | None:
    # The filing uses: “the following terms have the indicated meanings:”
    sentinel = soup.find(string=re.compile(r"following terms have the indicated meanings", re.I))
    if isinstance(sentinel, NavigableString) and isinstance(sentinel.parent, Tag):
        return sentinel.parent
    return None

def best_special_terms_anchor(soup: BeautifulSoup, headings: list[str]) -> Tag | None:
    # Prefer the sentinel sentence; fall back to closest “Special Terms” that yields pairs.
    anchor = find_sentinel_anchor(soup)
    if anchor:
        return anchor

    labels = set(norm_text(x) for x in headings) | {"special terms"}
    candidates = []
    for tag in soup.find_all(True):
        if tag.name in SKIP_TAGS: 
            continue
        text = norm_text(tag.get_text(" ", strip=True))
        if text in labels and not looks_like_toc_or_nav(tag):
            candidates.append(tag)

    # Score candidates by how many term pairs appear in their slice
    def score_anchor(tag: Tag) -> int:
        nodes = nodes_until_next_heading(tag)
        if not nodes: return 0
        text_block = html_slice_to_text_with_breaks(nodes)
        return sum(1 for ln in text_block.splitlines() if TERM_SPLIT_RE.match(ln))

    if candidates:
        candidates.sort(key=score_anchor, reverse=True)
        if score_anchor(candidates[0]) > 0:
            return candidates[0]
    return None

def extract_special_terms(raw_html: str, headings: list[str]) -> tuple[list[dict], str]:
    """Returns (terms, debug_html_slice)."""
    soup = BeautifulSoup(raw_html, "lxml")
    anchor = best_special_terms_anchor(soup, headings)
    debug_slice_html = ""

    if anchor:
        nodes = nodes_until_next_heading(anchor)
        if nodes:
            debug_slice_html = "".join(str(n) for n in nodes if n is not None)
            tmp = BeautifulSoup(f"<div id='tmp'>{debug_slice_html}</div>", "lxml").find(id="tmp")

            results = []
            # Structural formats (rare in this filing but supported)
            results.extend(parse_table_term_defs(tmp))
            results.extend(parse_dl_term_defs(tmp))
            # Primary inline accumulator
            text_block = html_slice_to_text_with_breaks(nodes)
            results.extend(parse_inline_block_accum(text_block))
            if results:
                return dedup_terms(results), debug_slice_html

    # Last-resort: text window between “Special Terms” and next major heading keyword
    full_text = (soup.body or soup).get_text("\n", strip=True)
    m_start = re.search(r"\bSpecial\s+Terms\b", full_text, flags=re.I)
    if m_start:
        m_stop = re.search(r"\b(Important Information|Overview of the Contract|RISKS|Fee Tables|Appendix)\b",
                           full_text[m_start.end():], flags=re.I)
        window = full_text[m_start.end(): m_start.end() + m_stop.start()] if m_stop else full_text[m_start.end():]
        results = parse_inline_block_accum(window)
        if results:
            return dedup_terms(results), "<pre>" + window[:4000] + "</pre>"

    return [], debug_slice_html

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="EDGAR Special Terms (Terms-Only)", layout="wide")
st.title("EDGAR — Extract Special Terms & Definitions")

with st.sidebar:
    st.header("Input")
    ua_default = "YourOrg YourApp/1.0 (name@example.com)"
    user_agent = st.text_input("HTTP User-Agent (required for SEC URLs)", value=ua_default)
    mode = st.radio("Mode", ["URL", "Upload HTML"], horizontal=True)

    if mode == "URL":
        url_val = st.text_input("SEC filing URL", value="", placeholder="https://www.sec.gov/Archives/...")
        run_btn = st.button("Extract terms")
    else:
        html_file = st.file_uploader("Upload HTML", type=["html","htm"])
        run_btn = st.button("Extract terms from upload")

    st.header("Detection")
    head_input = st.text_area(
        "Glossary/Terms headings (one per line)",
        "\n".join(DEFAULT_GLOSSARY_HEADINGS),
        help="Labels used to find the Special Terms section (fallback)."
    )
    glossary_heads = [h.strip() for h in head_input.splitlines() if h.strip()]

raw_html = None
if run_btn:
    try:
        if mode == "URL":
            if not url_val:
                st.error("Please provide a URL.")
            else:
                raw_html = fetch_html(url_val, user_agent=user_agent)
        else:
            if not html_file:
                st.error("Please upload an HTML file.")
            else:
                raw_html = html_file.read().decode("utf-8", errors="ignore")
    except Exception as e:
        st.error(f"Failed to load HTML: {e}")

if raw_html:
    with st.spinner("Scanning for Special Terms…"):
        terms, debug_slice = extract_special_terms(raw_html, glossary_heads)

    st.subheader("Results")
    if terms:
        st.success(f"Found {len(terms)} terms.")
        st.dataframe([{"term": t["term"], "definition": t["definition"]} for t in terms],
                     use_container_width=True)

        # CSV download
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["term", "definition"])
        for t in terms:
            writer.writerow([t["term"], t["definition"]])
        st.download_button("Download terms (CSV)",
                           data=csv_buf.getvalue().encode("utf-8"),
                           file_name="special_terms.csv", mime="text/csv")

        # JSON download
        st.download_button("Download terms (JSON)",
                           data=json.dumps(terms, ensure_ascii=False, indent=2).encode("utf-8"),
                           file_name="special_terms.json", mime="application/json")
    else:
        st.error("No Special Terms detected. See the debug slice below to confirm what was captured.")

    st.markdown("#### Debug: Captured 'Special Terms' slice (HTML)")
    if debug_slice:
        st.code(debug_slice[:6000], language="html")
    else:
        st.caption("No slice captured (anchor not found).")
else:
    st.info("Enter a URL or upload an HTML file, then click **Extract terms**.")
