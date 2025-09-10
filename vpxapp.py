# app_terms_only.py — Special Terms extractor (robust for SEC "plain text" sections)
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

# Section/heading cues common in these filings
MAJOR_HEADING_HINTS = (
    "important information", "overview of the contract", "benefits available",
    "buying the contract", "making withdrawals", "additional information about fees",
    "fee tables", "risks", "taxes", "conflicts of interest", "appendix", "table of contents",
)

# Safe dash characters for "Term — Definition"
# em (—), en (–), minus (−), hyphen (‐), non-breaking hyphen (-). ASCII '-' and ':' included separately.
DASH_CHARS = "\u2014\u2013\u2212\u2010\u2011"
DASH_CLASS = f"[{DASH_CHARS}:-]"  # ASCII '-' at end to avoid ranges

# ---------------- HTTP (SEC-friendly) ----------------
def fetch_html(source: str, user_agent: str) -> str:
    parsed = urlparse(source)
    if parsed.scheme not in ("http", "https"):
        return source  # treat as raw HTML

    s = requests.Session()
    retries = Retry(
        total=3, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))

    headers = {
        "User-Agent": user_agent or "YourOrg YourApp/1.0 (you@yourorg.com)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    }
    r = s.get(source, headers=headers, timeout=25)
    if r.status_code == 403:
        raise RuntimeError(
            "SEC returned 403 Forbidden. Provide a descriptive User-Agent with contact info, "
            "or download the HTML and use 'Upload HTML'."
        )
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
    # Treat short bold/centered/all-caps blocks as headings
    txt = (tag.get_text(" ", strip=True) or "")
    tnorm = norm_text(txt)
    if len(tnorm) < 3: return False
    if (tag.find(["b","strong"]) or tag.name in {"center"}) and len(txt) <= 160:
        return True
    if any(h in tnorm for h in MAJOR_HEADING_HINTS):
        return True
    if len(txt) <= 120 and txt == txt.upper():
        return True
    return False

def nodes_until_next_heading(start: Tag) -> list:
    """Collect next siblings after 'start' until a heading-like block or a hard separator."""
    nodes = []
    if not isinstance(start, Tag): return nodes
    cur = start.next_sibling
    while cur:
        if isinstance(cur, Tag):
            # stop at explicit headings or heading-like blocks
            if cur.name in HEADING_TAGS or is_heading_like(cur):
                break
            # stop at section separators like horizontal rules or star-lines
            if cur.name == "hr":
                break
            if looks_like_toc_or_nav(cur):
                break
        # also break on ornament separators that appear alone in their line
        text = cur.get_text(" ", strip=True) if isinstance(cur, Tag) else str(cur).strip()
        if text.strip() in {"* * *", "***"}:
            break
        nodes.append(cur)
        cur = cur.next_sibling
    return nodes

def clean_lines_from_nodes(nodes: list) -> list[str]:
    lines = []
    for n in nodes:
        if not n: continue
        if isinstance(n, Tag):
            if looks_like_toc_or_nav(n) or n.name in SKIP_TAGS: continue
            txt = n.get_text("\n", strip=True)
        else:
            txt = str(n)
        if not txt: continue
        for line in txt.splitlines():
            s = line.strip()
            if s and s not in {"* * *", "***"}:
                lines.append(s)
    return lines

# ---------------- Parsers ----------------
def parse_table_term_defs(container: Tag) -> list[dict]:
    out = []
    for table in container.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) < 2:
                continue
            left = tds[0].get_text(" ", strip=True)
            right = " ".join(td.get_text(" ", strip=True) for td in tds[1:])
            term = (left or "").strip(f" .:;{DASH_CHARS}-")
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
            term = (dt.get_text(" ", strip=True) or "").strip(f" .:;{DASH_CHARS}-")
            definition = (dd.get_text(" ", strip=True) or "").strip()
            if 2 <= len(term) <= 200 and len(definition) >= 5:
                out.append({"term": term, "definition": definition})
    return out

def parse_inline_defs(lines: list[str]) -> list[dict]:
    out = []
    # Accept em/en/minus/nb-hyphen/ASCII '-' or colon
    pattern = re.compile(rf"^(.+?)(?:\s*{DASH_CLASS}\s*)(.+)$")
    for line in lines:
        m = pattern.match(line)
        if not m:
            continue
        term = m.group(1).strip().rstrip(".:;")
        definition = m.group(2).strip()
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
def find_special_terms_anchor(soup: BeautifulSoup, headings: list[str]) -> Tag | None:
    # 1) exact text node "Special Terms"
    for node in soup.find_all(string=re.compile(r"^\s*Special\s+Terms\s*$", re.I)):
        if isinstance(node, NavigableString) and isinstance(node.parent, Tag):
            return node.parent
    # 2) any element whose own text is exactly "Special Terms"
    for tag in soup.find_all(True):
        if tag.name in SKIP_TAGS: 
            continue
        if norm_text(tag.get_text(" ", strip=True)) == "special terms":
            return tag
    # 3) TOC anchors <a href="#something">Special Terms</a>
    for a in soup.find_all("a"):
        if norm_text(a.get_text(strip=True)) == "special terms":
            href = (a.get("href") or "")
            if href.startswith("#"):
                ident = href[1:]
                target = soup.find(id=ident) or soup.find(attrs={"name": ident})
                if target:
                    return target
    # 4) fallback: search for recognized headings list
    labels_norm = set(norm_text(x) for x in headings)
    for tag in soup.find_all(True):
        if tag.name in SKIP_TAGS: continue
        if norm_text(tag.get_text(" ", strip=True)) in labels_norm:
            return tag
    return None

def extract_special_terms(raw_html: str, headings: list[str]) -> tuple[list[dict], str]:
    """
    Returns (terms, debug_html_slice)
    """
    soup = BeautifulSoup(raw_html, "lxml")
    anchor = find_special_terms_anchor(soup, headings)
    debug_slice_html = ""

    if anchor:
        # collect following siblings until next heading-like block
        nodes = nodes_until_next_heading(anchor)
        if nodes:
            section_html = "".join(str(n) for n in nodes if n is not None)
            debug_slice_html = section_html  # for preview
            tmp_soup = BeautifulSoup(f"<div id='tmp'>{section_html}</div>", "lxml")
            tmp = tmp_soup.find(id="tmp")

            results = []
            results.extend(parse_table_term_defs(tmp))
            results.extend(parse_dl_term_defs(tmp))
            results.extend(parse_inline_defs(clean_lines_from_nodes(nodes)))
            if results:
                return dedup_terms(results), debug_slice_html

    # last resort: text-window between "Special Terms" and next major heading keyword
    full_text = (soup.body or soup).get_text("\n", strip=True)
    m_start = re.search(r"\bSpecial\s+Terms\b", full_text, flags=re.I)
    if m_start:
        m_stop = re.search(r"\b(Important Information|Overview of the Contract|RISKS|Fee Tables|Appendix)\b", 
                           full_text[m_start.end():], flags=re.I)
        window = full_text[m_start.end(): m_start.end() + m_stop.start()] if m_stop else full_text[m_start.end():]
        lines = [l.strip() for l in window.splitlines() if l.strip() and l.strip() not in {"* * *","***"}]
        results = parse_inline_defs(lines)
        if results:
            return dedup_terms(results), "<pre>" + "\n".join(lines[:200]) + "</pre>"

    return [], debug_slice_html

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="EDGAR Special Terms (Terms-Only)", layout="wide")
st.title("EDGAR — Extract Special Terms & Definitions")

with st.sidebar:
    st.header("Input")
    ua_default = "YourOrg YourApp/1.0 (your.email@yourorg.com)"
    user_agent = st.text_input("HTTP User-Agent (required for SEC URLs)", value=ua_default)
    mode = st.radio("Mode", ["URL", "Upload HTML"], horizontal=True)

    if mode == "URL":
        url_val = st.text_input("SEC filing URL", placeholder="https://www.sec.gov/Archives/...")
        run_btn = st.button("Extract terms")
    else:
        html_file = st.file_uploader("Upload HTML", type=["html","htm"])
        run_btn = st.button("Extract terms from upload")

    st.header("Detection")
    head_input = st.text_area(
        "Glossary/Terms headings (one per line)",
        "\n".join(DEFAULT_GLOSSARY_HEADINGS),
        help="Labels used to find the Special Terms section."
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

        # CSV (safe quoting)
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["term", "definition"])
        for t in terms:
            writer.writerow([t["term"], t["definition"]])
        st.download_button(
            "Download terms (CSV)",
            data=csv_buf.getvalue().encode("utf-8"),
            file_name="special_terms.csv",
            mime="text/csv"
        )
        # JSON
        st.download_button(
            "Download terms (JSON)",
            data=json.dumps(terms, ensure_ascii=False, indent=2).encode("utf-8"),
            file_name="special_terms.json",
            mime="application/json"
        )
    else:
        st.error("No Special Terms detected. See the debug slice below to confirm what was captured.")

    # Debug preview of the captured slice under "Special Terms"
    st.markdown("#### Debug: Captured 'Special Terms' slice (HTML)")
    if debug_slice:
        st.code(debug_slice[:5000], language="html")
    else:
        st.caption("No slice captured (the anchor might not have been found).")

else:
    st.info("Enter a URL or upload an HTML file, then click **Extract terms**.")
