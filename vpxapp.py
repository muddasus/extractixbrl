# app_terms_only.py (fixed)
import re
import streamlit as st
from bs4 import BeautifulSoup, Tag
from collections import OrderedDict
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter, Retry

# --- Defaults ---
DEFAULT_GLOSSARY_HEADINGS = ["Special Terms", "Definitions", "Defined Terms", "Glossary"]
HEADING_TAGS = {"h1","h2","h3","h4","h5","h6"}
SKIP_TAGS = {"script","style"}
NAV_ROLE_HINTS = ("navigation", "doc-index", "doc-toc")
TOC_CLASS_HINTS = ("toc", "table of contents")
MAJOR_HEADING_HINTS = (
    "important information", "overview", "benefits available", "fee tables",
    "risks", "taxes", "conflicts of interest", "appendix", "buying the contract",
)

# --- HTTP fetch with SEC-friendly UA ---
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

# --- Small helpers ---
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
    if (tag.find(["b","strong"]) or tag.name in {"center"}) and len(txt) <= 140:
        return True
    if any(h in tnorm for h in MAJOR_HEADING_HINTS):
        return True
    if len(txt) <= 100 and txt == txt.upper():
        return True
    return False

def find_pseudo_heading(soup: BeautifulSoup, labels: list[str]) -> list[Tag]:
    labels_norm = set(norm_text(x) for x in labels)
    hits = []
    # true headings
    for h in soup.find_all(HEADING_TAGS):
        if norm_text(h.get_text(strip=True)) in labels_norm:
            hits.append(h)
    # bold/centered short blocks like <p><b>Special Terms</b></p>
    for tag in soup.find_all(True):
        if tag.name in SKIP_TAGS: continue
        txt = tag.get_text(" ", strip=True)
        if not txt or len(txt) > 160: continue
        if norm_text(txt) in labels_norm:
            hits.append(tag)
    # TOC anchors
    for a in soup.find_all("a"):
        if norm_text(a.get_text(strip=True)) in labels_norm:
            href = (a.get("href") or "")
            if href.startswith("#"):
                ident = href[1:]
                target = soup.find(id=ident) or soup.find(attrs={"name": ident})
                if target:
                    hits.append(target)
    # de-dup
    seen, out = set(), []
    for h in hits:
        key = id(h)
        if key not in seen:
            out.append(h); seen.add(key)
    return out

def nodes_until_next_heading(start: Tag) -> list:
    nodes = []
    if not isinstance(start, Tag): return nodes
    level = int(start.name[1]) if start.name in HEADING_TAGS else None
    cur = start.next_sibling
    while cur:
        if isinstance(cur, Tag):
            if cur.name in HEADING_TAGS and (level is None or int(cur.name[1]) <= level):
                break
            if is_heading_like(cur): break
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
            if s: lines.append(s)
    return lines

# --- Parsers for different glossary layouts ---
def parse_table_term_defs(container: Tag) -> list[dict]:
    out = []
    for table in container.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) < 2:
                continue
            left = tds[0].get_text(" ", strip=True)
            right = " ".join(td.get_text(" ", strip=True) for td in tds[1:])
            term = (left or "").strip(" .:;—–-")
            definition = (right or "").strip()
            if 2 <= len(term) <= 120 and len(definition) >= 5:
                out.append({"term": term, "definition": definition})
    return out

def parse_dl_term_defs(container: Tag) -> list[dict]:
    out = []
    for dl in container.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):  # zip keeps us safe if counts differ
            term = (dt.get_text(" ", strip=True) or "").strip(" .:;—–-")
            definition = (dd.get_text(" ", strip=True) or "").strip()
            if 2 <= len(term) <= 120 and len(definition) >= 5:
                out.append({"term": term, "definition": definition})
    return out

def parse_inline_defs(lines: list[str]) -> list[dict]:
    out = []
    # accept em dash, en dash, hyphen, or colon
    for line in lines:
        m = re.match(r"^(.+?)(?:\s*[—–-:]\s*)(.+)$", line)
        if not m: 
            continue
        term = m.group(1).strip().rstrip(".:;")
        definition = m.group(2).strip()
        if 2 <= len(term) <= 120 and len(definition) >= 5:
            out.append({"term": term, "definition": definition})
    return out

def extract_special_terms(raw_html: str, headings: list[str]) -> list[dict]:
    soup = BeautifulSoup(raw_html, "lxml")
    starts = find_pseudo_heading(soup, headings)
    results = []
    for start in starts:
        nodes = nodes_until_next_heading(start)
        if not nodes:
            continue
        # *** FIX: build one HTML string and parse once (no per-node append of BS objects) ***
        section_html = "".join(str(n) for n in nodes if n is not None)
        if not section_html.strip():
            continue
        tmp_soup = BeautifulSoup(f"<div id='tmp'>{section_html}</div>", "lxml")
        tmp = tmp_soup.find(id="tmp")

        # Try multiple formats
        results.extend(parse_table_term_defs(tmp))
        results.extend(parse_dl_term_defs(tmp))
        results.extend(parse_inline_defs(clean_lines_from_nodes(nodes)))

    # de-dup by lowercase term; prefer longest definition
    dedup = OrderedDict()
    for item in results:
        k = item["term"].lower()
        if (k not in dedup) or (len(item["definition"]) > len(dedup[k]["definition"])):
            dedup[k] = item
    return list(dedup.values())

# --- Streamlit UI (terms-only) ---
st.set_page_config(page_title="EDGAR Special Terms (Terms-Only)", layout="wide")
st.title("EDGAR — Extract Special Terms & Definitions")

with st.sidebar:
    st.header("Input")
    ua_default = "YourOrg YourApp/1.0 (your.email@yourorg.com)"
    user_agent = st.text_input("HTTP User-Agent (required for SEC URLs)", value=ua_default)
    mode = st.radio("Mode", ["URL", "Upload HTML"], horizontal=True)

    url_val = None
    html_file = None
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
        terms = extract_special_terms(raw_html, glossary_heads)

    st.subheader("Results")
    if terms:
        st.success(f"Found {len(terms)} terms.")
        st.dataframe([{"term": t["term"], "definition": t["definition"]} for t in terms],
                     use_container_width=True)
        st.download_button(
            "Download terms (CSV)",
            data=("term,definition\n" + "\n".join(
                f"\"{t['term'].replace('\"','\"\"')}\",\"{t['definition'].replace('\"','\"\"')}\""
                for t in terms
            )).encode("utf-8"),
            file_name="special_terms.csv",
            mime="text/csv"
        )
    else:
        st.warning("No Special Terms detected. Try adding more heading aliases (e.g., 'SPECIAL TERMS', 'GLOSSARY').")

else:
    st.info("Enter a URL or upload an HTML file, then click **Extract terms**.")
