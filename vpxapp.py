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
            continue
        for line in [x.strip() for x in txt.splitlines() if x.strip()]:
            consider(line)
    return names

def extract_appendix_blocks(soup: BeautifulSoup, appendix_hints: list[str]) -> list[list[str]]:
    blocks = []
    for h in soup.find_all(HEADING_TAGS):
        title = (h.get_text(strip=True) or "").lower()
        if any(k.lower() in title for k in appendix_hints):
            nodes = nodes_until_next_heading(h)
            lines = clean_lines_from_nodes(nodes)
            if lines:
                blocks.append(lines)
    return blocks

def extract_rider_brand_candidates(soup: BeautifulSoup) -> set[str]:
    cands = set()
    body_text = (soup.body or soup).get_text(" ", strip=True)
    for m in re.finditer(r"\b([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z0-9&()/-]+){1,7})\b", body_text):
        phrase = m.group(1).strip()
        low = phrase.lower()
        if any(h in low for h in RIDER_BRAND_HINTS):
            cands.add(phrase)
    return cands

def build_dynamic_exclusions(soup: BeautifulSoup, appendix_hints: list[str]) -> set[str]:
    exc = set()
    for lines in extract_appendix_blocks(soup, appendix_hints):
        for line in lines:
            m = re.match(r"^(.*?)(?:\s*[–-]\s*Class\s+[A-Za-z0-9]+)\s*$", line)
            if m and len(m.group(1)) > 3:
                exc.add(m.group(1).strip().lower()); continue
            m2 = re.match(r"^(.*?)(?:\s+Service\s+Class)\s*$", line)
            if m2 and len(m2.group(1)) > 3:
                exc.add(m2.group(1).strip().lower()); continue
            if any(line.endswith(t) for t in PROPER_NOUN_TAILS):
                exc.add(line.strip().lower())
    for name in extract_candidate_names_from_tables_and_lists(soup):
        exc.add(name.strip().lower())
    for name in extract_rider_brand_candidates(soup):
        exc.add(name.strip().lower())
    return exc

def should_skip_element(el: Tag) -> bool:
    if not isinstance(el, Tag):
        return True
    if el.name in SKIP_TAGS:
        return True
    if looks_like_toc_or_nav(el):
        return True
    if el.name in HEADING_TAGS:
        return True
    return False

def html_escape(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))

def hyperlink_with_terms(
    soup: BeautifulSoup,
    terms: list[dict],
    max_occurrences_per_term: int,
    glossary_headings: list[str],
    appendix_hints: list[str]
) -> BeautifulSoup:

    # Exclude glossary sections from linking
    glossary_heads = find_headings(soup, tuple(h.lower() for h in glossary_headings))
    glossary_regions = set()
    for h in glossary_heads:
        for n in nodes_until_next_heading(h):
            glossary_regions.add(id(n))

    # Build dynamic exclusions from THIS filing
    exclusions = build_dynamic_exclusions(soup, appendix_hints)

    # Sort terms longest-first; dedup key
    by_key = OrderedDict()
    for item in terms:
        key = item["term"].strip().lower()
        if key and key not in by_key:
            by_key[key] = item["definition"].strip()
    sorted_terms = sorted(by_key.items(), key=lambda kv: len(kv[0]), reverse=True)
    occurrences = {k: 0 for k in by_key.keys()}

    def is_excluded_phrase(text: str) -> bool:
        low = text.lower().strip()
        if low in exclusions:
            return True
        for pat in EXCLUSION_PATTERNS:
            if re.search(pat, text):
                return True
        if re.search(r"\b(?:Fund|Portfolio|Series|Index|Trust)\b", text):
            return True
        return False

    def replace_in_text(text: str) -> str:
        res = text
        for key, definition in sorted_terms:
            if occurrences[key] >= max_occurrences_per_term:
                continue
            if is_excluded_phrase(key):
                continue
            esc = re.escape(key)
            pattern = re.compile(rf"(?<!\w)({esc})(?!\w)", re.IGNORECASE)

            def _sub(m):
                original = m.group(1)
                if is_excluded_phrase(original):
                    return original
                if occurrences[key] >= max_occurrences_per_term:
                    return original
                span = (f'<span class="edgar-term" tabindex="0" '
                        f'data-term="{html_escape(original)}" '
                        f'data-def="{html_escape(definition)}">{original}</span>')
                occurrences[key] += 1
                return span

            res = pattern.sub(_sub, res)
        return res

    def walk(node: Tag):
        if not isinstance(node, Tag):
            return
        if id(node) in glossary_regions:
            return
        for child in list(node.children):
            if isinstance(child, NavigableString):
                parent: Tag = node
                if (not should_skip_element(parent)) and not parent.find_parent("a"):
                    new_html = replace_in_text(str(child))
                    if new_html != str(child):
                        frag = BeautifulSoup(new_html, "lxml")
                        child.replace_with(frag)
            elif isinstance(child, Tag):
                if should_skip_element(child):
                    continue
                walk(child)

    body = soup.body or soup
    walk(body)
    return soup

def decorate_html(raw_html: str, max_occ: int, glossary_headings: list[str], appendix_hints: list[str]):
    soup = BeautifulSoup(raw_html, "lxml")
    terms = extract_glossaries(soup, glossary_headings)
    exclusions = build_dynamic_exclusions(soup, appendix_hints)
    if not terms:
        return raw_html, terms, sorted(exclusions)
    soup = hyperlink_with_terms(soup, terms, max_occ, glossary_headings, appendix_hints)
    return str(soup), terms, sorted(exclusions)

# ------------------------------
# Streamlit UI
# ------------------------------
st.set_page_config(page_title="EDGAR Special Terms Linker", layout="wide")
st.title("EDGAR Special Terms → Popover Linker")

with st.sidebar:
    st.header("Settings")
    max_occ = st.slider("Max occurrences per term (page-wide)", 1, 10, 3)

    glossary_input = st.text_area(
        "Glossary headings (one per line)",
        "\n".join(DEFAULT_GLOSSARY_HEADINGS),
        help="Headings to look for when extracting Special Terms/Definitions."
    )
    glossary_heads = [h.strip() for h in glossary_input.splitlines() if h.strip()]

    appendix_input = st.text_area(
        "Appendix/fund section hints (one per line)",
        "\n".join(DEFAULT_APPX_HINTS),
        help="Headings to scan when building dynamic exclusions (fund names/classes/etc.)."
    )
    appendix_hints = [h.strip() for h in appendix_input.splitlines() if h.strip()]

    st.markdown("---")
    ua_default = "YourOrg YourApp/1.0 (your.email@yourorg.com)"
    user_agent = st.text_input(
        "HTTP User-Agent for SEC (required for URL fetch)",
        value=ua_default,
        help="SEC requires a descriptive User-Agent with contact info (email or domain)."
    )

    st.markdown("---")
    mode = st.radio("Choose input mode", ["URL", "Upload HTML"], horizontal=True)
    url_val = None
    html_upload = None
    if mode == "URL":
        url_val = st.text_input("SEC filing URL", placeholder="https://www.sec.gov/Archives/...")
    else:
        html_upload = st.file_uploader("Upload HTML file", type=["html", "htm"])

    run_btn = st.button("Process filing")

raw_html = None
if run_btn:
    try:
        if mode == "URL":
            if not url_val:
                st.error("Please provide a URL.")
            else:
                raw_html = fetch_html(url_val, user_agent=user_agent)
        else:
            if not html_upload:
                st.error("Please upload an HTML file.")
            else:
                raw_html = html_upload.read().decode("utf-8", errors="ignore")
    except Exception as e:
        st.error(f"Failed to load HTML: {e}")

if raw_html:
    with st.spinner("Extracting terms, building dynamic exclusions, and decorating HTML..."):
        out_html, terms, exclusions = decorate_html(raw_html, max_occ, glossary_heads, appendix_hints)

    col1, col2 = st.columns([2,1])

    with col1:
        st.subheader("Preview (decorated HTML)")

        # Inject minimal popover initializer so preview is interactive
        popover_bootstrap = """
        <script src="https://unpkg.com/@popperjs/core@2"></script>
        <script src="https://unpkg.com/tippy.js@6"></script>
        <link rel="stylesheet" href="https://unpkg.com/tippy.js@6/animations/shift-away.css"/>
        <style>
          .edgar-term { border-bottom: 1px dotted; cursor: help; }
          .tippy-box { max-width: 520px; }
        </style>
        <script>
          document.addEventListener('DOMContentLoaded', () => {
            setTimeout(() => {
              document.querySelectorAll('.edgar-term').forEach(el => {
                if (el._tippy) return;
                tippy(el, {
                  content: (() => {
                    const term = el.getAttribute('data-term');
                    const def  = el.getAttribute('data-def');
                    const d = document.createElement('div');
                    d.innerHTML = `<strong>${term}</strong><div style="margin-top:.25rem;line-height:1.35">${def}</div>`;
                    return d;
                  })(),
                  allowHTML: true, interactive: true,
                  trigger: 'mouseenter focus', placement: 'top',
                  animation: 'shift-away', maxWidth: 520, delay: [50,0]
                });
              });
            }, 100);
          });
        </script>
        """
        components.html(popover_bootstrap + out_html, height=820, scrolling=True)

        st.download_button(
            "Download decorated HTML",
            data=out_html.encode("utf-8"),
            file_name="decorated_filing.html",
            mime="text/html"
        )

    with col2:
        st.subheader("Extracted Special Terms")
        if terms:
            st.write(f"Found **{len(terms)}** terms from glossary-like sections.")
            st.dataframe(
                [{"term": t["term"], "definition": t["definition"]} for t in terms],
                use_container_width=True
            )
        else:
            st.info("No glossary-like sections detected (Special Terms / Definitions / Glossary).")

        st.subheader("Dynamic Exclusions (this filing)")
        st.caption("Derived from appendix-like sections, tables/lists, and rider/brand phrases.")
        st.write(f"Total exclusions: **{len(exclusions)}**")
        if exclusions:
            st.dataframe([{"excluded": x} for x in exclusions], use_container_width=True)

    st.success("Done! Hover/focus the dotted terms in the preview to see popovers.")
else:
    st.info("Enter a URL or upload an HTML file, adjust settings, then click **Process filing**.")
