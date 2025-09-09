# app.py
import re
import streamlit as st
from bs4 import BeautifulSoup, NavigableString, Tag
from collections import OrderedDict
from urllib.parse import urlparse
import streamlit.components.v1 as components
import requests
from requests.adapters import HTTPAdapter, Retry

# ---------- Defaults / Tunables ----------
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
    r"\bClass\s+[A-Za-z0-9]+\b",
    r"\bService\s+Class\b",
    r"\bSeries\s+[A-Za-z0-9]+\b",
    r"\bTicker:\s*[A-Z]{2,6}\b",
    r"\bCUSIP\b",
]
MAJOR_HEADING_HINTS = (
    "important information", "overview", "benefits available", "buying the contract",
    "withdrawals", "fee tables", "risks", "taxes", "conflicts of interest", "appendix",
)

# ---------- HTTP fetch (SEC-friendly) ----------
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

# ---------- Utilities ----------
def looks_like_toc_or_nav(el: Tag) -> bool:
    role = (el.get("role") or "").lower()
    if any(h in role for h in NAV_ROLE_HINTS): return True
    classes = " ".join(el.get("class", [])).lower()
    if any(h in classes for h in TOC_CLASS_HINTS): return True
    return False

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().rstrip(":").lower()

def find_headings_exact(soup: BeautifulSoup, names_lower: tuple[str, ...]) -> list[Tag]:
    hits = []
    for h in soup.find_all(HEADING_TAGS):
        if norm_text(h.get_text(strip=True)) in names_lower:
            hits.append(h)
    return hits

def is_heading_like(tag: Tag) -> bool:
    if not isinstance(tag, Tag): return False
    if tag.name in HEADING_TAGS: return True
    txt = (tag.get_text(" ", strip=True) or "")
    tnorm = norm_text(txt)
    if len(tnorm) < 5: return False
    # bold/strong/centered short text → headingish
    if (tag.find(["b","strong"]) or tag.name in {"center"}) and len(txt) <= 140:
        return True
    # obvious section titles
    if any(h in tnorm for h in MAJOR_HEADING_HINTS):
        return True
    # all-caps short lines
    if len(txt) <= 100 and txt == txt.upper():
        return True
    return False

def find_pseudo_heading(soup: BeautifulSoup, labels: list[str]) -> list[Tag]:
    labels_norm = set(norm_text(x) for x in labels)
    hits = []
    # 1) headings
    hits.extend(find_headings_exact(soup, tuple(labels_norm)))
    # 2) any element whose *own* text equals the label (common SEC pattern: <p><b>Special Terms</b></p>)
    for tag in soup.find_all(True):
        if tag.name in {"script","style"}: continue
        # prefer short blocks
        txt = tag.get_text(" ", strip=True)
        if not txt or len(txt) > 120: continue
        if norm_text(txt) in labels_norm:
            hits.append(tag)
    # 3) follow TOC anchor: <a href="#X">Special Terms</a> → element with id/name X
    for a in soup.find_all("a"):
        if norm_text(a.get_text(strip=True)) in labels_norm:
            href = a.get("href") or ""
            if href.startswith("#"):
                ident = href[1:]
                target = soup.find(id=ident) or soup.find(attrs={"name": ident})
                if target:
                    hits.append(target)
    # de-dup preserving order
    seen = set()
    out = []
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
            # stop at obvious heading-like blocks
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

# ---------- Glossary extraction (robust) ----------
def parse_table_term_defs(container: Tag) -> list[dict]:
    out = []
    for table in container.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) < 2: continue
            left = tds[0].get_text(" ", strip=True)
            right = " ".join(td.get_text(" ", strip=True) for td in tds[1:])
            term = left.strip(" .:;—-")
            definition = right.strip()
            if 2 <= len(term) <= 120 and len(definition) >= 5:
                out.append({"term": term, "definition": definition})
    return out

def parse_dl_term_defs(container: Tag) -> list[dict]:
    out = []
    for dl in container.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            term = dt.get_text(" ", strip=True).strip(" .:;—-")
            definition = dd.get_text(" ", strip=True)
            if 2 <= len(term) <= 120 and len(definition) >= 5:
                out.append({"term": term, "definition": definition})
    return out

def parse_inline_dash_defs(lines: list[str]) -> list[dict]:
    out = []
    # accept em dash, en dash, hyphen, or colon
    for line in lines:
        m = re.match(r"^(.+?)(?:\s*[—–-:]\s*)(.+)$", line)
        if not m: continue
        term = m.group(1).strip().rstrip(".:;")
        definition = m.group(2).strip()
        if 2 <= len(term) <= 120 and len(definition) >= 5:
            out.append({"term": term, "definition": definition})
    return out

def extract_glossaries(soup: BeautifulSoup, glossary_headings: list[str]) -> list[dict]:
    starts = find_pseudo_heading(soup, glossary_headings)
    results = []
    for start in starts:
        nodes = nodes_until_next_heading(start)
        if not nodes: continue
        # Build a transient container to run table/dl parsing
        tmp = BeautifulSoup("<div></div>", "lxml").div
        for n in nodes:
            if n: tmp.append(BeautifulSoup(str(n), "lxml"))
        # 1) tables
        results.extend(parse_table_term_defs(tmp))
        # 2) definition lists
        results.extend(parse_dl_term_defs(tmp))
        # 3) inline dash/colon lines
        results.extend(parse_inline_dash_defs(clean_lines_from_nodes(nodes)))

    # de-dup by lowercase term; prefer longest definition
    dedup = OrderedDict()
    for item in results:
        k = item["term"].lower()
        if (k not in dedup) or (len(item["definition"]) > len(dedup[k]["definition"])):
            dedup[k] = item
    return list(dedup.values())

# ---------- Dynamic exclusions ----------
def extract_candidate_names_from_tables_and_lists(soup: BeautifulSoup) -> set[str]:
    names = set()
    def consider(text: str):
        t = text.strip()
        if not t: return
        if re.search(r"\b(?:Fund|Portfolio|Series|Index|Trust)\b", t) and len(t) <= 200:
            names.add(t)
        m = re.match(r"^(.*?)(?:\s*[–—-]\s*Class\s+[A-Za-z0-9]+)\s*$", t)
        if m and len(m.group(1)) > 3:
            names.add(m.group(1).strip())
        m2 = re.match(r"^(.*?)(?:\s+Service\s+Class)\s*$", t)
        if m2 and len(m2.group(1)) > 3:
            names.add(m2.group(1).strip())

    for table in soup.find_all("table"):
        txt = table.get_text("\n", strip=True)
        for line in [x.strip() for x in txt.splitlines() if x.strip()]:
            consider(line)
    for ul in soup.find_all(["ul","ol"]):
        txt = ul.get_text("\n", strip=True)
        for line in [x.strip() for x in txt.splitlines() if x.strip()]:
            consider(line)
    return names

def extract_appendix_blocks(soup: BeautifulSoup, appendix_hints: list[str]) -> list[list[str]]:
    blocks = []
    for tag in soup.find_all(True):
        if tag.name in {"script","style"}: continue
        title = norm_text(tag.get_text(strip=True))
        if any(h.lower() in title for h in [*appendix_hints, "appendix b"]):
            # treat as section start if heading-like to avoid random paragraphs
            if is_heading_like(tag):
                nodes = nodes_until_next_heading(tag)
                lines = clean_lines_from_nodes(nodes)
                if lines: blocks.append(lines)
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
    # 1) Appendix-like blocks
    for lines in extract_appendix_blocks(soup, appendix_hints):
        for line in lines:
            m = re.match(r"^(.*?)(?:\s*[–—-]\s*Class\s+[A-Za-z0-9]+)\s*$", line)
            if m and len(m.group(1)) > 3:
                exc.add(m.group(1).strip().lower()); continue
            m2 = re.match(r"^(.*?)(?:\s+Service\s+Class)\s*$", line)
            if m2 and len(m2.group(1)) > 3:
                exc.add(m2.group(1).strip().lower()); continue
            if any(line.endswith(t) for t in PROPER_NOUN_TAILS):
                exc.add(line.strip().lower())
    # 2) Table/list sweep
    for name in extract_candidate_names_from_tables_and_lists(soup):
        exc.add(name.strip().lower())
    # 3) Rider/brand phrases
    for name in extract_rider_brand_candidates(soup):
        exc.add(name.strip().lower())
    return exc

# ---------- Rewriter ----------
def should_skip_element(el: Tag) -> bool:
    if not isinstance(el, Tag): return True
    if el.name in SKIP_TAGS: return True
    if looks_like_toc_or_nav(el): return True
    if el.name in HEADING_TAGS: return True
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
    glossary_heads = find_pseudo_heading(soup, glossary_headings)
    glossary_regions = set()
    for h in glossary_heads:
        for n in nodes_until_next_heading(h):
            glossary_regions.add(id(n))

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
        if low in exclusions: return True
        for pat in EXCLUSION_PATTERNS:
            if re.search(pat, text): return True
        if re.search(r"\b(?:Fund|Portfolio|Series|Index|Trust)\b", text):
            return True
        return False

    def replace_in_text(text: str) -> str:
        res = text
        for key, definition in sorted_terms:
            if occurrences[key] >= max_occurrences_per_term: continue
            if is_excluded_phrase(key): continue
            esc = re.escape(key)
            pattern = re.compile(rf"(?<!\w)({esc})(?!\w)", re.IGNORECASE)

            def _sub(m):
                original = m.group(1)
                if is_excluded_phrase(original): return original
                if occurrences[key] >= max_occurrences_per_term: return original
                span = (f'<span class="edgar-term" tabindex="0" '
                        f'data-term="{html_escape(original)}" '
                        f'data-def="{html_escape(definition)}">{original}</span>')
                occurrences[key] += 1
                return span

            res = pattern.sub(_sub, res)
        return res

    def walk(node: Tag):
        if not isinstance(node, Tag): return
        if id(node) in glossary_regions: return
        for child in list(node.children):
            if isinstance(child, NavigableString):
                parent: Tag = node
                if (not should_skip_element(parent)) and not parent.find_parent("a"):
                    new_html = replace_in_text(str(child))
                    if new_html != str(child):
                        frag = BeautifulSoup(new_html, "lxml")
                        child.replace_with(frag)
            elif isinstance(child, Tag):
                if should_skip_element(child): continue
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

# ---------- Streamlit UI ----------
st.set_page_config(page_title="EDGAR Special Terms Linker", layout="wide")
st.title("EDGAR Special Terms → Popover Linker (robust)")

with st.sidebar:
    st.header("Settings")
    max_occ = st.slider("Max occurrences per term (page-wide)", 1, 10, 3)

    glossary_input = st.text_area(
        "Glossary headings (one per line)",
        "\n".join(DEFAULT_GLOSSARY_HEADINGS),
        help="Headings or labels to locate the 'Special Terms' section. Case-insensitive."
    )
    glossary_heads = [h.strip() for h in glossary_input.splitlines() if h.strip()]

    appendix_input = st.text_area(
        "Appendix/fund section hints (one per line)",
        "\n".join(DEFAULT_APPX_HINTS),
        help="Used to build the dynamic exclusion list (funds/classes/series)."
    )
    appendix_hints = [h.strip() for h in appendix_input.splitlines() if h.strip()]

    st.markdown("---")
    ua_default = "YourOrg YourApp/1.0 (your.email@yourorg.com)"
    user_agent = st.text_input(
        "HTTP User-Agent for SEC (required for URL fetch)",
        value=ua_default,
        help="SEC requires a descriptive User-Agent with contact info."
    )

    st.markdown("---")
    mode = st.radio("Choose input mode", ["URL", "Upload HTML"], horizontal=True)
    url_val = st.text_input("SEC filing URL", placeholder="https://www.sec.gov/Archives/...") if mode == "URL" else None
    html_upload = st.file_uploader("Upload HTML file", type=["html", "htm"]) if mode != "URL" else None

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
            }, 120);
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
            st.dataframe([{"term": t["term"], "definition": t["definition"]} for t in terms],
                         use_container_width=True)
        else:
            st.error("No terms detected. Try adding heading aliases (e.g., 'SPECIAL TERMS') or use Upload HTML to verify structure.")

        st.subheader("Dynamic Exclusions (this filing)")
        st.caption("From appendix-like sections, tables/lists, and rider/brand phrases.")
        st.write(f"Total exclusions: **{len(exclusions)}**")
        if exclusions:
            st.dataframe([{"excluded": x} for x in exclusions], use_container_width=True)

    st.success("Done! Hover/focus the dotted terms in the preview to see popovers.")
else:
    st.info("Enter a URL or upload an HTML file, adjust settings, then click **Process filing**.")
