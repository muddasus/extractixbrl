# app_terms_only.py — Section-aware extractor + hyperlink + popover
import re
import io
import csv
import json
import html
import streamlit as st
import streamlit.components.v1 as components
from bs4 import BeautifulSoup, Tag, NavigableString
from collections import OrderedDict
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter, Retry

# =========================
# HTTP (SEC-friendly)
# =========================
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
        raise RuntimeError(
            "SEC returned 403 Forbidden. Provide a descriptive User-Agent with contact info, "
            "or download the HTML and use 'Upload HTML'."
        )
    r.raise_for_status()
    r.encoding = r.encoding or r.apparent_encoding
    return r.text

# =========================
# HTML → text with hard breaks (for parsing)
# =========================
def html_to_text_with_breaks(html_str: str) -> str:
    soup = BeautifulSoup(html_str, "lxml")
    root = soup.body or soup
    breakers = {"p","div","li","br","tr","td","th","dd","dt"}
    parts = []
    def walk(el):
        if isinstance(el, NavigableString):
            s = str(el)
            if s: parts.append(s)
            return
        if not isinstance(el, Tag): return
        if el.name in {"script","style"}: return
        for c in el.children: walk(c)
        if el.name in breakers: parts.append("\n")
    walk(root)
    txt = "".join(parts)
    txt = re.sub(r"\r", "", txt)
    txt = re.sub(r"[ \t\f\v]+\n", "\n", txt)
    txt = re.sub(r"\n{2,}", "\n", txt)
    return txt.strip()

# =========================
# Headings / Sectionization
# =========================
GOOD_SECTION_LABELS = {"special terms", "glossary", "definitions", "defined terms"}
BAD_SECTION_LABELS  = {"appendix", "appendix a", "appendix b", "appendix c", "table of contents"}

def is_heading_line(line: str) -> bool:
    s = line.strip()
    if not s or len(s) > 120: return False
    if s.endswith("."): return False
    if s == s.upper() and len(s.split()) <= 14:
        return True
    cap_like = sum(1 for w in s.split() if w[:1].isupper())
    if len(s.split()) <= 10 and cap_like >= max(2, len(s.split())//2):
        return True
    sn = s.lower()
    if any(k in sn for k in ["special terms", "glossary", "definitions", "defined terms",
                              "appendix", "table of contents"]):
        return True
    return False

def split_into_sections(lines: list[str]):
    heads = []
    for i, ln in enumerate(lines):
        if is_heading_line(ln):
            heads.append((i, ln.strip()))
    heads.append((len(lines), "__END__"))
    sections = []
    for k in range(len(heads)-1):
        i, title = heads[k]
        j, _ = heads[k+1]
        if j > i+1:
            sections.append((title, i, j, lines[i+1:j]))
    return sections

# =========================
# Parsing (dash / colon / dot styles)
# =========================
DASH_CHARS = "\u2014\u2013\u2212\u2010\u2011\u2012"  # em/en/minus/hyphen variants
DASH_CLASS = f"[{DASH_CHARS}:-]"

DASH_RE  = re.compile(rf"^\s*(.+?)\s*{DASH_CLASS}\s*(.+?)\s*$")
COLON_RE = re.compile(r"^\s*(.+?)\s*:\s*(.+?)\s*$")
DOT_RE   = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9()&'\/\-\s]{0,120}?)\.\s+(.*)$")

def try_match_term_line(line: str):
    m = DASH_RE.match(line)
    if m: return m.group(1).strip().rstrip(".:;"), m.group(2).strip()
    m = COLON_RE.match(line)
    if m: return m.group(1).strip().rstrip(".:;"), m.group(2).strip()
    m = DOT_RE.match(line)
    if m:
        term, defn = m.group(1).strip(), m.group(2).strip()
        words = term.split()
        cap_words = sum(1 for w in words if w[:1].isupper())
        if len(words) <= 8 or cap_words >= max(2, len(words)//2):
            return term, defn
    return None

def parse_accumulated(lines):
    items, cur_term, cur_def = [], None, []
    def flush():
        nonlocal cur_term, cur_def
        if cur_term:
            definition = " ".join(cur_def).strip()
            if 2 <= len(cur_term) <= 300 and len(definition) >= 5:
                items.append({"term": cur_term, "definition": definition})
        cur_term, cur_def = None, []
    for line in lines:
        if line in {"* * *", "***"}:
            flush()
            if items: break
            else: continue
        md = try_match_term_line(line)
        if md:
            flush()
            cur_term = md[0]
            cur_def = [md[1]]
        else:
            if cur_term:
                cur_def.append(line)
            else:
                continue
    flush()
    return items

def longest_term_run(all_lines, min_terms_in_run=4, max_gap=3):
    best = (0, 0, [])
    n = len(all_lines)
    i = 0
    while i < n:
        if not try_match_term_line(all_lines[i]):
            i += 1
            continue
        start = i
        j = i
        gaps = 0
        window_lines = []
        while j < n:
            ln = all_lines[j]
            window_lines.append(ln)
            if try_match_term_line(ln):
                gaps = 0
            else:
                gaps += 1
                if gaps > max_gap:
                    window_lines.pop()
                    break
            j += 1
        parsed = parse_accumulated(window_lines)
        if len(parsed) >= min_terms_in_run and len(parsed) > len(best[2]):
            best = (start, start + len(window_lines), parsed)
        i = max(j, i + 1)
    return best

# =========================
# Exclusions
# =========================
DEFAULT_EXCLUDE_TERMS = {
    "appendix", "appendix a", "appendix b", "appendix c",
    "administrative office",
    "table of contents",
}
EXCLUDE_PREFIXES = ("appendix ",)
EXCLUDE_REGEXES = [
    re.compile(r"^appendix\b", re.I),
    re.compile(r"^table of contents\b", re.I),
]

# Fund-name heuristics: words like Fund/Portfolio/Trust/Series/Index nearby
FUND_TOKENS = re.compile(r"\b(Fund|Portfolio|Trust|Series|Index|ETF|Variable Product Trust|VIP)\b", re.I)

def should_exclude_term(term: str, definition: str, extra_exclusions: set[str]) -> bool:
    t = (term or "").strip().lower()
    if not t: return True
    if t in DEFAULT_EXCLUDE_TERMS or t in extra_exclusions:
        return True
    if any(t.startswith(pfx) for pfx in EXCLUDE_PREFIXES):
        return True
    for rx in EXCLUDE_REGEXES:
        if rx.search(term): return True
    if re.search(r"\bP\.?\s*O\.?\s*Box\b", definition, re.I): return True
    if re.search(r"\bCustomer Service\b", definition, re.I): return True
    return False

# =========================
# Extraction: section-aware + fallback
# =========================
def extract_special_terms_section_aware(raw_html: str, extra_exclusions: set[str]):
    text = html_to_text_with_breaks(raw_html)
    if not text:
        return [], "", ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    sections = split_into_sections(lines)

    best_overall = ([], ("", 0, 0))
    for title, i, j, slice_lines in sections:
        title_norm = title.strip().lower()
        if any(lbl == title_norm for lbl in BAD_SECTION_LABELS) or any(title_norm.startswith(lbl) for lbl in BAD_SECTION_LABELS):
            continue
        good = any(lbl in title_norm for lbl in GOOD_SECTION_LABELS)

        start, end, items = longest_term_run(slice_lines, min_terms_in_run=3 if good else 5, max_gap=3)
        if items:
            items = [it for it in items if not should_exclude_term(it["term"], it["definition"], extra_exclusions)]
            # dedupe
            dedup = OrderedDict()
            for it in items:
                k = it["term"].strip().lower()
                v = it["definition"].strip()
                if not k or not v: continue
                if (k not in dedup) or (len(v) > len(dedup[k]["definition"])):
                    dedup[k] = {"term": it["term"].strip(), "definition": v}
            items = list(dedup.values())
            if items:
                take = False
                if not best_overall[0]:
                    take = True
                else:
                    prev_items, (prev_title, *_rest) = best_overall
                    prev_good = any(lbl in (prev_title or "").lower() for lbl in GOOD_SECTION_LABELS)
                    if good and not prev_good:
                        take = True
                    elif good == prev_good and len(items) > len(prev_items):
                        take = True
                if take:
                    best_overall = (items, (title, i+1+start, i+1+end))

    if best_overall[0]:
        items, (title, start_idx, end_idx) = best_overall
        debug_slice = "\n".join(lines[start_idx:end_idx])
        return items, title, debug_slice

    # global fallback
    start, end, items = longest_term_run(lines, min_terms_in_run=4, max_gap=3)
    if items:
        items = [it for it in items if not should_exclude_term(it["term"], it["definition"], extra_exclusions)]
        dedup = OrderedDict()
        for it in items:
            k = it["term"].strip().lower()
            v = it["definition"].strip()
            if not k or not v: continue
            if (k not in dedup) or (len(v) > len(dedup[k]["definition"])):
                dedup[k] = {"term": it["term"].strip(), "definition": v}
        items = list(dedup.values())
        return items, "(global fallback)", "\n".join(lines[start:end])

    return [], "", ""

# =========================
# Hyperlinking / Popover decorator
# =========================
EXCLUDED_ANCESTOR_TAGS = {"script","style","a","title","head"}
HEADING_TAGS = {"h1","h2","h3","h4","h5","h6"}

def is_heading_like_tag(tag: Tag) -> bool:
    if tag.name in HEADING_TAGS:
        return True
    # treat short, all-caps blocks as headings
    txt = (tag.get_text(" ", strip=True) or "")
    if txt and len(txt) <= 120 and txt == txt.upper():
        return True
    return False

def build_term_patterns(terms: list[dict]):
    # Sort by length desc to avoid partial shadowing (e.g., "Account" before "Account Value")
    sorted_terms = sorted(terms, key=lambda t: len(t["term"]), reverse=True)
    compiled = []
    for t in sorted_terms:
        term = t["term"]
        # word boundary-ish: allow punctuation around, but avoid mid-word matches
        pattern = re.compile(rf"(?<!\w)({re.escape(term)}) (?!\w)|(?<!\w)({re.escape(term)})(?!\w)", re.IGNORECASE)
        # The above handles cases with or without trailing space via two alternations; simpler match group use
        compiled.append((term, t["definition"], pattern))
    return compiled

def _should_skip_match_context(s: str, start: int, end: int) -> bool:
    """
    Simple fund-name context guard:
    - If the 3 words to the right OR left contain Fund/Portfolio/Trust/Series/Index, skip.
    """
    left_ctx  = s[max(0, start-80):start]
    right_ctx = s[end:min(len(s), end+80)]
    context = left_ctx + " " + right_ctx
    if FUND_TOKENS.search(context):
        return True
    return False

def decorate_html_with_terms(raw_html: str, terms: list[dict]) -> str:
    if not terms:
        return raw_html

    soup = BeautifulSoup(raw_html, "lxml")
    body = soup.body or soup
    patterns = build_term_patterns(terms)

    # Walk all text nodes, skipping headings/excluded containers
    text_nodes = []
    for el in body.find_all(text=True):
        parent = el.parent
        if not isinstance(parent, Tag): continue
        if parent.name in EXCLUDED_ANCESTOR_TAGS: continue
        # skip if inside a heading-like element
        heading_ancestor = False
        for anc in parent.parents:
            if isinstance(anc, Tag):
                if anc.name in EXCLUDED_ANCESTOR_TAGS: 
                    heading_ancestor = True; break
                if is_heading_like_tag(anc):
                    heading_ancestor = True; break
        if heading_ancestor: continue
        text_nodes.append(el)

    # Replace in each text node (marker strategy to avoid nested soups)
    MARKER_PREFIX = "\uFFF0TERM"
    marker_counter = 0

    for node in text_nodes:
        s = str(node)
        original = s
        replacements = []  # (marker, display_text, term_lower, definition)
        # For each pattern, replace with a unique marker via callback
        for term, definition, pat in patterns:
            def cb(m):
                nonlocal marker_counter
                span_text = m.group(0)
                # Determine exact matched display text (remove trailing alt)
                disp = m.group(1) or m.group(2) or span_text
                start = m.start()
                end = m.end()
                # Context test to avoid fund names
                if _should_skip_match_context(s, start, end):
                    return span_text
                marker = f"{MARKER_PREFIX}{marker_counter}\uFFF1"
                marker_counter += 1
                replacements.append((marker, disp, term.lower(), definition))
                return marker
            s = pat.sub(cb, s)
        if s != original and replacements:
            # Build HTML with markers replaced by spans
            for marker, disp, term_lower, definition in replacements:
                safe_disp = html.escape(disp)
                safe_def  = html.escape(definition)
                span_html = (f'<span class="st-term" tabindex="0" role="button" '
                             f'data-term="{safe_disp}" data-def="{safe_def}">{safe_disp}</span>')
                s = s.replace(marker, span_html)
            frag = BeautifulSoup(s, "lxml")
            node.replace_with(frag)

    # Inject CSS/JS for popovers
    inject_popover_assets(soup)
    return str(soup)

def inject_popover_assets(soup: BeautifulSoup):
    style = """
<style>
.st-term { text-decoration: underline dotted; cursor: pointer; }
.st-pop { position: absolute; z-index: 99999; max-width: 420px;
          background: #111827; color: #F9FAFB; border: 1px solid #374151;
          border-radius: 10px; padding: 12px 14px; box-shadow: 0 8px 30px rgba(0,0,0,.25); }
.st-pop .st-term-title { font-weight: 700; margin: 0 0 6px 0; font-size: 14px; }
.st-pop .st-term-def { font-size: 13px; line-height: 1.35; }
.st-pop .st-close { position: absolute; top: 6px; right: 8px; border: 0; background: transparent;
                    color: #9CA3AF; cursor: pointer; font-size: 14px; }
.st-pop .st-close:hover { color: #E5E7EB; }
</style>
"""
    script = """
<script>
(function(){
  let current;
  function closePop() {
    if (current && current.parentNode) current.parentNode.removeChild(current);
    current = null;
  }
  document.addEventListener('click', function(e){
    // Click outside → close
    if (current && !e.target.closest('.st-pop') && !e.target.classList.contains('st-term')) {
      closePop();
    }
  });
  document.addEventListener('keydown', function(e){
    if (e.key === 'Escape') closePop();
  });
  document.addEventListener('click', function(e){
    const el = e.target.closest('.st-term');
    if (!el) return;
    e.preventDefault();
    const term = el.getAttribute('data-term') || '';
    const def  = el.getAttribute('data-def') || '';
    closePop();

    const pop = document.createElement('div');
    pop.className = 'st-pop';
    pop.innerHTML = `
      <button class="st-close" aria-label="Close">✕</button>
      <div class="st-term-title">${term}</div>
      <div class="st-term-def">${def}</div>
    `;
    document.body.appendChild(pop);

    const rect = el.getBoundingClientRect();
    const popRect = pop.getBoundingClientRect();
    let top = window.scrollY + rect.bottom + 6;
    let left = window.scrollX + rect.left;
    if (left + popRect.width > window.scrollX + window.innerWidth - 12) {
      left = window.scrollX + window.innerWidth - popRect.width - 12;
    }
    if (top + popRect.height > window.scrollY + window.innerHeight - 12) {
      top = window.scrollY + rect.top - popRect.height - 6;
    }
    pop.style.top = top + 'px';
    pop.style.left = left + 'px';
    current = pop;

    pop.querySelector('.st-close').addEventListener('click', closePop);
  });
})();
</script>
"""
    head = soup.head or soup
    head.append(BeautifulSoup(style, "lxml"))
    head.append(BeautifulSoup(script, "lxml"))

# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="EDGAR — Special Terms with Popovers", layout="wide")
st.title("EDGAR — Hyperlink Special Terms with Popovers")

with st.sidebar:
    st.header("Input")
    ua_default = "YourOrg YourApp/1.0 (name@example.com)"
    user_agent = st.text_input("HTTP User-Agent (required for SEC URLs)", value=ua_default)
    mode = st.radio("Mode", ["URL", "Upload HTML"], horizontal=True)
    if mode == "URL":
        url_val = st.text_input("SEC filing URL", placeholder="https://www.sec.gov/Archives/...")
        run_btn = st.button("Process filing")
    else:
        html_file = st.file_uploader("Upload HTML", type=["html","htm"])
        run_btn = st.button("Process uploaded HTML")

    st.header("Exclusions")
    excl_text = st.text_area(
        "Extra terms to exclude from linking (one per line, case-insensitive)",
        "Administrative Office\nAppendix A\nAppendix B\nAppendix C",
        help="These will not be extracted OR linked."
    )
    extra_exclusions = {ln.strip().lower() for ln in excl_text.splitlines() if ln.strip()}

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

# ====== Extraction core (reused from your section-aware extractor) ======
def extract_terms_for_linking(html_str: str, extra_exclusions: set[str]) -> list[dict]:
    terms, where_title, _ = extract_special_terms_section_aware(html_str, extra_exclusions)
    return terms

if raw_html:
    with st.spinner("Extracting terms…"):
        terms = extract_terms_for_linking(raw_html, extra_exclusions)

    st.subheader("Extracted Special Terms")
    if terms:
        st.success(f"Found {len(terms)} terms.")
        st.dataframe([{"term": t["term"], "definition": t["definition"]} for t in terms],
                     use_container_width=True)

        # downloads
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["term", "definition"])
        for t in terms:
            writer.writerow([t["term"], t["definition"]])
        st.download_button("Download terms (CSV)",
                           data=csv_buf.getvalue().encode("utf-8"),
                           file_name="special_terms.csv", mime="text/csv")
        st.download_button("Download terms (JSON)",
                           data=json.dumps(terms, ensure_ascii=False, indent=2).encode("utf-8"),
                           file_name="special_terms.json", mime="application/json")

        with st.spinner("Decorating HTML with popovers…"):
            decorated_html = decorate_html_with_terms(raw_html, terms)

        st.subheader("Filing (terms hyperlinked)")
        # Render full HTML with JS/CSS
        components.html(decorated_html, height=900, scrolling=True)
    else:
        st.error("No terms found to link. Try adjusting exclusions or check the debug slice in your earlier run.")
else:
    st.info("Enter a URL or upload an HTML file, then click **Process filing**.")
