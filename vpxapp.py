# app_terms_only.py — Section-aware Special Terms / Glossary extractor with exclusions
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
        raise RuntimeError(
            "SEC returned 403 Forbidden. Provide a descriptive User-Agent with contact info, "
            "or download the HTML and use 'Upload HTML'."
        )
    r.raise_for_status()
    r.encoding = r.encoding or r.apparent_encoding
    return r.text

# ---------------- Text conversion ----------------
def html_to_text_with_breaks(html: str) -> str:
    """Convert body HTML to plain text with hard breaks at p/br/li/tr/td/th/dt/dd."""
    soup = BeautifulSoup(html, "lxml")
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

# ---------------- Headings / Sections ----------------
GOOD_SECTION_LABELS = {
    "special terms", "glossary", "definitions", "defined terms"
}
BAD_SECTION_LABELS = {
    "appendix", "appendix a", "appendix b", "appendix c", "table of contents"
}

def is_heading_line(line: str) -> bool:
    """Heuristic: short, title-like line (no trailing period), often Title Case or ALL CAPS."""
    s = line.strip()
    if not s or len(s) > 120: return False
    if s.endswith("."): return False
    # many headings are short (<= 8–10 words)
    if len(s.split()) <= 10:
        # looks like title or all caps
        if s == s.upper():
            return True
        # Title-ish (Most Words Capitalized)
        cap_like = sum(1 for w in s.split() if w[:1].isupper())
        if cap_like >= max(2, len(s.split())//2):
            return True
    # obvious keywords
    sn = s.lower()
    if any(k in sn for k in ["special terms", "glossary", "definitions", "defined terms",
                              "appendix", "table of contents"]):
        return True
    return False

def split_into_sections(lines: list[str]):
    """
    Split the document into (heading_text, start_idx, end_idx, lines_slice).
    A 'heading' is a line that looks heading-like by heuristic.
    """
    # collect heading indices
    heads = []
    for i, ln in enumerate(lines):
        if is_heading_line(ln):
            heads.append((i, ln.strip()))
    # add sentinel end
    heads.append((len(lines), "__END__"))
    sections = []
    for k in range(len(heads)-1):
        i, title = heads[k]
        j, _ = heads[k+1]
        # section content starts after the heading line
        if j > i+1:
            sections.append((title, i, j, lines[i+1:j]))
    return sections

# ---------------- Parsing ----------------
# Unicode dashes: em (—), en (–), minus (−), hyphen (‐), nb-hyphen (-). Place ASCII '-' at end.
DASH_CHARS = "\u2014\u2013\u2212\u2010\u2011\u2012"
DASH_CLASS = f"[{DASH_CHARS}:-]"

DASH_RE  = re.compile(rf"^\s*(.+?)\s*{DASH_CLASS}\s*(.+?)\s*$")
COLON_RE = re.compile(r"^\s*(.+?)\s*:\s*(.+?)\s*$")
# Dot style: “Term. Definition”
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
    """Stateful parser: new term starts on a recognized line; following lines join the definition until next term."""
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
    """
    Find the longest contiguous run that yields term/definition pairs.
    Allow up to 'max_gap' non-matching lines between term lines (for wrapped definitions).
    """
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

# ---------------- Exclusions ----------------
DEFAULT_EXCLUDE_TERMS = {
    "appendix", "appendix a", "appendix b", "appendix c",
    "administrative office",  # noisy address-style glossary items
    "table of contents",
}
EXCLUDE_PREFIXES = ("appendix ",)  # e.g., "Appendix A", "Appendix B"
EXCLUDE_REGEXES = [
    re.compile(r"^appendix\b", re.I),
    re.compile(r"^table of contents\b", re.I),
]

def should_exclude_term(term: str, definition: str, extra_exclusions: set[str]) -> bool:
    t = (term or "").strip().lower()
    if not t: return True
    if t in DEFAULT_EXCLUDE_TERMS or t in extra_exclusions:
        return True
    if any(t.startswith(pfx) for pfx in EXCLUDE_PREFIXES):
        return True
    for rx in EXCLUDE_REGEXES:
        if rx.search(term): return True
    # simple address heuristics (avoid PO Boxes / admin addresses)
    if re.search(r"\bP\.?\s*O\.?\s*Box\b", definition, re.I): return True
    if re.search(r"\bCustomer Service\b", definition, re.I): return True
    return False

# ---------------- Extraction (section-aware + fallback) ----------------
def extract_special_terms_section_aware(raw_html: str, extra_exclusions: set[str]):
    """
    1) Convert entire body to line-broken text.
    2) Split into sections using heading-like heuristics.
    3) Prefer sections with GOOD_SECTION_LABELS; skip BAD_SECTION_LABELS.
    4) Inside each candidate section, take the longest term run.
    5) If none found, fallback to global longest run.
    6) Apply exclusions; dedupe; return items and debug info.
    """
    text = html_to_text_with_breaks(raw_html)
    if not text:
        return [], "", ""

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    sections = split_into_sections(lines)

    best_overall = ([], ("", 0, 0))  # (items, (title, start_idx, end_idx))
    for title, i, j, slice_lines in sections:
        title_norm = title.strip().lower()
        # Skip bad sections outright
        if any(lbl == title_norm for lbl in BAD_SECTION_LABELS) or any(title_norm.startswith(lbl) for lbl in BAD_SECTION_LABELS):
            continue
        # Prefer only good headings; if not good, still allow but with lower priority
        good = any(lbl in title_norm for lbl in GOOD_SECTION_LABELS)

        start, end, items = longest_term_run(slice_lines, min_terms_in_run=3 if good else 5, max_gap=3)
        if items:
            # filter & dedupe
            items = [it for it in items if not should_exclude_term(it["term"], it["definition"], extra_exclusions)]
            dedup = OrderedDict()
            for it in items:
                k = it["term"].strip().lower()
                v = it["definition"].strip()
                if not k or not v: continue
                if (k not in dedup) or (len(v) > len(dedup[k]["definition"])):
                    dedup[k] = {"term": it["term"].strip(), "definition": v}
            items = list(dedup.values())
            if items:
                # prefer good sections; otherwise choose the one with more items
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
                    best_overall = (items, (title, i+1+start, i+1+end))  # map back to global indices

    if best_overall[0]:
        items, (title, start_idx, end_idx) = best_overall
        debug_slice = "\n".join(lines[start_idx:end_idx])
        return items, title, debug_slice

    # ---- Fallback: global longest run across the entire doc ----
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

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="EDGAR Special Terms / Glossary Extractor", layout="wide")
st.title("EDGAR — Extract Special Terms & Definitions (Section-aware)")

with st.sidebar:
    st.header("Input")
    ua_default = "YourOrg YourApp/1.0 (name@example.com)"
    user_agent = st.text_input("HTTP User-Agent (required for SEC URLs)", value=ua_default)
    mode = st.radio("Mode", ["URL", "Upload HTML"], horizontal=True)
    if mode == "URL":
        url_val = st.text_input("SEC filing URL", placeholder="https://www.sec.gov/Archives/...")
        run_btn = st.button("Extract terms")
    else:
        html_file = st.file_uploader("Upload HTML", type=["html","htm"])
        run_btn = st.button("Extract terms from upload")

    st.header("Filters")
    excl_text = st.text_area(
        "Extra terms to exclude (one per line, case-insensitive)",
        "Administrative Office\nAppendix A\nAppendix B\nAppendix C",
        help="These terms will be dropped even if they parse as glossary entries."
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

if raw_html:
    with st.spinner("Finding the Special Terms / Glossary block…"):
        terms, where_title, debug_text = extract_special_terms_section_aware(raw_html, extra_exclusions)

    st.subheader("Results")
    if terms:
        st.success(f"Found {len(terms)} terms from section: **{where_title or 'N/A'}**")
        st.dataframe([{"term": t["term"], "definition": t["definition"]} for t in terms],
                     use_container_width=True)

        # CSV
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["term", "definition"])
        for t in terms:
            writer.writerow([t["term"], t["definition"]])
        st.download_button("Download terms (CSV)",
                           data=csv_buf.getvalue().encode("utf-8"),
                           file_name="special_terms.csv",
                           mime="text/csv")

        # JSON
        st.download_button("Download terms (JSON)",
                           data=json.dumps(terms, ensure_ascii=False, indent=2).encode("utf-8"),
                           file_name="special_terms.json",
                           mime="application/json")
    else:
        st.error("No Special Terms / Glossary block detected. See the debug slice below.")

    st.markdown("#### Debug: Detected block (text)")
    if debug_text:
        st.code(debug_text[:8000], language="text")
    else:
        st.caption("No contiguous run of term/definition lines found.")
else:
    st.info("Enter a URL or upload an HTML file, then click **Extract terms**.")
