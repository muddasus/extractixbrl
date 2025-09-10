# app_terms_only.py — Anchorless Special Terms / Glossary extractor (dash/colon/dot styles)
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

# ---------------- Parsing ----------------
# Unicode dashes: em (—), en (–), minus (−), hyphen (‐), nb-hyphen (-). Place ASCII '-' at end.
DASH_CHARS = "\u2014\u2013\u2212\u2010\u2011\u2012"
DASH_CLASS = f"[{DASH_CHARS}:-]"

# term line matchers
DASH_RE  = re.compile(rf"^\s*(.+?)\s*{DASH_CLASS}\s*(.+?)\s*$")
COLON_RE = re.compile(r"^\s*(.+?)\s*:\s*(.+?)\s*$")

# For the “Term. Definition” style, keep it strict enough to avoid ordinary sentences.
# Heuristics: term starts with a letter/number, <= 8 words, may include ()&/'- characters, ends with a dot, followed by a space.
DOT_RE   = re.compile(
    r"^\s*([A-Za-z0-9][A-Za-z0-9()&'\/\-\s]{0,120}?)\.\s+(.*)$"
)

def try_match_term_line(line: str):
    """Return (term, definition) if the line is a term-definition start; else None."""
    m = DASH_RE.match(line)
    if m:
        return m.group(1).strip().rstrip(".:;"), m.group(2).strip()
    m = COLON_RE.match(line)
    if m:
        return m.group(1).strip().rstrip(".:;"), m.group(2).strip()
    m = DOT_RE.match(line)
    if m:
        # Guard against false positives: require term not too long (<= 8 words) OR contains capitalized words
        term = m.group(1).strip()
        defn = m.group(2).strip()
        words = term.split()
        cap_words = sum(1 for w in words if w[:1].isupper())
        if len(words) <= 8 or cap_words >= max(2, len(words)//2):
            return term, defn
    return None

def parse_accumulated(lines):
    """Stateful parser: new term starts on a recognized line; following lines join definition until next term."""
    items = []
    cur_term = None
    cur_def = []
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

def longest_term_run(all_lines, min_terms_in_run=4, max_gap=2):
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

def extract_special_terms_anchorless(raw_html: str):
    """
    1) Convert entire body to text with line breaks.
    2) Locate longest run of recognizable term lines (dash/colon/dot).
    3) Parse with accumulation; dedupe terms (favor longest definitions).
    """
    text = html_to_text_with_breaks(raw_html)
    if not text:
        return [], ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    start, end, items = longest_term_run(lines, min_terms_in_run=4, max_gap=3)
    if not items:
        # Very lenient fallback
        start, end, items = longest_term_run(lines, min_terms_in_run=2, max_gap=5)
    debug_slice = "\n".join(lines[start:end]) if end > start else ""
    # Dedup
    dedup = OrderedDict()
    for it in items:
        t = it["term"].strip()
        d = it["definition"].strip()
        if not t or not d: continue
        k = t.lower()
        if (k not in dedup) or (len(d) > len(dedup[k]["definition"])):
            dedup[k] = {"term": t, "definition": d}
    return list(dedup.values()), debug_slice

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="EDGAR Special Terms / Glossary Extractor", layout="wide")
st.title("EDGAR — Extract Special Terms & Definitions (Anchorless, multi-style)")

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
        terms, debug_text = extract_special_terms_anchorless(raw_html)

    st.subheader("Results")
    if terms:
        st.success(f"Found {len(terms)} terms.")
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
