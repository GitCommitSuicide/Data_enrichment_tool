"""
search.py — Tavily search + scraping + query building

Threading model
───────────────
• All queries are dispatched in parallel via ThreadPoolExecutor.
• Scrapes are also parallelized per-query batch.
• Results are assembled in original query declaration order.
"""
from __future__ import annotations
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from .config import tavily, MAX_RESULTS_PER_QUERY, MAX_CONTEXT

MAX_QUERY_WORKERS  = 15
MAX_SCRAPE_WORKERS = 20

# ── Scraper ───────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
_NOISE_TAGS = frozenset([
    "script", "style", "nav", "footer", "header", "aside",
    "noscript", "form", "button", "iframe", "svg"
])


def scrape_url(url: str, timeout: int = 10) -> str:
    try:
        resp = requests.get(url, timeout=timeout, headers=_HEADERS, verify=False)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(_NOISE_TAGS):
            tag.decompose()
        return " ".join(soup.stripped_strings)[:25_000]
    except Exception as e:
        print(f"  [scraper] {url}: {e}")
        return ""


# ── Social / email extraction ─────────────────────────────────────────────────

_SOCIAL_RE = re.compile(
    r"https?://(?:www\.)?(?:"
    r"linkedin\.com/in/[A-Za-z0-9\-_%]+"
    r"|linkedin\.com/company/[A-Za-z0-9\-_%]+"
    r"|twitter\.com/[A-Za-z0-9_]{1,50}"
    r"|crunchbase\.com/(?:person|organization)/[A-Za-z0-9\-_]+"
    r"|rocketreach\.co/[A-Za-z0-9\-_%/]+"
    r"|apollo\.io/people/[A-Za-z0-9\-_%/]+"
    r")",
    re.IGNORECASE,
)
_EMAIL_RE   = re.compile(r"\b[A-Za-z0-9._%+\-]{2,64}@[A-Za-z0-9.\-]{2,255}\.[A-Za-z]{2,6}\b")
_EMAIL_JUNK = re.compile(
    r"(?:example\.|noreply|no-reply|@sentry\.|@w3\.org|wixpress\.com)",
    re.IGNORECASE,
)


def extract_social_urls(text: str) -> list[str]:
    seen: set[str] = set()
    out:  list[str] = []
    for url in _SOCIAL_RE.findall(text):
        url = url.rstrip(".,);\"'")
        if url not in seen:
            seen.add(url); out.append(url)
    for email in _EMAIL_RE.findall(text):
        email = email.lower().strip(".,;\"'")
        if not _EMAIL_JUNK.search(email) and email not in seen:
            seen.add(email); out.append(email)
    return out


# ── Tavily fetch ──────────────────────────────────────────────────────────────

def fetch(query: str, max_results: int = MAX_RESULTS_PER_QUERY) -> list[dict]:
    try:
        sr = tavily.search(
            query=query,
            search_depth="advanced",
            include_answer=True,
            include_raw_content=True,
            max_results=max_results,
        )
        items = []
        for r in sr.get("results", []):
            url     = r.get("url", "").strip()
            content = (r.get("raw_content") or r.get("content") or "").strip()
            if url and content:
                items.append({
                    "url":     url,
                    "title":   r.get("title", "").strip(),
                    "content": content,
                })
        return items
    except Exception as e:
        print(f"  [tavily] {e}  ({query[:80]})")
        return []


# ── Query builder ─────────────────────────────────────────────────────────────

def build_queries(company: str) -> list[str]:
    """
    Focused query set — fewer, higher-signal queries to cut wall time.
    Covers: identity/legal, leadership, financials, products/tech, news,
    subsidiaries, reviews, and company/CIN numbers specifically.
    """
    c = company.strip()
    return [
        # 1. Core identity + legal numbers (CIN, company number, GST, PAN)
        f'"{c}" ("company number" OR "CIN" OR "registration number" OR "GSTIN" OR "GST number" OR "PAN") '
        f'site:companieshouse.gov.uk OR site:zaubacorp.com OR site:tofler.in OR site:mca.gov.in OR site:find-and-update.company-information.service.gov.uk',

        # 2. Official website / overview
        f'"{c}" official site company overview products services industry',

        # 3. Leadership + LinkedIn
        f'"{c}" (CEO OR founder OR CTO OR "managing director" OR director) site:linkedin.com/company OR site:linkedin.com/in',

        # 4. Financials / funding
        f'"{c}" (revenue OR valuation OR funding OR "series A" OR "series B" OR "annual report") '
        f'site:crunchbase.com OR site:pitchbook.com OR site:tracxn.com',

        # 5. Subsidiaries / group structure
        f'"{c}" (subsidiaries OR "parent company" OR "group companies" OR "wholly owned" OR divisions OR "sister company")',

        # 6. Recent news 2024-2025
        f'"{c}" (news OR announcement OR launch OR partnership OR acquisition OR expansion) 2024 OR 2025',

        # 7. Awards, certifications, reviews
        f'"{c}" (awards OR certifications OR "ISO" OR glassdoor OR trustpilot OR g2 OR reviews OR rating)',

        # 8. Tech stack + integrations
        f'"{c}" (technology OR "tech stack" OR cloud OR integrations OR API OR platform)',
    ]


def build_person_queries(company: str, person_name: str) -> list[str]:
    if not person_name:
        return []
    p, c = person_name.strip(), company.strip()
    return [
        # Direct LinkedIn profile
        f'"{p}" "{c}" site:linkedin.com/in',
        # Contact details from enrichment services
        f'"{p}" "{c}" (email OR "contact" OR phone) site:rocketreach.co OR site:apollo.io OR site:contactout.com',
        # General profile
        f'"{p}" "{c}" profile OR biography OR background',
    ]


# ── Threaded fetch + scrape ───────────────────────────────────────────────────

def _fetch_and_scrape(idx: int, query: str, total: int) -> tuple[int, list[dict]]:
    """Fetch one query, scrape thin results in parallel."""
    print(f"  [{idx+1:>2}/{total}] {query[:100]}")
    items = fetch(query)
    if not items:
        return idx, []

    needs_scrape = [(i, it) for i, it in enumerate(items) if len(it["content"]) < 600]

    scraped_content: dict[int, str] = {}
    if needs_scrape:
        with ThreadPoolExecutor(max_workers=min(MAX_SCRAPE_WORKERS, len(needs_scrape))) as pool:
            fut_map = {pool.submit(scrape_url, it["url"]): i for i, it in needs_scrape}
            for fut in as_completed(fut_map):
                i = fut_map[fut]
                try:
                    scraped_content[i] = fut.result()
                except Exception:
                    scraped_content[i] = ""

    result_items = []
    for i, it in enumerate(items):
        content = it["content"]
        if i in scraped_content and len(scraped_content[i]) > len(content):
            content = scraped_content[i]
        result_items.append({**it, "content": content})

    return idx, result_items


# ── Main gather ───────────────────────────────────────────────────────────────

def gather(company: str, person_name: str = "") -> tuple[str, list[str]]:
    """
    Run all queries fully in parallel, return (research_text, source_urls).
    """
    company_queries = build_queries(company)
    person_queries  = build_person_queries(company, person_name)
    all_queries     = company_queries + person_queries
    total           = len(all_queries)

    print(f"  Dispatching {total} queries in parallel (max {MAX_QUERY_WORKERS} workers) …\n")

    raw_results: dict[int, list[dict]] = {}

    with ThreadPoolExecutor(max_workers=MAX_QUERY_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_and_scrape, i, q, total): i
            for i, q in enumerate(all_queries)
        }
        for fut in as_completed(futures):
            try:
                idx, items = fut.result()
                raw_results[idx] = items
            except Exception as e:
                idx = futures[fut]
                print(f"  [warn] query {idx+1} failed: {e}")
                raw_results[idx] = []

    # Assemble in query order, deduplicated
    seen_urls:   set[str]  = set()
    seen_prints: set[str]  = set()
    chunks:      list[str] = []
    urls:        list[str] = []

    for i in range(total):
        for item in raw_results.get(i, []):
            url, content = item["url"], item["content"]
            if url in seen_urls:
                continue
            seen_urls.add(url)
            fp = content[:120].lower()
            if fp in seen_prints:
                continue
            seen_prints.add(fp)
            chunks.append(f"SOURCE: {url}\nTitle: {item['title']}\n{content[:18_000]}\n")
            urls.append(url)

    print(f"\n  Gathered {len(urls)} unique sources from {total} queries")

    if not chunks:
        return "", []

    full_text = "\n\n".join(chunks)
    socials   = extract_social_urls(full_text)
    print(f"  Raw text length: {len(full_text)}")

    if len(full_text) > MAX_CONTEXT:
        full_text = full_text[:MAX_CONTEXT]

    if socials:
        full_text += "\n\nEXTRACTED CONTACT & SOCIAL URLS:\n" + "\n".join(socials)

    return full_text, urls