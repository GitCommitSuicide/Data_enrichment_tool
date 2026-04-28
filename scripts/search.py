"""
search.py — Tavily search + scraping + query building

Threading model
───────────────
• All queries are dispatched in parallel via ThreadPoolExecutor.
• Each query also fans out scrape jobs for its results in parallel.
• Results are collected keyed by query index so the final chunk list
  is always assembled in the original query declaration order.
"""
from __future__ import annotations
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import tavily, MAX_RESULTS_PER_QUERY, MAX_CONTEXT

MAX_QUERY_WORKERS = 12  # parallel Tavily calls
MAX_SCRAPE_WORKERS = 15  # parallel scrape calls per query batch

# ── Scraper ───────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
_NOISE_TAGS = frozenset(["script","style","nav","footer","header","aside","noscript","form","button","iframe","svg"])


def scrape_url(url: str) -> str:
    try:
        resp = requests.get(url, timeout=12, headers=_HEADERS, verify=False)
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(_NOISE_TAGS):
            tag.decompose()
        return " ".join(soup.stripped_strings)[:30_000]
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
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]{2,64}@[A-Za-z0-9.\-]{2,255}\.[A-Za-z]{2,6}\b")
_EMAIL_JUNK = re.compile(r"(?:example\.|noreply|no-reply|@sentry\.|@w3\.org|wixpress\.com)", re.IGNORECASE)


def extract_social_urls(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
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

def fetch(query: str) -> list[dict]:
    try:
        sr = tavily.search(
            query=query,
            search_depth="advanced",
            include_answer=True,
            include_raw_content=True,
            max_results=MAX_RESULTS_PER_QUERY,
        )
        items = []
        for r in sr.get("results", []):
            url     = r.get("url", "").strip()
            content = (r.get("raw_content") or r.get("content") or "").strip()
            if url and content:
                items.append({"url": url, "title": r.get("title","").strip(), "content": content})
        return items
    except Exception as e:
        print(f"  [tavily] {e}  ({query[:80]})")
        return []


# ── Query builder ─────────────────────────────────────────────────────────────

def build_queries(company: str) -> list[str]:
    c = company.strip()
    return [
        f'"{c}" (official website OR site:linkedin.com/company OR site:crunchbase.com OR site:bloomberg.com)',
        f'"{c}" company overview products services industry customers differentiators',
        f'"{c}" (CEO OR founder OR CTO OR "managing director") leadership team site:linkedin.com OR site:rocketreach.co',
        f'"{c}" (revenue OR valuation OR funding OR "series A" OR "series B") site:crunchbase.com OR site:pitchbook.com',
        f'"{c}" (CIN OR ROC OR MCA OR GSTIN OR "GST number") site:zaubacorp.com OR site:tofler.in OR site:mca.gov.in',
         f'"{c}" latest news OR recent developments OR press release 2025 OR 2026',
        f'"{c}" major achievements OR milestones OR awards OR key highlights 2025 OR 2026',
        f'"{c}" subsidiaries OR "parent company" OR divisions OR "group companies" OR "wholly owned and their full details"',
        f'"{c}" tech stack infrastructure cloud platform integrations',
        f'"{c}" funding acquisition partnership expansion 2025 OR 2026',
        f'"{c}" latest news OR recent developments OR press release 2025 OR 2026',
        f'"{c}" major achievements OR milestones OR awards OR key highlights 2025 OR 2026',
        f'"{c}" (reviews OR rating OR awards OR certifications OR partnerships) (site:glassdoor.com OR site:trustpilot.com OR site:g2.com OR site:ambitionbox.com OR site:indeed.com)',

    ]


def build_person_queries(company: str, person_name: str) -> list[str]:
    if not person_name:
        return []
    p, c = person_name.strip(), company.strip()
    return [
        f'"{p}" "{c}" (email OR phone OR "contact details") site:linkedin.com OR site:rocketreach.co OR site:apollo.io',
        f'"{p}" "{c}" profile experience education site:linkedin.com/in',
    ]


# ── Threaded helpers ──────────────────────────────────────────────────────────

def _fetch_and_scrape(idx: int, query: str, total: int) -> tuple[int, list[dict]]:
    """
    Fetch one query from Tavily, then scrape shallow results in parallel.
    Returns (idx, list_of_result_dicts) so the caller can sort by idx.
    """
    print(f"  [{idx+1:>2}/{total}] {query[:100]}")
    items = fetch(query)
    if not items:
        return idx, []

    # Identify which items need a deeper scrape (Tavily returned a short snippet)
    needs_scrape = [(i, it) for i, it in enumerate(items) if len(it["content"]) < 500]
    needs_keep   = [(i, it) for i, it in enumerate(items) if len(it["content"]) >= 500]

    # Fan out scrapes in parallel
    scraped_content: dict[int, str] = {}
    if needs_scrape:
        with ThreadPoolExecutor(max_workers=MAX_SCRAPE_WORKERS) as scrape_pool:
            fut_map = {
                scrape_pool.submit(scrape_url, it["url"]): i
                for i, it in needs_scrape
            }
            for fut in as_completed(fut_map):
                i = fut_map[fut]
                try:
                    scraped_content[i] = fut.result()
                except Exception:
                    scraped_content[i] = ""

    # Merge scrape results back
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
    Run all queries in parallel, scrape results in parallel per query,
    then assemble chunks in the original query declaration order.
    Returns (research_text, source_urls).
    """
    all_queries = build_queries(company) + build_person_queries(company, person_name)
    total       = len(all_queries)

    print(f"  Dispatching {total} queries in parallel (max {MAX_QUERY_WORKERS} workers) …\n")

    # ── Phase 1: run all queries in parallel, collect results keyed by index ──
    # raw_results[i] = list of result dicts for query i
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

    # ── Phase 2: assemble chunks in original query order ──────────────────────
    seen_urls:   set[str]  = set()
    seen_prints: set[str]  = set()
    chunks:      list[str] = []
    urls:        list[str] = []

    for i in range(total):
        for item in raw_results.get(i, []):
            url     = item["url"]
            content = item["content"]

            if url in seen_urls:
                continue
            seen_urls.add(url)

            fp = content[:120].lower()
            if fp in seen_prints:
                continue
            seen_prints.add(fp)

            chunks.append(f"SOURCE: {url}\nTitle: {item['title']}\n{content[:20_000]}\n")
            urls.append(url)

    print(f"\n  Gathered {len(urls)} unique sources from {total} queries")

    if not chunks:
        return "", []

    full_text = "\n\n".join(chunks)
    socials   = extract_social_urls(full_text)
    print("Raw_ text length",len(full_text))
    if len(full_text) > MAX_CONTEXT:
        full_text = full_text[:MAX_CONTEXT]

    if socials:
        full_text += "\n\nEXTRACTED CONTACT & SOCIAL URLS:\n" + "\n".join(socials)

    return full_text, urls