"""
enricher.py — Company gap-filling enrichment (parallel extraction)
"""
from __future__ import annotations
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from .search import fetch, scrape_url
from .extractor import call_ollama

_RESULTS_PER_QUERY = 5
_MAX_SOURCES       = 10
_SCRAPE_CAP        = 10_000

_EMPTY = frozenset(["", "...", "not specified", "n/a", "unknown", "none", "null", "not available"])

_QUERY_PROMPT = """\
You are a senior OSINT analyst.
Given what we know about a company and its data gaps, produce exactly 5 targeted search queries
to fill the most critical missing fields.

Prioritise gaps in this order:
1. Legal registration numbers (company_number, CIN, GSTIN) — if missing
2. Financial data (revenue, funding, valuation)
3. Recent news and developments (2024-2025)
4. LinkedIn URLs for leaders missing them
5. Subsidiaries, group companies, certifications

Return ONLY a JSON array of 5 strings. No markdown, no explanation.
"""

_EXTRACT_PROMPT = """\
You are an intelligence extraction specialist.
Extract every useful fact from the raw web content about "{company}" into a JSON object.

Focus on:
- Legal registration numbers (company_number, CIN, GSTIN, PAN)
- Financial data (revenue, funding, valuation)
- Recent news and announcements (2024-2025)
- Leadership LinkedIn URLs
- Subsidiaries and group companies
- Awards, certifications, reviews

Use descriptive snake_case keys. Return valid JSON only. If no useful data, return {}.
"""

_MERGE_PROMPT = """\
You are a data-fusion analyst.
Merge all enrichment fragments into the original company JSON.

RULES:
- Fill gaps where original has missing/empty values
- NEVER overwrite existing real values
- Source priority: government registry > official site > LinkedIn > news
- Deduplicate all lists
- company_number (UK, 8 chars) vs cin_number (India, 21 chars) — keep both if present, never confuse
- Subsidiaries: list ALL found group/sister companies
- recent_news_and_updates: include ALL news items found (2024-2026 priority)
- key_achievements: include ALL milestones found
- Remove duplicate keys (e.g. if both "title" and "current_title" exist, keep only "current_title")
- Do NOT include employee-level info in leadership unless they are director/C-suite
- Include "sources" array with all unique URLs
- Output valid JSON only — starts with { ends with }
"""


def _is_empty(val) -> bool:
    if val is None:
        return True
    if isinstance(val, str) and val.strip().lower() in _EMPTY:
        return True
    if isinstance(val, (list, dict)) and not val:
        return True
    return False


def _find_gaps(data: dict, prefix: str = "") -> list[str]:
    gaps = []
    for key, val in data.items():
        path = f"{prefix}.{key}" if prefix else key
        if _is_empty(val):
            gaps.append(path)
        elif isinstance(val, dict):
            gaps.extend(_find_gaps(val, path))
        elif isinstance(val, list):
            for i, item in enumerate(val):
                if isinstance(item, dict):
                    gaps.extend(_find_gaps(item, f"{path}[{i}]"))
    return gaps


def _generate_queries(data: dict) -> list[str]:
    gaps     = _find_gaps(data)
    co       = data.get("company", {})
    leaders  = data.get("leadership", [])
    name     = co.get("name", "?")

    missing_li = [l.get("name", "?") for l in leaders if not l.get("linkedin_url")]
    missing_reg = []
    if not co.get("company_number") and not co.get("cin_number"):
        missing_reg.append("registration number")

    summary  = f"Company: {name}\nHQ: {co.get('headquarters',{}).get('city','?')}\n"
    if missing_reg:
        summary += f"MISSING: {', '.join(missing_reg)}\n"
    if missing_li:
        summary += f"MISSING LinkedIn for: {', '.join(missing_li[:5])}\n"
    summary += f"Gaps ({len(gaps)}): {', '.join(gaps[:30])}"

    print(f"  [enricher] {len(gaps)} gaps found — generating queries …")
    raw = call_ollama(_QUERY_PROMPT, summary)

    if isinstance(raw, dict):
        for k in ("queries", "results", "data", "search_queries"):
            if isinstance(raw.get(k), list):
                raw = raw[k]
                break
        else:
            raw = list(raw.values())

    if not isinstance(raw, list):
        raw = [
            f'"{name}" (revenue OR funding OR valuation)',
            f'"{name}" "company number" OR "registration" site:companieshouse.gov.uk OR site:find-and-update.company-information.service.gov.uk',
            f'"{name}" news OR announcement OR launch 2024 OR 2025',
            f'"{name}" CEO OR founder OR director site:linkedin.com/in',
            f'"{name}" subsidiaries OR "group companies" OR certifications OR awards',
        ]

    queries = [str(q).strip() for q in raw if q][:5]
    for i, q in enumerate(queries, 1):
        print(f"    Q{i}: {q[:100]}")
    return queries


def _search_and_scrape_parallel(queries: list[str]) -> list[dict]:
    """Run all queries in parallel, scrape results in parallel."""
    n = len(queries)

    # Phase 1: parallel Tavily fetches
    raw_results: dict[int, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=n) as pool:
        fut_map = {pool.submit(fetch, q, _RESULTS_PER_QUERY): i for i, q in enumerate(queries)}
        for fut in as_completed(fut_map):
            i = fut_map[fut]
            try:
                raw_results[i] = fut.result()
            except Exception as e:
                print(f"  [enricher] fetch error Q{i+1}: {e}")
                raw_results[i] = []

    # Deduplicate in order
    seen: set[str]  = set()
    ordered: list[dict] = []
    for i in range(n):
        for item in raw_results.get(i, []):
            if item["url"] not in seen and len(ordered) < _MAX_SOURCES:
                seen.add(item["url"])
                ordered.append(item)

    # Phase 2: parallel scrapes for short content
    needs_scrape = [(i, it) for i, it in enumerate(ordered) if len(it["content"]) < 800]
    scraped: dict[int, str] = {}
    if needs_scrape:
        with ThreadPoolExecutor(max_workers=min(len(needs_scrape), 10)) as pool:
            fut_map2 = {pool.submit(scrape_url, it["url"]): i for i, it in needs_scrape}
            for fut in as_completed(fut_map2):
                i = fut_map2[fut]
                try:
                    scraped[i] = fut.result()
                except Exception:
                    scraped[i] = ""

    sources = []
    for i, item in enumerate(ordered):
        content = item["content"]
        if i in scraped and len(scraped[i]) > len(content):
            content = scraped[i]
        sources.append({**item, "content": content[:_SCRAPE_CAP]})

    print(f"  [enricher] {len(sources)} sources collected")
    return sources


def enrich(company_data: dict, company_name: str) -> dict:
    """Enrich company data with gap-filling. Returns enriched dict."""
    print(f"\n{'═'*55}\n  ENRICHMENT — {company_name}\n{'═'*55}")

    queries = _generate_queries(company_data)
    sources = _search_and_scrape_parallel(queries)
    if not sources:
        return company_data

    all_urls = [s["url"] for s in sources]
    for u in company_data.get("sources", []):
        if u not in all_urls:
            all_urls.append(u)

    # Phase 3: parallel per-source extraction
    print(f"\n  [enricher] Extracting from {len(sources)} sources in parallel …")
    fragments: list[dict] = []
    extract_prompt = _EXTRACT_PROMPT.replace("{company}", company_name)

    with ThreadPoolExecutor(max_workers=min(len(sources), 6)) as pool:
        fut_map = {
            pool.submit(
                call_ollama,
                extract_prompt,
                f"URL: {src['url']}\n\n{src['content'][:6_000]}",
                1,  # 1 retry for speed
            ): i
            for i, src in enumerate(sources)
            if len(src["content"].strip()) >= 100
        }
        results: dict[int, dict] = {}
        for fut in as_completed(fut_map):
            i = fut_map[fut]
            try:
                frag = fut.result()
                if frag:
                    frag["_from"] = sources[i]["url"]
                    results[i]    = frag
                    print(f"  [enricher] extracted [{i+1}/{len(sources)}] {sources[i]['url'][:70]}")
            except Exception as e:
                print(f"  [enricher] extract error [{i+1}]: {e}")

    # Reassemble in source order
    for i in range(len(sources)):
        if i in results:
            fragments.append(results[i])

    print(f"  [enricher] {len(fragments)} fragments extracted")
    if not fragments:
        company_data["sources"] = all_urls[:_MAX_SOURCES]
        return company_data

    clean_frags = [{k: v for k, v in f.items() if k != "_from"} for f in fragments]

    user_msg = (
        "ORIGINAL:\n"
        + json.dumps(company_data, indent=2, ensure_ascii=False)[:35_000]
        + "\n\nFRAGMENTS:\n"
        + json.dumps(clean_frags, indent=2, ensure_ascii=False)[:70_000]
        + "\n\nSOURCES:\n"
        + json.dumps(all_urls[:_MAX_SOURCES])
    )

    merged = call_ollama(_MERGE_PROMPT, user_msg, retries=2)
    if not merged:
        company_data["sources"] = all_urls[:_MAX_SOURCES]
        return company_data

    # Ensure all source URLs included
    existing_sources = set(merged.get("sources", []))
    for u in all_urls[:_MAX_SOURCES]:
        existing_sources.add(u)
    merged["sources"] = list(existing_sources)[:_MAX_SOURCES]

    return merged