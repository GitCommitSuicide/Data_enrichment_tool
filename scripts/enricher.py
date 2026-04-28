"""
enricher.py — Company gap-filling enrichment
Saves its own JSON to data_scraped/<company>_enriched.json
"""
from __future__ import annotations
import json
from .search import fetch, scrape_url
from .extractor import call_ollama

_RESULTS_PER_QUERY = 5
_MAX_SOURCES       = 12
_SCRAPE_CAP        = 12_000

_EMPTY = frozenset(["", "...", "not specified", "n/a", "unknown", "none", "null", "not available"])

_QUERY_PROMPT = """\
You are a senior OSINT analyst.
Given what we know about a company and its data gaps, produce exactly 5 search queries
to fill the most critical missing fields. Prioritise:
1. LinkedIn URLs for leaders missing them (at least 2 queries must target this)
2. Financials (revenue, funding, valuation)
3. Subsidiaries and group companies
4. Tech stack
5. Certifications, real competitors

Return ONLY a JSON array of 5 strings. No markdown, no explanation.
"""

_EXTRACT_PROMPT = """\
You are an intelligence extraction specialist.
Extract every useful fact from the raw web content about "{company}" into a JSON object.
Use descriptive snake_case keys. No fixed schema — structure based on what's available.
Return valid JSON only. If no useful data, return {}.
"""

_MERGE_PROMPT = """\
You are a data-fusion analyst.
Merge all enrichment fragments into the original company JSON.
- Fill gaps where original has missing/empty values
- Never remove existing real values
- Resolve conflicts: government > official site > LinkedIn > news
- Deduplicate entities
- Include "sources" array with all unique URLs
- Subsidiaries: list ALL found subsidiaries and group companies
- Output valid JSON only — starts with { ends with }
-remove duplicates likes title , current_title 
-Do not include the info about the Employees , Add if the HR in a seprate as Key_contacts
-include the useful summary of the target person
"""


def _is_empty(val) -> bool:
    if val is None: return True
    if isinstance(val, str) and val.strip().lower() in _EMPTY: return True
    if isinstance(val, (list, dict)) and not val: return True
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
    gaps    = _find_gaps(data)
    co      = data.get("company", {})
    leaders = data.get("leadership", [])
    missing_li = [l.get("name","?") for l in leaders if not l.get("linkedin_url")]

    summary = f"Company: {co.get('name','?')}\n"
    summary += f"HQ: {co.get('headquarters',{}).get('city','?')}\n"
    if missing_li:
        summary += f"MISSING LinkedIn for: {', '.join(missing_li)}\n"
    summary += f"Gaps ({len(gaps)}): {', '.join(gaps[:40])}"

    print(f"  [enricher] {len(gaps)} gaps found — generating queries …")
    raw = call_ollama(_QUERY_PROMPT, summary)

    # Unwrap if LLM returned dict wrapping a list
    if isinstance(raw, dict):
        for k in ("queries", "results", "data", "search_queries"):
            if isinstance(raw.get(k), list):
                raw = raw[k]; break
        else:
            raw = list(raw.values())

    if not isinstance(raw, list):
        name = co.get("name", "company")
        raw = [
            f'"{name}" revenue funding valuation',
            f'"{name}" CEO founder leadership LinkedIn',
            f'"{name}" subsidiaries "group companies" "wholly owned"',
            f'"{name}" latest news OR recent developments OR press release 2025 OR 2026'
            f'"{name}" technology stack cloud platform',
            f'"{name}" competitors certifications awards',
            
        ]

    queries = [str(q).strip() for q in raw if q][:5]
    for i, q in enumerate(queries, 1):
        print(f"    Q{i}: {q[:100]}")
    return queries


def _search_and_scrape(queries: list[str]) -> list[dict]:
    """
    Run all enrichment queries sequentially, scrape results sequentially,
    then return sources in original query order (deduplicated).
    """
    n = len(queries)

    # Phase 1: sequential Tavily fetches
    seen: set[str] = set()
    ordered_items: list[dict] = []
    for i, query in enumerate(queries):
        print(f"  [enricher] Q{i+1}/{n}: {query[:90]}")
        try:
            items = fetch(query)[:_RESULTS_PER_QUERY]
        except Exception as e:
            print(f"  [enricher] fetch error: {e}")
            items = []
        for item in items:
            if item["url"] not in seen and len(ordered_items) < _MAX_SOURCES:
                seen.add(item["url"])
                ordered_items.append(item)

    # Phase 2: sequential scrapes
    sources: list[dict] = []
    for item in ordered_items:
        content = item["content"]
        if len(content) < 800:
            scraped = scrape_url(item["url"])
            if len(scraped) > len(content):
                content = scraped
        sources.append({**item, "content": content[:_SCRAPE_CAP]})

    print(f"  [enricher] {len(sources)} sources collected")
    return sources


def enrich(company_data: dict, company_name: str) -> dict:
    """Enrich company data. Returns enriched dict (also saved separately)."""
    print(f"\n{'═'*55}\n  ENRICHMENT — {company_name}\n{'═'*55}")

    queries = _generate_queries(company_data)
    sources = _search_and_scrape(queries)
    if not sources:
        return company_data

    all_urls = [s["url"] for s in sources]
    # Preserve existing sources
    for u in company_data.get("sources", []):
        if u not in all_urls:
            all_urls.append(u)

    # Per-source extraction — run all sequentially, preserve source order
    print(f"\n  [enricher] Extracting from {len(sources)} sources sequentially …")

    fragments: list[dict] = []
    for i, src in enumerate(sources):
        content = src["content"][:8_000]
        if len(content.strip()) < 100:
            continue
        prompt = _EXTRACT_PROMPT.replace("{company}", company_name)
        frag   = call_ollama(prompt, f"URL: {src['url']}\n\n{content}", retries=1)
        if frag:
            frag["_from"] = src["url"]
        print(f"  [enricher] extracted [{i+1}/{len(sources)}] {src['url'][:70]}")
        if frag:
            fragments.append(frag)

    print(f"  [enricher] {len(fragments)} fragments extracted")
    if not fragments:
        company_data["sources"] = all_urls[:_MAX_SOURCES]
        return company_data

    clean_frags = [{k: v for k, v in f.items() if k != "_source_url"} for f in fragments]

    user_msg = (
        "ORIGINAL:\n" + json.dumps(company_data, indent=2, ensure_ascii=False)[:40_000]
        + "\n\nFRAGMENTS:\n" + json.dumps(clean_frags, indent=2, ensure_ascii=False)[:80_000]
        + "\n\nSOURCES:\n" + json.dumps(all_urls[:_MAX_SOURCES])
    )

    merged = call_ollama(_MERGE_PROMPT, user_msg, retries=2)
    if not merged:
        company_data["sources"] = all_urls[:_MAX_SOURCES]
        return company_data

    # Ensure all URLs in sources
    seen = set(merged.get("sources", []))
    for u in all_urls[:_MAX_SOURCES]:
        seen.add(u)
    merged["sources"] = list(seen)[:_MAX_SOURCES]
    return merged