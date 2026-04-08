from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import tavily, MAX_RESULTS_PER_QUERY, MAX_WORKERS, MAX_CONTEXT
from .queries import build_queries

def fetch(query: str) -> list[dict]:
    """Fetch search results for a single query via Tavily."""
    items = []
    try:
        sr = tavily.search(
            query=query,
            search_depth="advanced",
            include_answer=True,
            max_results=MAX_RESULTS_PER_QUERY,
        )
        for r in sr.get("results", []):
            url     = r.get("url", "").strip()
            content = r.get("content", "").strip()
            if url and content:
                items.append({
                    "url":     url,
                    "title":   r.get("title", "").strip(),
                    "content": content,
                })
    except Exception as e:
        print(f"  [warn] query failed — {e}  ({query[:60]})")
    return items

def gather(company: str) -> tuple[str, list[str]]:
    """
    Run all search queries concurrently and aggregate unique results.
    Returns (research_text, source_urls).
    """
    queries  = build_queries(company)
    seen_urls     = set()
    seen_snippets = set()   # deduplicate near-identical content
    chunks   = []
    all_urls = []

    print(f"  Running {len(queries)} queries …\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, q): q for q in queries}

        for i, fut in enumerate(as_completed(futures), 1):
            q = futures[fut]
            print(f"  [{i:>2}/{len(queries)}] {q[:70]}")

            for item in fut.result():
                url = item["url"]

                # Deduplicate by URL
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                # Deduplicate by content fingerprint (first 120 chars)
                fingerprint = item["content"][:120].lower()
                if fingerprint in seen_snippets:
                    continue
                seen_snippets.add(fingerprint)

                all_urls.append(url)
                chunks.append(
                    f"Source: {url}\n"
                    f"Title: {item['title']}\n"
                    f"{item['content']}"
                )

    if not chunks:
        print("  [warn] No content gathered — all queries returned empty results.")
        return "", all_urls

    # Join chunks and trim at a sentence boundary near MAX_CONTEXT
    research_text = "\n\n".join(chunks)
    if len(research_text) > MAX_CONTEXT:
        trimmed = research_text[:MAX_CONTEXT]
        # Try to end at the last complete sentence
        last_period = trimmed.rfind(". ")
        research_text = trimmed[: last_period + 1] if last_period > 0 else trimmed

    return research_text, all_urls
