import json
import re
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tavily import TavilyClient
from queries import build_queries
import os
from dotenv import load_dotenv
load_dotenv()


TAVILY_API_KEY  = os.getenv("TAVILY_API_KEY")
OLLAMA_URL      = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "gpt-oss:120b-cloud")
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", 6))
MAX_CONTEXT     = int(os.getenv("MAX_CONTEXT", 100000))   # chars sent to model
OLLAMA_TIMEOUT  = int(os.getenv("OLLAMA_TIMEOUT", 120)) 
MAX_RESULTS_PER_QUERY = 12

# ── Clients ───────────────────────────────────────────────────────────────────
if not TAVILY_API_KEY:
    sys.exit("[fatal] TAVILY_API_KEY is not set in .env")

tavily = TavilyClient(api_key=TAVILY_API_KEY)


# ── Search & Fetch ────────────────────────────────────────────────────────────
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


# ── Ollama health check ───────────────────────────────────────────────────────
def check_ollama() -> bool:
    try:
        r = requests.get(OLLAMA_URL.replace("/api/generate", ""), timeout=5)
        return r.status_code == 200
    except requests.exceptions.ConnectionError:
        return False


# ── Extraction ────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """
You are a strict and intelligent company data extractor.

TASK:
Extract ALL important and useful company-related information from the provided text and return it as structured JSON.

CRITICAL RULES (NON-NEGOTIABLE):
1. ONLY extract information explicitly present in the text.
2. NEVER infer, assume, or guess any data.
3. NEVER fabricate names, emails, numbers, or dates.
4. If information is unclear or uncertain → DO NOT include it.
5. Include ONLY verifiable and meaningful data.

EXTRACTION RULES:
- Capture ALL important data such as:
  - company details (name, description, website, location, industry)
  - services/products
  - leadership and key people
  - financials (revenue, funding, valuation)
  - company structure, subsidiaries
  - partnerships, technologies, certifications
  - online presence, reviews, metrics
  - any other useful business-related information

- DO NOT leave out useful information just because it doesn’t fit a fixed structure.
- If new types of information are found, create meaningful keys in snake_case.
- Group similar information into lists when appropriate.
- Keep the structure clean, logical, and well-organized.

OUTPUT RULES:
- Output ONLY valid JSON.
- NO markdown, NO explanation, NO extra text.
- DO NOT include null values, empty arrays, or empty objects.
- Keep data deduplicated and concise but complete.

STRUCTURE GUIDELINES:
- Use clear and meaningful key names (snake_case).
- Organize data logically (e.g., group related fields together).
- Avoid repeating the same information in multiple places.

GOAL:
Return a clean, structured JSON that contains ALL important information from the text without losing any valuable data.
"""

def _parse_json_safe(raw: str) -> dict:
    # Strip markdown code fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    # Attempt direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Find the outermost { … } safely using brace counting
    start = raw.find("{")
    if start == -1:
        return {}

    depth = 0
    end   = -1
    for i, ch in enumerate(raw[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end == -1:
        return {}

    try:
        return json.loads(raw[start:end])
    except json.JSONDecodeError as e:
        print(f"  [warn] JSON parse failed after extraction: {e}")
        return {}


def extract(research_text: str, company: str, retries: int = 2) -> dict:
    
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"Company: {company}\n\n"
        f"Research:\n{research_text}"
    )

    for attempt in range(1, retries + 2):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model":   OLLAMA_MODEL,
                    "prompt":  prompt,
                    "stream":  False,
                    "options": {"temperature": 0.0},
                },
                timeout=OLLAMA_TIMEOUT,
            )
            response.raise_for_status()
            raw = response.json().get("response", "").strip()
            result = _parse_json_safe(raw)
            if result:
                return result
            print(f"  [warn] Attempt {attempt}: empty parse result, retrying…")
        except requests.exceptions.Timeout:
            print(f"  [warn] Attempt {attempt}: Ollama timed out after {OLLAMA_TIMEOUT}s")
        except requests.exceptions.RequestException as e:
            print(f"  [error] Attempt {attempt}: Ollama request error — {e}")
            break

        if attempt <= retries:
            time.sleep(2)

    print("  [error] All extraction attempts failed.")
    return {}


# ── Main ──────────────────────────────────────────────────────────────────────
def normalise_input(raw: str) -> str:
    """Convert email or domain input to a plain company name."""
    raw = raw.strip()
    if "@" in raw:
        domain  = raw.split("@")[1]
        company = domain.split(".")[0]
        print(f"  Detected email → using company name: {company}")
        return company
    # Strip common TLDs if someone pastes a bare domain
    if re.match(r"^[\w-]+\.\w{2,}$", raw):
        company = raw.split(".")[0]
        print(f"  Detected domain → using company name: {company}")
        return company
    return raw


def main():
    raw_input = input("Enter company name, email, or domain: ").strip()
    if not raw_input:
        sys.exit("No input provided.")

    company = normalise_input(raw_input)
    print(f"\n🔍 Researching: {company}\n{'─' * 60}")

    # ── Ollama health check ──
    if not check_ollama():
        sys.exit(
            f"[fatal] Cannot reach Ollama at {OLLAMA_URL}.\n"
        
        )

    # ── Gather research ──
    research_text, all_urls = gather(company)


    if not research_text:
        result = {"error": "No research data gathered", "sources": all_urls}
    else:
        print(f"\n  Extracting structured data with {OLLAMA_MODEL} …")
        result = extract(research_text, company)

        if result:
            result["sources"] = all_urls[:20]
        else:
            result = {"error": "Extraction failed", "sources": all_urls[:20]}

    # ── Output ──
    output = json.dumps(result, indent=2, ensure_ascii=False)
    print(f"\n{'═' * 60}")
    print(output)
    print(f"{'═' * 60}")

    fname = "new.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\n✅  Saved → {fname}")


if __name__ == "__main__":
    main()