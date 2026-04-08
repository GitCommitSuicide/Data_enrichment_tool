import json
import re
import sys
from modules.search import gather
from modules.extractor import check_ollama, extract
from modules.config import OLLAMA_URL, OLLAMA_MODEL

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
    print(f"\n  Saved → {fname}")


if __name__ == "__main__":
    main()
