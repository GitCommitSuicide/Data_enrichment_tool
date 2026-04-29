"""
main.py
───────
Pipeline:
  1. Gather raw research via Tavily  → saved as <company>_tavily.json
  2. Extract structured data via LLM → saved as <company>_extracted.json
  3. Enrich people profiles           → saved as <company>_people.json
  4. Enrich company data gaps         → saved as <company>_enriched.json
  5. Combine all into final output    → saved as <company>_final.json
"""
from __future__ import annotations

import json
import os
import re
import sys
import time

from scripts.config import OLLAMA_URL, OLLAMA_MODEL
from scripts.extractor import check_ollama, extract
from scripts.search import gather
from scripts.cleaner import clean_output
from scripts.validator import validate_all_people

# ── Constants ─────────────────────────────────────────────────────────────────

OUTPUT_DIR  = "data_scraped"
FREE_MAIL   = frozenset(["gmail", "yahoo", "hotmail", "outlook", "protonmail"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _save(data: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    print(f"  💾  Saved → {os.path.abspath(path)}")


def _safe(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name)


def normalise_input(raw: str) -> tuple[str, str, str]:
    """Return (company, person_name, person_email) from raw input."""
    raw = raw.strip()

    if "@" in raw:
        username, domain = raw.split("@", 1)
        domain_root = domain.split(".")[0].lower()
        if domain_root in FREE_MAIL:
            sys.exit("[error] Please enter a work email — free mail providers not supported.")
        company     = domain_root
        person_name = re.sub(r"[^a-zA-Z]+", " ", username).strip().title()
        print(f"  Detected email → company: {company}  |  person: {person_name}")
        return company, person_name, raw

    if re.match(r"^[\w-]+\.\w{2,}$", raw):
        company = raw.split(".")[0]
        print(f"  Detected domain → company: {company}")
        return company, "", ""

    return raw, "", ""


def _merge_people_into_data(company_data: dict, people_data: dict) -> dict:
    """
    Merge enriched people profiles back into company_data.
    people_data keys: "leadership" list + "target_person" dict.
    """
    if "leadership" in people_data and "leadership" in company_data:
        enriched_by_name = {
            p.get("name", "").lower(): p
            for p in people_data["leadership"]
        }
        for i, leader in enumerate(company_data["leadership"]):
            key = leader.get("name", "").lower()
            if key in enriched_by_name:
                enriched = enriched_by_name[key]
                for field, val in enriched.items():
                    if field == "name":
                        continue
                    if company_data["leadership"][i].get(field) in (None, "", [], {}):
                        company_data["leadership"][i][field] = val

    if "target_person" in people_data and "target_person" in company_data:
        for field, val in people_data["target_person"].items():
            if field == "name":
                continue
            if company_data["target_person"].get(field) in (None, "", [], {}):
                company_data["target_person"][field] = val

    return company_data


def _combine(
    tavily_data:    dict,
    extracted_data: dict,
    people_data:    dict,
    enriched_data:  dict,
) -> dict:
    """
    Combine all pipeline outputs into one final JSON.
    Priority (highest → lowest): enriched > people > extracted > tavily raw.
    """
    # Start from enriched (most complete), merge missing from earlier stages
    final = dict(enriched_data)

    # Merge people enrichment directly (already done in pipeline, but re-apply for safety)
    final = _merge_people_into_data(final, people_data)

    # Fill any top-level gaps from extracted
    for key, val in extracted_data.items():
        if key not in final or final[key] in (None, "", [], {}):
            final[key] = val

    # Consolidate all source URLs
    all_sources: list[str] = []
    seen_sources: set[str] = set()
    for pool in [
        enriched_data.get("sources", []),
        people_data.get("sources", []),
        extracted_data.get("additional_info", {}).get("data_sources", []),
        tavily_data.get("urls", []),
    ]:
        for u in pool:
            if isinstance(u, str) and u not in seen_sources:
                seen_sources.add(u)
                all_sources.append(u)

    final["sources"] = all_sources[:50]

    # Add pipeline metadata
    final["_pipeline"] = {
        "tavily_urls_count":   len(tavily_data.get("urls", [])),
        "extracted":           bool(extracted_data),
        "people_enriched":     len(people_data.get("leadership", [])),
        "enrichment_sources":  len(enriched_data.get("sources", [])),
    }

    return final


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    raw = input("Enter company name, email, or domain: ").strip()
    if not raw:
        sys.exit("[error] No input provided.")

    company, person_name, person_email = normalise_input(raw)
    slug = _safe(company)
    if person_name:
        slug += f"_{_safe(person_name)}"

    print(f"\n🔍  Researching: {company}"
          f"{f'  (target: {person_name})' if person_name else ''}\n"
          f"{'─' * 60}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Health check ──────────────────────────────────────────────────────────
    if not check_ollama():
        sys.exit(
            f"[fatal] Cannot reach Ollama at {OLLAMA_URL}.\n"
            f"        Make sure Ollama is running and {OLLAMA_MODEL} is loaded."
        )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — Gather raw research via Tavily
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}\nSTEP 1 — Gathering research (Tavily)\n{'─'*60}")
    t0 = time.time()
    research_text, all_urls = gather(company, person_name)
    print(f"  Gathered {len(all_urls)} sources in {time.time()-t0:.1f}s")

    tavily_data: dict = {"urls": all_urls, "text_length": len(research_text)}
    _save(tavily_data, os.path.join(OUTPUT_DIR, f"{slug}_tavily.json"))

    if not research_text:
        sys.exit("[error] No research data gathered — all queries returned empty.")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — LLM extraction
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}\nSTEP 2 — Extracting structured data (LLM)\n{'─'*60}")
    t0 = time.time()
    extracted_data = extract(research_text, company, person_name, person_email)
    print(f"  Extraction done in {time.time()-t0:.1f}s")

    if not extracted_data:
        sys.exit("[error] LLM extraction failed — got empty result.")

    extracted_data = clean_output(extracted_data)
    _save(extracted_data, os.path.join(OUTPUT_DIR, f"{slug}_extracted.json"))

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — People enrichment
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}\nSTEP 3 — People enrichment\n{'─'*60}")
    from scripts.people_enricher import enrich_people

    # Work on a copy so we can save people data separately
    import copy
    people_input  = copy.deepcopy(extracted_data)
    people_output = enrich_people(people_input, company)

    # Build people-only snapshot
    people_data: dict = {
        "leadership":    people_output.get("leadership", []),
        "target_person": people_output.get("target_person", {}),
        "sources":       [],
    }
    # Collect sources touched during people enrichment
    for leader in people_data["leadership"]:
        for src in leader.get("sources", []):
            if src not in people_data["sources"]:
                people_data["sources"].append(src)

    _save(people_data, os.path.join(OUTPUT_DIR, f"{slug}_people.json"))

    # Merge people results back into working copy
    extracted_with_people = _merge_people_into_data(copy.deepcopy(extracted_data), people_data)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3.5 — LinkedIn cross-validation
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}\nSTEP 3.5 — LinkedIn validation\n{'─'*60}")
    extracted_with_people = validate_all_people(extracted_with_people, company)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — Company enrichment (gap-filling)
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}\nSTEP 4 — Company enrichment\n{'─'*60}")
    from scripts.enricher import enrich

    enriched_data = enrich(extracted_with_people, company)
    enriched_data = clean_output(enriched_data)
    _save(enriched_data, os.path.join(OUTPUT_DIR, f"{slug}_enriched.json"))

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5 — Combine into final output
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}\nSTEP 5 — Combining all outputs\n{'─'*60}")
    final = _combine(tavily_data, extracted_data, people_data, enriched_data)
    final = clean_output(final)

    final_path = os.path.join(OUTPUT_DIR, f"{slug}_final.json")
    _save(final, final_path)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"  ✅  Pipeline complete for: {company}")
    print(f"{'═'*60}")
    print(f"  📄  Tavily raw    → {slug}_tavily.json    ({len(all_urls)} URLs)")
    print(f"  📄  Extracted     → {slug}_extracted.json")
    print(f"  📄  People        → {slug}_people.json    ({len(people_data['leadership'])} leaders)")
    print(f"  📄  Enriched      → {slug}_enriched.json  ({len(enriched_data.get('sources',[]))} sources)")
    print(f"  📄  Final         → {slug}_final.json     ← combined output")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    
    start = time.time()
    main()
    end = time.time()
    print(f"Total time: {end - start:.2f} seconds")