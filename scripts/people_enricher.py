"""
people_enricher.py — Per-person OSINT enrichment (fully parallel)
"""
from __future__ import annotations
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from .search import fetch, scrape_url, extract_social_urls
from .extractor import call_ollama

_ENRICH_PROMPT = """\
You are an expert professional profile researcher.

Extract a detailed profile for ONLY "{person_name}" who works (or worked) at "{company_name}".
Use ONLY what is explicitly stated in the research text. NEVER hallucinate.

Return ONLY valid JSON (omit fields you cannot verify):
{{
  "name": "...",
  "current_title": "...",
  "nationality": "...",
  "linkedin_url": "https://linkedin.com/in/...",
  "contact": {{
    "email": ["full@domain.com"],
    "phone": ["+91 9876543210"]
  }},
  "experience": [
    {{"title":"...","company":"...","duration":"...","is_current":true}}
  ],
  "education": [
    {{"institution":"...","degree":"..."}}
  ],
  "skills": ["top 5 professional skills"],
  "summary": "3-sentence professional bio covering role, expertise, and key achievements."
}}

STRICT RULES:
1. Only extract what is EXPLICITLY stated. Never infer or guess.
2. Omit any field that is empty, unknown, or uncertain.
3. Target ONLY {person_name} at {company_name}. Discard same-name duplicates at other companies.
4. PHONES: real digits only (e.g. +91 9876543210). OMIT masked numbers with X/x/* placeholders.
5. EMAILS: full valid address only (e.g. john@company.com). OMIT partial or obfuscated emails.
6. LinkedIn URL: include ONLY if the URL demonstrably belongs to this exact person.
7. Experience: include current AND recent past roles. Mark current with is_current=true.
8. Output starts with {{ ends with }}.
"""

# ── Contact validation ────────────────────────────────────────────────────────

_PHONE_RE   = re.compile(r'^[\d\s\+\-\(\)]{7,20}$')
_PHONE_JUNK = re.compile(r'[xX]{2,}|[xX]\d|\d[xX]')

_EMAIL_VALID = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')
_EMAIL_JUNK  = re.compile(
    r'(example\.|test\.|noreply|no-reply|donotreply'
    r'|@capsitech|@sentry\.|@w3\.org|@schema\.|@xmlns\.'
    r'|\.png@|\.jpg@|\.svg@|\.css@|\.js@|xxxx|@x+\.)',
    re.IGNORECASE,
)


def _clean_phones(phones: list) -> list:
    out = []
    for p in phones:
        p = str(p).strip()
        if _PHONE_JUNK.search(p):
            continue
        if not _PHONE_RE.match(p):
            continue
        if len(re.sub(r'\D', '', p)) < 7:
            continue
        out.append(p)
    return out


def _clean_emails(emails: list) -> list:
    out = []
    for e in emails:
        e = str(e).strip().lower()
        if not _EMAIL_VALID.match(e):
            continue
        if _EMAIL_JUNK.search(e):
            continue
        out.append(e)
    return out


def _clean_contact(enriched: dict) -> dict:
    contact = enriched.get("contact", {})
    if "phone" in contact:
        contact["phone"] = _clean_phones(contact["phone"])
        if not contact["phone"]:
            del contact["phone"]
    if "email" in contact:
        contact["email"] = _clean_emails(contact["email"])
        if not contact["email"]:
            del contact["email"]
    if not contact:
        enriched.pop("contact", None)
    # Top-level phone/email
    if "phone" in enriched:
        cleaned = _clean_phones([enriched["phone"]] if isinstance(enriched["phone"], str) else enriched["phone"])
        enriched["phone"] = cleaned[0] if cleaned else None
    if "email" in enriched:
        cleaned = _clean_emails([enriched["email"]] if isinstance(enriched["email"], str) else enriched["email"])
        enriched["email"] = cleaned[0] if cleaned else None
    return enriched


# ── Single person enrichment ──────────────────────────────────────────────────

def _enrich_single(person_name: str, company: str, is_target: bool = False) -> dict:
    if not person_name or person_name.lower() in {"...", "unknown"}:
        return {}

    print(f"  [people_enricher] → {person_name} @ {company}{' (target)' if is_target else ''}")

    # Targeted queries — broad enough to find real info
    queries = [
        f'"{person_name}" "{company}" site:linkedin.com/in',
        f'"{person_name}" "{company}" profile OR experience OR background',
        f'"{person_name}" "{company}" (email OR phone OR contact) site:rocketreach.co OR site:apollo.io OR site:contactout.com',
    ]

    seen_urls: set[str]  = set()
    chunks:    list[str] = []

    # Run all 3 queries in parallel
    with ThreadPoolExecutor(max_workers=3) as pool:
        fetch_futs = {pool.submit(fetch, q, 3): q for q in queries}
        for fut in as_completed(fetch_futs):
            try:
                items = fut.result()
            except Exception:
                items = []
            for item in items:
                url = item["url"]
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                content = item["content"]
                if len(content) < 500:
                    scraped = scrape_url(url)
                    if len(scraped) > len(content):
                        content = scraped
                chunks.append(f"Source: {url}\n{content[:5_000]}\n")

    if not chunks:
        return {}

    research = "\n\n".join(chunks)
    socials  = extract_social_urls(research)
    if socials:
        research += "\n\nEXTRACTED URLS:\n" + "\n".join(socials)

    prompt   = _ENRICH_PROMPT.replace("{person_name}", person_name).replace("{company_name}", company)
    enriched = call_ollama(prompt, f"Target: {person_name}\nCompany: {company}\n\n{research}")

    if not enriched:
        return {}

    # Deduplicate experience by company
    if "experience" in enriched:
        seen_cos: dict[str, dict] = {}
        for exp in enriched["experience"]:
            key = exp.get("company", "").lower().strip()
            if key and key not in seen_cos:
                seen_cos[key] = exp
        enriched["experience"] = list(seen_cos.values()) or None

    # Deduplicate education
    if "education" in enriched:
        seen_deg: set[str] = set()
        unique = []
        for e in enriched["education"]:
            k = f"{e.get('institution','')}|{e.get('degree','')}".lower()
            if k not in seen_deg:
                seen_deg.add(k)
                unique.append(e)
        enriched["education"] = unique or None

    enriched = _clean_contact(enriched)

    # Strip education for non-target persons
    if not is_target:
        enriched.pop("education", None)

    # Always remove current_title to avoid duplication with title
    if "current_title" in enriched and "title" not in enriched:
        enriched["title"] = enriched.pop("current_title")
    else:
        enriched.pop("current_title", None)

    enriched = {k: v for k, v in enriched.items() if v not in (None, "", [], {})}
    enriched["sources"] = list(seen_urls)
    return enriched


# ── Merge helper ──────────────────────────────────────────────────────────────

def _merge(base: dict, enriched: dict) -> dict:
    """Merge enriched into base — existing real values win."""
    skip = {"name", "sources"}
    for key, val in enriched.items():
        if key in skip:
            continue
        existing = base.get(key)
        if existing in (None, "", [], {}):
            base[key] = val
        elif isinstance(existing, list) and isinstance(val, list):
            # Extend lists rather than replace (e.g. experience from multiple sources)
            seen = {str(x) for x in existing}
            for item in val:
                if str(item) not in seen:
                    existing.append(item)
                    seen.add(str(item))
    return base


# ── Main entry point ──────────────────────────────────────────────────────────

def enrich_people(company_data: dict, company_name: str) -> dict:
    """
    Enrich all leaders + target_person fully in parallel.
    Returns updated company_data.
    """
    print(f"\n  [people_enricher] Starting for {company_name} …")

    # Build work list — leaders + target_person
    to_enrich: list[tuple[str, int, str]] = []

    for idx, leader in enumerate(company_data.get("leadership", [])):
        name = leader.get("name", "")
        if name and name.lower() not in {"...", "unknown"}:
            to_enrich.append(("leadership", idx, name))

    tp = company_data.get("target_person", {})
    if tp and tp.get("name") and tp["name"].lower() not in {"...", "unknown"}:
        tp_name = tp["name"]
        # Check if target_person is already in leadership (avoid double-fetch)
        already_in_leadership = any(
            item[2].lower() == tp_name.lower()
            for item in to_enrich
            if item[0] == "leadership"
        )
        if not already_in_leadership:
            to_enrich.append(("target_person", -1, tp_name))

    if not to_enrich:
        print("  [people_enricher] No people to enrich.")
        return company_data

    # Deduplicate names
    seen_names: set[str] = set()
    deduped = []
    for item in to_enrich:
        _, _, nm = item
        if nm.lower() not in seen_names:
            seen_names.add(nm.lower())
            deduped.append(item)

    print(f"  [people_enricher] Enriching {len(deduped)} people in parallel …")

    results: dict[tuple[str, int], dict] = {}

    with ThreadPoolExecutor(max_workers=min(len(deduped), 8)) as pool:
        fut_map = {
            pool.submit(_enrich_single, name, company_name, section == "target_person"): (section, idx)
            for section, idx, name in deduped
        }
        for fut in as_completed(fut_map):
            section, idx = fut_map[fut]
            try:
                enriched = fut.result()
                if enriched:
                    results[(section, idx)] = enriched
            except Exception as e:
                name = next(nm for s, i, nm in deduped if (s, i) == (section, idx))
                print(f"  [people_enricher] Failed for {name}: {e}")

    # Apply results
    for section, idx, name in deduped:
        enriched = results.get((section, idx))
        if not enriched:
            print(f"  [people_enricher] No enrichment data for {name}")
            continue

        if section == "target_person":
            company_data["target_person"] = _merge(company_data["target_person"], enriched)
        else:
            company_data["leadership"][idx] = _merge(company_data["leadership"][idx], enriched)

        # If enriched person IS the target_person (was in leadership), also update target_person
        tp = company_data.get("target_person", {})
        if tp and tp.get("name", "").lower() == name.lower() and section == "leadership":
            company_data["target_person"] = _merge(company_data.get("target_person", {"name": name}), enriched)

    return company_data