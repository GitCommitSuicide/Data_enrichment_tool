"""
people_enricher.py — Per-person OSINT enrichment
Saves its own JSON to data_scraped/<company>_people.json
"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from .search import fetch, scrape_url, extract_social_urls
from .extractor import call_ollama

_ENRICH_PROMPT = """\
You are an expert profile enricher.

Extract a detailed profile for the ONE person named "{person_name}" who works (or worked) at "{company_name}".
Use ONLY what is explicitly in the research text. Do NOT hallucinate.

Return ONLY valid JSON (omit unknown fields):
{
  "name": "...",
  "current_title": "...",
  "nationality": "...",
  "linkedin_url": "https://linkedin.com/in/...",
  "contact": { "email": ["full@domain.com"], "phone": ["+91 9876543210"] },
  "experience": [{ "title":"...","company":"...","duration":"...","is_current":true }],
  "education": [{ "institution":"...","degree":"..." }],
  "skills (TOP 5 ONLY)": ["..."],
  "summary": "Two-sentence professional bio ."
}

STRICT RULES:
1. Only extract what is explicitly stated. Never guess or infer.
2. Omit any field that is empty, unknown, or uncertain.
3. Target ONLY {person_name} at {company_name} — discard same-name duplicates.
4. PHONES: real digits only (e.g. +91 9876543210). NEVER include masked/placeholder
   numbers containing X, x, *, or # (e.g. +1 (XXX) XXX-XXXX → OMIT entirely).
5. EMAILS: full valid address only (e.g. john@company.com). NEVER include partial,
   obfuscated, or placeholder emails (e.g. j***@c***.com, user@capsitech → OMIT).
6. LinkedIn URL: include ONLY if the URL demonstrably belongs to this exact person.
7. Experience: current roles only (is_current=true). One entry per company.
8. Output starts with {{ ends with }}.
"""


import re

# Valid phone: digits/spaces/+-(), no X/x placeholders, min 7 digits
_PHONE_RE    = re.compile(r'^[\d\s\+\-\(\)]{7,20}$')
_PHONE_JUNK  = re.compile(r'[xX]{2,}|[xX]\d|\d[xX]')   # XX, x1, 1x patterns

# Valid email: standard format, reject placeholder domains / obfuscated locals
_EMAIL_VALID = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')
_EMAIL_JUNK  = re.compile(
    r'(example\.|test\.|noreply|no-reply|donotreply'
    r'|@capsitech|@sentry\.|@w3\.org|@schema\.|@xmlns\.'
    r'|\.png@|\.jpg@|\.svg@|\.css@|\.js@|xxxx|@x+\.)',
    re.IGNORECASE
)


def _clean_phones(phones: list) -> list:
    out = []
    for p in phones:
        p = str(p).strip()
        if _PHONE_JUNK.search(p):        # contains X placeholders
            continue
        if not _PHONE_RE.match(p):       # non-standard format
            continue
        digits = re.sub(r'\D', '', p)
        if len(digits) < 7:              # too short to be real
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
    """Strip invalid phones and emails anywhere in the profile."""
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
    # Also clean top-level email/phone if present
    if "phone" in enriched:
        cleaned = _clean_phones([enriched["phone"]] if isinstance(enriched["phone"], str) else enriched["phone"])
        enriched["phone"] = cleaned[0] if cleaned else None
    if "email" in enriched:
        cleaned = _clean_emails([enriched["email"]] if isinstance(enriched["email"], str) else enriched["email"])
        enriched["email"] = cleaned[0] if cleaned else None
    return enriched


def _enrich_single(person_name: str, company: str) -> dict:
    if not person_name or person_name.lower() in {"...", "unknown"}:
        return {}

    print(f"  [people_enricher] {person_name} @ {company}")

    queries = [
        f'"{person_name}" "{company}" (email OR phone OR "contact details") site:linkedin.com OR site:rocketreach.co OR site:apollo.io',
        f'"{person_name}" "{company}" profile experience education site:linkedin.com/in OR site:crunchbase.com',
    ]

    seen_urls: set[str] = set()
    chunks: list[str] = []

    for q in queries:
        for item in fetch(q)[:3]:
            url = item["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)
            content = item["content"]
            if len(content) < 500:
                scraped = scrape_url(url)
                if len(scraped) > len(content):
                    content = scraped
            chunks.append(f"Source: {url}\n{content[:6_000]}\n")

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

    # Keep only current roles, deduplicate by company
    if "experience" in enriched:
        current = [e for e in enriched["experience"] if e.get("is_current")]
        by_co: dict[str, dict] = {}
        for exp in current:
            key = exp.get("company", "").lower().strip()
            if key and key not in by_co:
                by_co[key] = exp
        enriched["experience"] = list(by_co.values()) or None

    # Deduplicate education
    if "education" in enriched:
        seen_deg: set[str] = set()
        unique = []
        for e in enriched["education"]:
            k = f"{e.get('institution','')}|{e.get('degree','')}".lower()
            if k not in seen_deg:
                seen_deg.add(k); unique.append(e)
        enriched["education"] = unique or None

    # Strip invalid phones / emails
    enriched = _clean_contact(enriched)

    enriched = {k: v for k, v in enriched.items() if v not in (None, "", [], {})}
    enriched["sources"] = list(seen_urls)
    return enriched


def _merge(base: dict, enriched: dict) -> dict:
    """Merge enriched into base — base values win, never overwrite real data."""
    skip = {"name", "sources"}
    for key, val in enriched.items():
        if key in skip:
            continue
        if base.get(key) in (None, "", [], {}):
            base[key] = val
    return base


def enrich_people(company_data: dict, company_name: str) -> dict:
    """Enrich all leaders + target_person in parallel. Returns updated company_data."""
    print(f"\n  [people_enricher] Starting for {company_name} …")

    to_enrich: list[tuple[str, int, str]] = []

    for idx, leader in enumerate(company_data.get("leadership", [])):
        name = leader.get("name", "")
        if name and name.lower() not in {"...", "unknown"}:
            to_enrich.append(("leadership", idx, name))

    tp = company_data.get("target_person", {})
    if tp and tp.get("name") and tp["name"].lower() not in {"...", "unknown"}:
        to_enrich.append(("target_person", -1, tp["name"]))

    if not to_enrich:
        print("  [people_enricher] No people to enrich.")
        return company_data

    # Deduplicate by name
    seen: set[str] = set()
    deduped = []
    for item in to_enrich:
        _, _, nm = item
        if nm.lower() not in seen:
            seen.add(nm.lower()); deduped.append(item)

    print(f"  [people_enricher] Enriching {len(deduped)} people in parallel …")

    # Run all enrichments in parallel
    # Results keyed by (section, idx) so we can apply them in order
    results: dict[tuple[str, int], dict] = {}

    with ThreadPoolExecutor(max_workers=len(deduped)) as pool:
        fut_map = {
            pool.submit(_enrich_single, name, company_name): (section, idx)
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

    # Apply results in original order (leadership by idx, target_person last)
    for section, idx, name in deduped:
        enriched = results.get((section, idx))
        if not enriched:
            continue
        if section == "target_person":
            company_data["target_person"] = _merge(company_data["target_person"], enriched)
        else:
            company_data["leadership"][idx] = _merge(company_data["leadership"][idx], enriched)

    return company_data