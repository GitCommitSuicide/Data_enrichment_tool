"""
people_enricher.py — Per-person OSINT enrichment (fully parallel)
"""
from __future__ import annotations
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from .search import fetch, scrape_url, extract_social_urls
from .extractor import call_ollama

# ── Prompt: decision makers (concise — current role only) ─────────────────────

_LEADER_PROMPT = """\
You are an expert professional profile researcher. Extract a verified profile for ONLY "{person_name}" who holds a director/C-suite role at "{company_name}".

CRITICAL ANTI-HALLUCINATION RULES:
- NEVER invent or infer any fact not explicitly stated in the research text.
- If a field is not clearly stated for THIS person at THIS company, omit it entirely.
- Do NOT use your training memory — only extract from the research text below.

Return ONLY valid JSON. Omit fields you cannot verify from the text:
{{
  "name": "...",
  "title": "primary current title at {company_name}",
  "nationality": "only if explicitly stated",
  "linkedin_url": "https://linkedin.com/in/... — only if found in text",
  "experience": [
    {{"title": "...", "company": "...", "duration": "start - present", "is_current": true}}
  ],
  "summary": "1-2 sentence bio covering their current roles and expertise. Do NOT list past employers."
}}

STRICT RULES:
1. Only extract what is EXPLICITLY stated in the research text.
2. Omit any field that is empty, unknown, or uncertain.
3. Target ONLY {person_name} at {company_name}. Discard data about same-name people at other companies.
4. LinkedIn URL: include ONLY if the URL demonstrably belongs to this exact person (verify name matches).
5. DO NOT include past roles, education, skills, or contact details for decision makers. ONLY list current active roles in the experience array (up to 3).
6. Output starts with {{ and ends with }}.
"""

# ── Prompt: target person (full deep-dive) ────────────────────────────────────

_TARGET_PROMPT = """\
You are an expert professional profile researcher. Extract a COMPLETE, detailed profile for ONLY "{person_name}" who works at "{company_name}".

CRITICAL ANTI-HALLUCINATION RULES:
- NEVER invent or infer any fact not explicitly stated in the research text.
- If a field is not clearly stated for THIS person, omit it entirely.
- Do NOT use training memory — only use facts from the research text provided.
- If you cannot confirm this person works at {company_name}, return an empty object {{}}.

FIRST CHECK: Does the research text explicitly mention "{person_name}" by name? If NO, return {{}}.

If YES, extract everything you can find:
{{
  "name": "...",
  "title": "current job title",
  "nationality": "only if stated",
  "linkedin_url": "https://linkedin.com/in/... — only if confirmed to be this person",
  "contact": {{
    "email": ["verified full email addresses only — no partial/obfuscated"],
    "phone": ["+country-code number — no masked digits"]
  }},
  "experience": [
    {{"title": "...", "company": "...", "duration": "start - end or present", "is_current": true}}
  ],
  "education": [
    {{"institution": "...", "degree": "...", "year": "graduation year if known"}}
  ],
  "skills": ["skills explicitly mentioned in source"],
  "certifications": ["verified certifications or courses"],
  "summary": "3-4 sentence bio: their current role and what they do, educational background, key skills/certifications, and any notable projects or achievements."
}}

STRICT RULES:
1. Only extract what is EXPLICITLY stated. Never infer or guess.
2. Omit any field that is empty, unknown, or uncertain.
3. Target ONLY {person_name} at {company_name}. Ignore same-name people at other organisations.
4. PHONES: real digits only (e.g. +91 9876543210). OMIT any number with X/x/* placeholders.
5. EMAILS: full valid address only. OMIT partial or obfuscated emails.
6. LinkedIn URL: include ONLY if the URL demonstrably belongs to this exact person.
7. Output starts with {{ and ends with }}.
"""

# ── Contact validation ─────────────────────────────────────────────────────────

_PHONE_RE   = re.compile(r'^[\d\s\+\-\(\)]{7,20}$')
_PHONE_JUNK = re.compile(r'[xX]{2,}|[xX]\d|\d[xX]')

_EMAIL_VALID = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')
_EMAIL_JUNK  = re.compile(
    r'(example\.|test\.|noreply|no-reply|donotreply'
    r'|@sentry\.|@w3\.org|@schema\.|@xmlns\.'
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


# ── Single person enrichment ───────────────────────────────────────────────────

def _enrich_single(person_name: str, company: str, is_target: bool = False) -> dict:
    if not person_name or person_name.lower() in {"...", "unknown"}:
        return {}

    print(f"  [people_enricher] → {person_name} @ {company}{' (target)' if is_target else ''}")

    if is_target:
        # Broader queries for target person
        queries = [
            f'"{person_name}" "{company}" site:linkedin.com/in',
            f'"{person_name}" "{company}" profile OR background OR bio OR resume',
            f'"{person_name}" "{company}" (email OR phone OR contact)',
            f'"{person_name}" "{company}" certification OR skills OR education',
        ]
    else:
        # Concise queries for decision makers
        queries = [
            f'"{person_name}" "{company}" site:linkedin.com/in',
            f'"{person_name}" "{company}" director OR CEO OR founder OR managing',
        ]

    seen_urls: set[str]  = set()
    chunks:    list[str] = []

    with ThreadPoolExecutor(max_workers=len(queries)) as pool:
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

    # Choose prompt based on role
    if is_target:
        prompt = (
            _TARGET_PROMPT
            .replace("{person_name}", person_name)
            .replace("{company_name}", company)
        )
    else:
        prompt = (
            _LEADER_PROMPT
            .replace("{person_name}", person_name)
            .replace("{company_name}", company)
        )

    enriched = call_ollama(prompt, f"Research for: {person_name} at {company}\n\n{research}")

    if not enriched:
        return {}

    # Reject result if name doesn't match (anti-hallucination guard)
    found_name = enriched.get("name", "")
    if found_name and person_name.lower() not in found_name.lower() and found_name.lower() not in person_name.lower():
        print(f"  [people_enricher] Name mismatch for {person_name}: got '{found_name}' — discarding")
        return {}

    # For decision makers: strip everything except allowed fields
    if not is_target:
        allowed = {"name", "title", "nationality", "linkedin_url", "summary", "experience"}
        enriched = {k: v for k, v in enriched.items() if k in allowed}

    # For target person: clean experience, deduplicate education
    if is_target:
        if "experience" in enriched:
            seen_cos: dict[str, dict] = {}
            for exp in enriched["experience"]:
                key = exp.get("company", "").lower().strip()
                if key and key not in seen_cos:
                    seen_cos[key] = exp
            enriched["experience"] = list(seen_cos.values()) or None

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

    # Normalise current_title → title
    if "current_title" in enriched and "title" not in enriched:
        enriched["title"] = enriched.pop("current_title")
    else:
        enriched.pop("current_title", None)

    enriched = {k: v for k, v in enriched.items() if v not in (None, "", [], {})}
    enriched["sources"] = list(seen_urls)
    return enriched


# ── Merge helper ───────────────────────────────────────────────────────────────

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
            seen = {str(x) for x in existing}
            for item in val:
                if str(item) not in seen:
                    existing.append(item)
                    seen.add(str(item))
    return base


# ── Main entry point ───────────────────────────────────────────────────────────

def enrich_people(company_data: dict, company_name: str) -> dict:
    """
    Enrich all leaders + target_person fully in parallel.
    Returns updated company_data.

    Decision makers: concise — current role/title/summary/LinkedIn only.
    Target person: full deep-dive — experience, education, skills, certifications, contact.
    """
    print(f"\n  [people_enricher] Starting for {company_name} …")

    # Build work list
    to_enrich: list[tuple[str, int, str, bool]] = []

    for idx, leader in enumerate(company_data.get("leadership", [])):
        name = leader.get("name", "")
        if name and name.lower() not in {"...", "unknown"}:
            to_enrich.append(("leadership", idx, name, False))

    tp = company_data.get("target_person", {})
    tp_name = ""
    if isinstance(tp, dict) and tp.get("name") and tp["name"].lower() not in {"...", "unknown"}:
        tp_name = tp["name"]
        # Check if target is already in leadership
        already_in_leadership = any(
            item[2].lower() == tp_name.lower()
            for item in to_enrich
            if item[0] == "leadership"
        )
        if not already_in_leadership:
            to_enrich.append(("target_person", -1, tp_name, True))

    if not to_enrich:
        print("  [people_enricher] No people to enrich.")
        return company_data

    # Deduplicate names
    seen_names: set[str] = set()
    deduped = []
    for item in to_enrich:
        _, _, nm, is_target = item
        if nm.lower() not in seen_names:
            seen_names.add(nm.lower())
            deduped.append(item)

    print(f"  [people_enricher] Enriching {len(deduped)} people in parallel …")

    results: dict[tuple[str, int], dict] = {}

    with ThreadPoolExecutor(max_workers=min(len(deduped), 8)) as pool:
        fut_map = {
            pool.submit(_enrich_single, name, company_name, is_target): (section, idx)
            for section, idx, name, is_target in deduped
        }
        for fut in as_completed(fut_map):
            section, idx = fut_map[fut]
            try:
                enriched = fut.result()
                if enriched:
                    results[(section, idx)] = enriched
            except Exception as e:
                name = next(nm for s, i, nm, _ in deduped if (s, i) == (section, idx))
                print(f"  [people_enricher] Failed for {name}: {e}")

    # Apply results
    for section, idx, name, is_target in deduped:
        enriched = results.get((section, idx))
        if not enriched:
            print(f"  [people_enricher] No enrichment data for {name}")
            continue

        if section == "target_person":
            tp_existing = company_data.get("target_person", {})
            if not isinstance(tp_existing, dict):
                tp_existing = {}
            company_data["target_person"] = _merge(tp_existing, enriched)
        else:
            company_data["leadership"][idx] = _merge(company_data["leadership"][idx], enriched)

        # If enriched leader IS the target person, also update target_person
        if section == "leadership" and tp_name and tp_name.lower() == name.lower():
            tp_existing = company_data.get("target_person", {"name": name})
            if not isinstance(tp_existing, dict):
                tp_existing = {"name": name}
            company_data["target_person"] = _merge(tp_existing, enriched)

    return company_data