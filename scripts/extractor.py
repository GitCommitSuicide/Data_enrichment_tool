"""
extractor.py — LLM extraction via Ollama
"""
from __future__ import annotations
import json, re, time, requests
from .config import OLLAMA_URL, OLLAMA_MODEL, OLLAMA_TIMEOUT

SYSTEM_PROMPT = """You are a strict, senior company intelligence analyst. Your job is to extract verified facts ONLY from the provided research text.

CRITICAL ANTI-HALLUCINATION RULES (read before anything else):
- NEVER invent, infer, or assume any fact not explicitly stated in the input text.
- If a value is not found in the text, omit the entire field — do NOT write "unknown", "N/A", "...", or any placeholder.
- If you are not 100% certain a fact belongs to this specific company/person, omit it.
- Do NOT copy data from your training memory. Only use what is in the research text provided.
- Verify every number, date, and name against the source text before including it.

IMPORTANT FIELD DISTINCTIONS:
- "cin_number": Indian CIN — exactly 21 alphanumeric chars (e.g. U72900MH2010PTC123456). Indian companies only.
- "company_number": UK Companies House registration — 8 digits or 2 letters + 6 digits. UK companies only.
- "gstin": Indian GST Identification Number — exactly 15 chars.
- "pan_number": Indian PAN — exactly 10 chars.
Never swap these identifiers. If unsure, omit.

{
  "company": {
    "name": "official trading name",
    "legal_name": "full registered legal name",
    "founded_year": 0,
    "company_number": "UK Companies House number (8 chars) — omit if Indian company",
    "cin_number": "Indian CIN (21 chars) — omit if UK company",
    "gstin": "15-char GSTIN — omit if not Indian",
    "pan_number": "10-char PAN — omit if not Indian",
    "company_type": "Private Limited / Public / LLP / etc.",
    "status": "Active / Dissolved / Struck off",
    "description": "Write 8-10 factual sentences covering: (1) what the company does and its core business model, (2) key products and services, (3) industries and verticals served, (4) founding story and key milestones, (5) geographic presence and markets, (6) technology capabilities and stack, (7) financial scale and growth trajectory, (8) notable achievements, partnerships or contracts, (9) company culture or employer brand, (10) future direction or strategic focus. Use ONLY facts from the research text.",
    "industry": ["primary industry categories — array of strings"],
    "sub_industry": ["specific sub-categories"],
    "headquarters": {"address": "street", "city": "...", "state": "...", "country": "...", "zip_code": "..."},
    "other_locations": [{"address": "...", "city": "...", "state": "...", "country": "...", "zip_code": "...", "type": "branch/office/registered"}],
    "website": "https://...",
    "employee_count_range": "e.g. 51-200",
    "jurisdiction": "country of primary registration",
    "business_type": "B2B / B2C / B2B2C"
  },
  "contact": {
    "email": ["verified work emails only — no free mail providers"],
    "phone": ["verified phone numbers with country code"],
    "social_media": {
      "linkedin": "company LinkedIn URL",
      "twitter": "Twitter/X URL",
      "facebook": "Facebook URL",
      "instagram": "Instagram URL"
    }
  },
  "leadership": [
    {
      "name": "full name",
      "title": "primary CURRENT job title at this company ONLY — e.g. Managing Director",
      "din": "Indian Director Identification Number if available",
      "appointment_date": "date appointed to this company",
      "nationality": "nationality if stated",
      "linkedin_url": "verified LinkedIn profile URL — must contain linkedin.com/in/",
      "experience": [
        {"title": "current role 1", "company": "...", "duration": "start - present", "is_current": true}
      ],
      "summary": "1-2 sentence bio: their current roles and main expertise at THIS company only. Do NOT list past employers."
    }
  ],
  "target_person": {
    "CONDITION": "ONLY include this key if the target person is explicitly mentioned in the research text with their name matching. If not found, omit the entire target_person key.",
    "name": "full name as found in source",
    "title": "current job title",
    "linkedin_url": "verified LinkedIn URL — must contain linkedin.com/in/",
    "email": "verified work email address",
    "phone": "verified phone with country code",
    "nationality": "if stated",
    "experience": [
      {"title": "...", "company": "...", "duration": "start - end or present", "is_current": true}
    ],
    "education": [
      {"institution": "...", "degree": "...", "year": "graduation year if known"}
    ],
    "skills": ["verified skills mentioned in source"],
    "certifications": ["verified certifications or courses completed"],
    "summary": "3-4 sentence bio covering: current role and responsibilities, educational background, key skills and achievements, and any notable projects or contributions."
  },
  "products_and_services": [
    {
      "name": "product or service name",
      "type": "Product or Service",
      "category": "category",
      "description": "2-3 sentence description of what it does and who uses it"
    }
  ],
  "financials": {
    "revenue": "exact figure or range with currency and year",
    "revenue_year": 0,
    "revenue_source": "source of this figure",
    "revenue_growth_pct": "YoY growth percentage if stated",
    "total_funding": "total funding raised",
    "valuation": "company valuation if known",
    "authorized_capital": "authorized share capital",
    "paid_up_capital": "paid-up share capital",
    "EBITDA_growth_percent": 0.0,
    "net_worth_growth_percent": 0.0,
    "funding_status": "Bootstrapped / Seed / Series A / etc."
  },
  "funding": [
    {
      "round_name": "Seed / Series A / etc.",
      "amount": "amount with currency",
      "date": "date of funding",
      "lead_investors": ["investor names"],
      "source": "source URL or publication"
    }
  ],
  "structure": {
    "parent_company": "parent company name if any",
    "subsidiaries": ["ALL subsidiary and group company names found"],
    "divisions": ["business divisions if any"],
    "sister_companies": ["sister company names"]
  },
  "technology": {
    "tech_stack": ["programming languages, frameworks, databases explicitly mentioned"],
    "cloud_providers": ["AWS / Azure / GCP etc."],
    "integrations": ["third-party integrations"],
    "development_practices": ["Agile / DevOps / CI/CD etc. if mentioned"]
  },
  "market_intelligence": {
    "target_customers": ["specific customer segments"],
    "target_geographies": ["countries or regions served"],
    "key_competitors": [{"name": "...", "website": "..."}],
    "differentiators": ["specific competitive advantages stated in source"],
    "certifications": ["ISO, CMMI, or other certifications"],
    "partnerships": ["named partner companies or government bodies"]
  },
  "recent_news_and_updates": [
    {
      "title": "headline",
      "summary": "2-3 sentence factual summary of the news item",
      "date": "publication date",
      "source": "publication name",
      "url": "source URL"
    }
  ],
  "key_achievements": [
    {
      "achievement": "short title",
      "date": "date or year",
      "description": "one sentence explaining the achievement and its significance"
    }
  ],
  "reviews_and_ratings": [
    {
      "platform": "Glassdoor / Trustpilot / G2 / AmbitionBox",
      "rating": "numeric rating",
      "review_summary": "brief summary of common themes in reviews",
      "total_reviews": "number of reviews",
      "url": "platform URL"
    }
  ],
  "additional_info": {
    "incorporation_date": "full date of incorporation",
    "business_model": "brief description of revenue model",
    "annual_general_meeting": "date of last AGM",
    "registrar": "registrar of companies"
  },
  "sources": ["all URLs from which data was extracted"]
}

OUTPUT RULES:
1. ONLY extract what is EXPLICITLY stated in the input text. Zero hallucination tolerated.
2. Omit any field that is empty, null, [], or {}. Do NOT write placeholders.
3. company_number vs cin_number: Never confuse. UK=company_number, India=cin_number.
4. LinkedIn URLs: include ONLY if the URL is complete and explicitly in the source (contains linkedin.com/in/ for people, linkedin.com/company/ for companies).
5. All arrays must be JSON arrays, never comma-separated strings.
6. products_and_services: max 7 most important, with real descriptions.
7. key_competitors: max 5 real direct competitors. NEVER list Microsoft, Google, AWS etc. unless the text explicitly says they compete.
8. recent_news_and_updates: include ALL news items found from 2023-2026.
9. key_achievements: include ALL milestones and achievements explicitly mentioned.
10. leadership: include ONLY director/C-suite/VP level people. Each entry has ONLY their current active roles (max 3) in the experience array — NO past employers, NO education, NO skills.
11. target_person: ONLY add this key if the target person's name is explicitly found in the research text. If not found, DO NOT include target_person at all.
12. Output valid JSON only — starts with { and ends with }. No markdown code fences.
13. sources: list every URL from which you extracted at least one fact.
"""


def _parse_json(raw: str) -> dict:
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw).strip()
    raw = re.sub(r",\s*([\]}])", r"\1", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Brace-matching fallback
    start = raw.find("{")
    if start == -1:
        return {}
    depth = end = 0
    for i, ch in enumerate(raw[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if not end:
        return {}
    try:
        return json.loads(raw[start:end])
    except json.JSONDecodeError as e:
        print(f"  [warn] JSON parse failed: {e}")
        return {}


def check_ollama() -> bool:
    base = OLLAMA_URL.replace("/api/generate", "").replace("/api/chat", "")
    try:
        return requests.get(base, timeout=5).status_code == 200
    except requests.exceptions.ConnectionError:
        return False


def call_ollama(system_prompt: str, user_content: str, retries: int = 2) -> dict:
    for attempt in range(1, retries + 2):
        try:
            resp = requests.post(
                OLLAMA_URL,
                json={
                    "model":   OLLAMA_MODEL,
                    "prompt":  user_content,
                    "system":  system_prompt,
                    "stream":  False,
                    "options": {"temperature": 0.0},
                },
                timeout=OLLAMA_TIMEOUT,
            )
            resp.raise_for_status()
            data   = resp.json()
            raw    = data.get("response", "").strip()
            pt, rt = data.get("prompt_eval_count", 0), data.get("eval_count", 0)
            print(f"  [tokens] prompt={pt}  response={rt}  total={pt+rt}")
            result = _parse_json(raw)
            if result:
                return result
            print(f"  [warn] attempt {attempt}: empty JSON")
        except requests.exceptions.Timeout:
            print(f"  [warn] attempt {attempt}: timeout ({OLLAMA_TIMEOUT}s)")
        except requests.exceptions.RequestException as e:
            print(f"  [error] attempt {attempt}: {e}")
            break
        if attempt <= retries:
            time.sleep(2)
    return {}


def extract(
    research_text: str,
    company:       str,
    person_name:   str = "",
    person_email:  str = "",
) -> dict:
    print(f"  [extractor] Extracting data for: {company} …")
    user_content  = f"Company: {company}\n"
    if person_name:
        user_content += f"Target Person Name: {person_name}\n"
        if person_email:
            user_content += f"Target Person Email: {person_email}\n"
        user_content += (
            "\nIMPORTANT: Search the research text for the target person '"
            + person_name
            + "'. If and ONLY IF you find this person explicitly mentioned by name, "
            "extract their full profile under 'target_person'. "
            "If the target person is NOT mentioned in the text, DO NOT add target_person key at all.\n"
        )
    else:
        user_content += "Target Person: None — do NOT add target_person key.\n"

    user_content += f"\nResearch Document:\n{research_text}"

    result = call_ollama(SYSTEM_PROMPT, user_content)
    if result:
        # Post-process: normalise current_title → title on target_person
        tp = result.get("target_person", {})
        if isinstance(tp, dict):
            # Remove the CONDITION metadata key if model echoed it
            tp.pop("CONDITION", None)
            if "current_title" in tp and "title" not in tp:
                tp["title"] = tp.pop("current_title")
            elif "current_title" in tp:
                tp.pop("current_title")
            # If target_person is empty after cleanup, remove it
            if not tp or tp == {}:
                result.pop("target_person", None)
            else:
                result["target_person"] = tp

        # Normalise leadership: remove experience/education, keep only current title
        for leader in result.get("leadership", []):
            leader.pop("experience", None)
            leader.pop("education", None)
            leader.pop("current_title", None)
            leader.pop("CONDITION", None)

        print("  [extractor] Done.")
    else:
        print("  [extractor] Failed.")
    return result