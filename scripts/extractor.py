"""
extractor.py — LLM extraction via Ollama
"""
from __future__ import annotations
import json, re, time, requests
from .config import OLLAMA_URL, OLLAMA_MODEL, OLLAMA_TIMEOUT

SYSTEM_PROMPT = """You are a strict company intelligence extractor.

Extract every meaningful fact from the raw research text into ONE structured JSON object.
Follow the schema below. Omit any key whose value is truly empty/null/unknown.

IMPORTANT DISTINCTIONS:
- "cin_number": Indian CIN — exactly 21 alphanumeric chars (e.g. U72900MH2010PTC123456). Indian companies only.
- "company_number": UK Companies House registration number — typically 8 digits or 2 letters + 6 digits (e.g. 14296520 or SC123456). UK/UK-registered companies only.
- "gstin": Indian GST Identification Number — exactly 15 chars.
- "pan_number": Indian PAN — exactly 10 chars.
Never swap these. If a number doesn't fit the format, omit it.

{
  "company": {
    "name": "...",
    "legal_name": "...",
    "founded_year": 0,
    "company_number": "UK Companies House number (8 chars)",
    "cin_number": "Indian CIN (21 chars) — omit if UK company",
    "gstin": "15-char GSTIN — omit if not Indian",
    "pan_number": "10-char PAN — omit if not Indian",
    "company_type": "...",
    "status": "Active/Dissolved/etc",
    "description": "4-6 sentence factual description covering what the company does, who it serves, its key differentiators, and history",
    "industry": ["..."],
    "sub_industry": ["..."],
    "headquarters": {"address":"...","city":"...","state":"...","country":"...","zip_code":"..."},
    "other_locations": [{"address":"...","city":"...","state":"...","country":"...","type":"branch/office/registered"}],
    "website": "...",
    "employee_count_range": "...",
    "jurisdiction": "...",
    "business_type": "B2B/B2C/B2B2C"
  },
  "contact": {
    "email": ["..."],
    "phone": ["..."],
    "social_media": {"linkedin":"...","twitter":"...","facebook":"...","instagram":"..."}
  },
  "leadership": [
    {
      "name": "...",
      "title": "...",
      "din": "Indian DIN number if available",
      "appointment_date": "...",
      "nationality": "...",
      "linkedin_url": "full verified URL only",
      "contact": {"email": ["..."], "phone": ["..."]},
      "experience": [{"title":"...","company":"...","duration":"...","is_current":true}],
      "skills": ["top 5 skills"],
      "summary": "2-sentence professional bio"
    }
  ],
  "target_person": {
    "name": "...",
    "current_title": "...",
    "linkedin_url": "...",
    "email": "...",
    "phone": "...",
    "nationality": "...",
    "experience": [{"title":"...","company":"...","duration":"...","is_current":true}],
    "education": [{"institution":"...","degree":"..."}],
    "skills": ["..."],
    "summary": "3-sentence professional bio including key achievements"
  },
  "products_and_services": [
    {"name":"...","type":"Product or Service","category":"...","description":"..."}
  ],
  "financials": {
    "revenue": "...",
    "revenue_year": 0,
    "revenue_source": "...",
    "total_funding": "...",
    "valuation": "...",
    "authorized_capital": "...",
    "paid_up_capital": "...",
    "funding_status": "..."
  },
  "funding": [
    {"round_name":"...","amount":"...","date":"...","lead_investors":["..."],"source":"..."}
  ],
  "structure": {
    "parent_company": "...",
    "subsidiaries": ["list ALL subsidiaries and group companies found"],
    "divisions": ["..."],
    "sister_companies": ["..."]
  },
  "technology": {
    "tech_stack": ["..."],
    "cloud_providers": ["..."],
    "integrations": ["..."]
  },
  "market_intelligence": {
    "target_customers": ["..."],
    "target_geographies": ["..."],
    "key_competitors": [{"name":"...","website":"..."}],
    "differentiators": ["..."],
    "certifications": ["..."],
    "partnerships": ["..."]
  },
  "recent_news_and_updates": [
    {
      "title": "...",
      "summary": "2-3 sentence summary",
      "date": "...",
      "source": "...",
      "url": "..."
    }
  ],
  "key_achievements": [
    {
      "achievement": "...",
      "date": "...",
      "description": "..."
    }
  ],
  "reviews_and_ratings": [
    {
      "platform": "glassdoor/trustpilot/g2/ambitionbox",
      "rating": "...",
      "review_summary": "...",
      "total_reviews": "...",
      "url": "..."
    }
  ],
  "additional_info": {
    "incorporation_date": "...",
    "business_model": "...",
    "confidence_scores": {
      "overall": 0.0,
      "financials": 0.0,
      "leadership": 0.0,
      "contact": 0.0
    }
  },
  "sources": []
}

STRICT RULES:
1. ONLY extract what is explicitly in the input. NEVER hallucinate or guess.
2. Omit empty fields (null, "", [], {}).
3. company_number vs cin_number: NEVER confuse these. UK=company_number (8 chars), India=cin_number (21 chars).
4. LinkedIn URLs: include ONLY verified, complete URLs.
5. Lists are always JSON arrays, never comma-separated strings.
6. products_and_services: max 5 most important.
7. key_competitors: max 5. Must be REAL direct competitors of similar size and industry. NEVER list global giants (Microsoft, Google, IBM, Salesforce, etc.) unless the company explicitly competes with them.
8. recent_news: include ALL found news items, prioritise 2024-2026.
9. key_achievements: include ALL milestones found.
10. target_person: extract FULLY — this is critical. If person is in leadership list, copy and expand their data here too.
11. Output valid JSON only — starts with { ends with }.
12. sources: include all URLs you found data from.
13. Do NOT include education for leadership entries. Education is ONLY for target_person.
14. Use ONLY "title" for leadership roles — do NOT add a separate "current_title" field on leaders.
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
    user_content += f"Target Person: {person_name or 'None'}\n"
    if person_email:
        user_content += f"Target Person Email: {person_email}\n"
    user_content += (
        "\nIMPORTANT: Extract the target person's profile FULLY under 'target_person'. "
        "If the target person appears in leadership, copy their full data into target_person too.\n"
    )
    user_content += f"\nResearch Document:\n{research_text}"

    result = call_ollama(SYSTEM_PROMPT, user_content)
    if result:
        print("  [extractor] Done.")
    else:
        print("  [extractor] Failed.")
    return result