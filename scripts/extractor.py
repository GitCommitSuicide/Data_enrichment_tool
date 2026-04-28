"""
extractor.py — LLM extraction via Ollama
"""
from __future__ import annotations
import json, re, time, requests
from .config import OLLAMA_URL, OLLAMA_MODEL, OLLAMA_TIMEOUT

SYSTEM_PROMPT = """You are a strict company data extractor.

Extract every meaningful data point from the raw research text and return ONE structured JSON object.
Follow this schema exactly. Omit any key whose value is empty/null/unknown.

{
  "company": {
    "name": "...", "legal_name": "...", "founded_year": 0,
    "cin_number": "...", "gst_number": "...", "pan_number": "...",
    "company_type": "...", "status": "...",
    "description": "3-5 sentence factual description",
    "industry": ["..."], "sub_industry": ["..."],
    "headquarters": {"address":"...","city":"...","state":"...","country":"...","zip_code":"..."},
    "other_locations": [{"address":"...","city":"...","state":"...","country":"...","type":"..."}],
    "website": "...", "employee_count_range": "..."
  },
  "contact": {
    "email": ["..."], "phone": ["..."],
    "social_media": {"linkedin":"...","twitter":"...","facebook":"...","instagram":"..."}
  },
  "leadership": [{
    "name":"...","title":"...","din":"...","appointment_date":"...","nationality":"...",
    "linkedin_url":"verified URL only",
    "contact":{"email":["..."],"phone":["..."]},
    "experience":[{"title":"...","company":"...","duration":"...","is_current":true}],
    "education":[{"institution":"...","degree":"..."}],
    "summary":"one-sentence bio"
  }],
  "products_and_services": [{"name":"...","type":"Product or Service","category":"...","description":"..."}],
  "financials": {
    "revenue":"...","revenue_year":0,"revenue_source":"...",
    "total_funding":"...","valuation":"...",
    "authorized_capital":"...","paid_up_capital":"..."
  },
  "funding": [{"round_name":"...","amount":"...","date":"...","lead_investors":["..."],"source":"..."}],
  "structure": {
    "parent_company":"...",
    "subsidiaries":["..."],
    "divisions":["..."]
  },
  "technology": {"tech_stack":["..."],"cloud_providers":["..."]},
  "market_intelligence": {
    "target_customers":["..."],"target_geographies":["..."],
    "key_competitors":[{"name":"...","website":"..."}],
    "differentiators":["..."],"certifications":["..."]
  },
  "recent_news_and_updates": [
    {"title":"...","summary":"...","key_points":["..."],"date":"...","url":"..."}
  ],
  "key_achievements": [
    {"achievement":"...","date":"...","description":"..."}
  ],
  {
  "Reviws and Updates": "",
  "news_and_developments": [
    {
      "title": "",
      "summary": "",
      "date": "",
      "source": "",
      "url": ""
    }
  ],
  "achievements_and_milestones": [
    {
      "title": "",
      "description": "",
      "year": "",
      "source": "",
      "url": ""
    }
  ],
  "reviews_and_ratings": [
    {
      "platform": "",
      "rating": "",
      "review_summary": "",
      "total_reviews": "",
      "source": "",
      "url": ""
    }
  ],
  "partnerships_and_certifications": [
    {
      "type": "",
      "partner_or_certification": "",
      "details": "",
      "year": "",
      "source": "",
      "url": ""
    }
  ],
  "sources": []
},
  "additional_info": {
    "incorporation_date":"...","business_model":"...",
    "data_sources":["..."],
    "confidence_scores":{"overall":0.0,"financials":0.0,"leadership":0.0,"contact":0.0}
  },
  "target_person": {
    "name":"...","current_title":"...","linkedin_url":"...","email":"...","phone":"...",
    "experience":[{"title":"...","company":"...","duration":"...","is_current":true}],
    "education":[{"institution":"...","degree":"..."}],
    "skills":["..."]
  }
}

RULES:
1. ONLY extract what is explicitly in the input. Never hallucinate.
2. Omit empty fields (null, "", [], {}).
3. GSTIN=15 chars, CIN=21 chars, PAN=10 chars — verify format or omit.
4. LinkedIn URLs: verified only.
5. Lists are always arrays, never comma-separated strings.
6. Products: max 5. Competitors: max 5.
7. Output valid JSON only — starts with { ends with }.
8. Subsidiaries: list ALL subsidiaries and group companies found.
9. Try to add recent achievements and milestone only not old records.
"""


def _parse_json(raw: str) -> dict:
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw).strip()
    raw = re.sub(r",\s*([\]}])", r"\1", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    start = raw.find("{")
    if start == -1:
        return {}
    depth = end = 0
    for i, ch in enumerate(raw[start:], start):
        if ch == "{": depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1; break
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
                json={"model": OLLAMA_MODEL, "prompt": user_content, "system": system_prompt,
                      "stream": False, "options": {"temperature": 0.0}},
                timeout=OLLAMA_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            raw  = data.get("response", "").strip()
            pt, rt = data.get("prompt_eval_count", 0), data.get("eval_count", 0)
            print(f"  [tokens] prompt={pt}  response={rt}  total={pt+rt}")
            result = _parse_json(raw)
            if result:
                return result
            print(f"  [warn] attempt {attempt}: empty JSON")
        except requests.exceptions.Timeout:
            print(f"  [warn] attempt {attempt}: timeout ({OLLAMA_TIMEOUT}s)")
        except requests.exceptions.RequestException as e:
            print(f"  [error] attempt {attempt}: {e}"); break
        if attempt <= retries:
            time.sleep(2)
    return {}


def extract(research_text: str, company: str, person_name: str = "", person_email: str = "") -> dict:
    print(f"  [extractor] Extracting data for: {company} …")
    user_content = f"Company: {company}\nTarget Person: {person_name or 'None'}\n"
    if person_email:
        user_content += f"Target Person Email: {person_email}\n"
    user_content += f"\nResearch Document:\n{research_text}"
    result = call_ollama(SYSTEM_PROMPT, user_content)
    if result:
        print("  [extractor] Done.")
    else:
        print("  [extractor] Failed.")
    return result