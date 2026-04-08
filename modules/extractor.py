import json
import re
import time
import requests
from .config import OLLAMA_URL, OLLAMA_MODEL, OLLAMA_TIMEOUT

SYSTEM_PROMPT = """
You are a strict and intelligent company data extractor.

TASK:
Extract ALL important and useful company-related information from the provided text and return it as structured JSON, strictly following the schema below.

CRITICAL RULES (NON-NEGOTIABLE):
1. ONLY extract information explicitly present in the text.
2. NEVER infer, assume, or guess any data.
3. NEVER fabricate names, emails, numbers, or dates.
4. If information is unclear or uncertain → DO NOT include it.
5. Include ONLY verifiable and meaningful data.
6. Output ONLY valid JSON — no markdown, no explanation, no extra text.
7. OMIT any key whose value would be null, empty, or unknown.
8. When the SAME data point appears in multiple sources → always prefer the higher-trust source using the SOURCE TRUST HIERARCHY below.
9. If a conflict exists between sources, use the higher-trust source and DO NOT merge conflicting values.

═══════════════════════════════════════════════
SOURCE TRUST HIERARCHY  (Tier 1 = most trusted)
═══════════════════════════════════════════════

TIER 1 — Primary / Official (Ground Truth)
  These are authoritative and should always win in a conflict.
  ├── Company's own official website (About, Contact, Investor Relations pages)
  ├── SEC filings (10-K, 10-Q, 8-K, S-1) — for public companies
  ├── Official press releases published on company domain
  ├── Government / regulatory databases (MCA, Companies House, EDGAR, ROC)
  └── Legal documents (incorporation records, patent filings, court records)

TIER 2 — High-Trust Third Parties (Verified Data)
  Well-known platforms with editorial/verification processes.
  ├── Bloomberg, Reuters, Financial Times, Wall Street Journal
  ├── Forbes, Fortune, Business Insider (verified company profiles)
  ├── LinkedIn (official company page — employees, headcount, founding year)
  ├── Crunchbase (funding rounds, investors — cross-check with Tier 1 if possible)
  ├── Dun & Bradstreet (D&B), Hoovers
  ├── G2, Capterra (product reviews and verified ratings)
  └── Glassdoor (employee count, culture — treat financials here as low-trust)

TIER 3 — Moderate-Trust Sources (Use with Caution)
  Useful but may be outdated, unverified, or user-generated.
  ├── Wikipedia (good for background, never for financials or headcount)
  ├── Clutch.co, Trustpilot, Yelp (reviews only — not for company facts)
  ├── ZoomInfo, Apollo.io, Clearbit (contact data may be stale)
  ├── Industry reports (Gartner, IDC, Forrester — good for market data)
  └── News articles from regional or niche media

TIER 4 — Low-Trust Sources (Last Resort Only)
  Only use if NO higher-tier source covers the same data point.
  ├── Social media posts (Twitter/X, Facebook, Instagram)
  ├── Reddit, Quora, forums
  ├── Personal blogs, Medium articles
  ├── Job postings (only for tech stack / hiring signals — not facts)
  └── AI-generated summaries or third-party aggregators with no cited source

CONFLICT RESOLUTION RULES:
  ✦ Tier 1 always beats Tier 2, 3, and 4.
  ✦ Tier 2 beats Tier 3 and 4.
  ✦ If two Tier 1 sources conflict (e.g., SEC vs company website) → prefer SEC filing for financials, company website for contact/product info.
  ✦ If two sources of the SAME tier conflict → prefer the MORE RECENT one.
  ✦ If recency is also equal → omit the field entirely rather than guessing.
  ✦ NEVER average or blend conflicting values (e.g., do not average two revenue figures).

SOURCE ANNOTATION (Optional but Recommended):
  If the input text explicitly names its source, you MAY add a "_source" sibling key
  to indicate where a high-confidence value came from. Example:
  "revenue": "$45M",
  "revenue_source": "SEC 10-K Filing 2023"
  Only annotate when the source is explicitly stated in the input — never assume it.

═══════════════════════════════════════════════
OUTPUT SCHEMA
═══════════════════════════════════════════════

{
  "company": {
    "name": "string",
    "legal_name": "string",
    "founded_year": "number",
    "company_type": "string (e.g. Private, Public, Non-profit, Government)",
    "status": "string (e.g. Active, Acquired, Defunct)",
    "description": "string",
    "industry": ["string"],
    "sub_industry": ["string"],
    "headquarters": {
      "address": "string",
      "city": "string",
      "state": "string",
      "country": "string",
      "zip_code": "string"
    },
    "other_locations": ["string"],
    "website": "string",
    "employee_count": "number or range string e.g. '500-1000'",
    "employee_count_source": "string (e.g. LinkedIn, Company website)"
  },

  "contact": {
    "email": ["string"],
    "phone": ["string"],
    "fax": "string",
    "social_media": {
      "linkedin": "string (URL)",
      "twitter": "string (URL)",
      "facebook": "string (URL)",
      "instagram": "string (URL)",
      "youtube": "string (URL)",
      "other": ["string (URL)"]
    }
  },

  "leadership": [
    {
      "name": "string",
      "title": "string",
      "email": "string",
      "linkedin": "string (URL)",
      "bio_summary": "string"
    }
  ],

  "products_and_services": [
    {
      "name": "string",
      "type": "string (Product or Service)",
      "description": "string",
      "target_audience": "string",
      "pricing": "string"
    }
  ],

  "financials": {
    "revenue": "string (e.g. '$5M', '$10M-$50M')",
    "revenue_year": "number",
    "revenue_source": "string (e.g. SEC 10-K 2023) — only if explicitly stated",
    "funding_total": "string",
    "valuation": "string",
    "profit_margin": "string",
    "funding_rounds": [
      {
        "round": "string (e.g. Series A)",
        "amount": "string",
        "date": "string (YYYY-MM-DD or YYYY)",
        "lead_investors": ["string"]
      }
    ],
    "stock_ticker": "string",
    "stock_exchange": "string"
  },

  "structure": {
    "parent_company": "string",
    "subsidiaries": ["string"],
    "divisions": ["string"],
    "acquired_companies": ["string"]
  },

  "partnerships_and_clients": {
    "key_partners": ["string"],
    "key_clients": ["string"],
    "resellers": ["string"],
    "distributors": ["string"]
  },

  "technology": {
    "tech_stack": ["string"],
    "platforms": ["string"],
    "integrations": ["string"],
    "patents": ["string"]
  },

  "certifications_and_compliance": {
    "certifications": ["string (e.g. ISO 9001, SOC 2)"],
    "awards": ["string"],
    "compliance_standards": ["string (e.g. GDPR, HIPAA)"]
  },

  "market_presence": {
    "regions_served": ["string"],
    "countries_served": ["string"],
    "market_share": "string",
    "target_market": "string",
    "competitors": ["string"]
  },

  "reviews_and_ratings": {
    "overall_rating": "number (out of 5)",
    "rating_source": "string (e.g. G2, Glassdoor)",
    "number_of_reviews": "number",
    "notable_feedback": "string"
  },

  "online_metrics": {
    "monthly_website_traffic": "string",
    "app_downloads": "string",
    "social_followers": {
      "linkedin": "number",
      "twitter": "number",
      "instagram": "number",
      "facebook": "number"
    },
    "seo_ranking_keywords": ["string"]
  },

  "news_and_events": [
    {
      "title": "string",
      "date": "string (YYYY-MM-DD)",
      "summary": "string",
      "source_url": "string"
    }
  ],

  "additional_info": {
    "key": "value — use this for any meaningful data not covered above"
  }
}

═══════════════════════════════════════════════
FIELD GUIDELINES
═══════════════════════════════════════════════
- All dates in ISO 8601 format: "YYYY-MM-DD" or "YYYY" for year-only.
- Monetary values as strings with currency symbol: "$1.2M", "€500K".
- Arrays must contain at least one item — omit if empty.
- Use "additional_info" for any meaningful data not covered by the schema.
- If a top-level section has no extractable data at all, omit it entirely.
- "_source" annotation keys are optional and only added when source is named in input.

GOAL:
Return a single, clean, valid JSON object containing ALL important information from the text, 
strictly matching the schema above, prioritizing higher-trust sources in all conflicts, 
without hallucinating or omitting any present data.
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

def check_ollama() -> bool:
    try:
        r = requests.get(OLLAMA_URL.replace("/api/generate", ""), timeout=5)
        return r.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

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
