# 🔍 Company Data Enrichment Tool

An automated **OSINT (Open Source Intelligence)** pipeline that researches any company from a single input — a company name, domain, or work email — and produces a comprehensive, structured JSON intelligence report.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline Stages](#pipeline-stages)
- [Project Structure](#project-structure)
- [Module Details](#module-details)
- [Output Schema](#output-schema)
- [Setup & Installation](#setup--installation)
- [How to Run](#how-to-run)
- [Configuration](#configuration)
- [Output Files](#output-files)
- [Known Issues & Notes](#known-issues--notes)

---

## Overview

This tool accepts one of three input formats:

| Input Type     | Example                    | What Happens                                          |
| -------------- | -------------------------- | ----------------------------------------------------- |
| **Work Email** | `rohit.dey@capsitech.com`  | Extracts company name + target person from the email   |
| **Domain**     | `capsitech.com`            | Extracts company name from domain                      |
| **Company**    | `capsitech`                | Uses directly as company name                          |

It then runs a **5-step pipeline** to produce a full intelligence report saved as JSON.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         main.py                             │
│                   (Pipeline Orchestrator)                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  STEP 1 ──► search.py          Tavily API queries           │
│             (gather)           + parallel scraping           │
│                  │                                          │
│                  ▼                                          │
│  STEP 2 ──► extractor.py       LLM-powered structured      │
│             (extract)          data extraction via Ollama    │
│                  │                                          │
│                  ▼                                          │
│  STEP 3 ──► people_enricher.py Per-person OSINT enrichment  │
│             (enrich_people)    via targeted searches         │
│                  │                                          │
│                  ▼                                          │
│  STEP 4 ──► enricher.py        Gap-filling enrichment       │
│             (enrich)           LLM detects gaps → searches  │
│                  │              → extracts → merges          │
│                  ▼                                          │
│  STEP 5 ──► main.py            Combine all outputs into     │
│             (_combine)         final unified JSON            │
│                                                             │
│  Throughout ► cleaner.py       Strips bloat, placeholders,  │
│               (clean_output)   and caps list sizes           │
├─────────────────────────────────────────────────────────────┤
│             config.py          Loads .env, initialises       │
│                                Tavily client & Ollama config │
└─────────────────────────────────────────────────────────────┘
```

---

## Pipeline Stages

### STEP 1 — Gather Research (Tavily)
**Module:** `scripts/search.py` → `gather()`

- Builds **12+ targeted search queries** covering:
  - Official website, LinkedIn, Crunchbase, Bloomberg
  - Leadership team profiles
  - Revenue, funding, valuation
  - Legal identifiers (CIN, GSTIN, PAN) from Indian registrar sites
  - Recent news, press releases (2025/2026)
  - Achievements, milestones, awards
  - Subsidiaries and group companies
  - Tech stack and infrastructure
  - Reviews and ratings (Glassdoor, G2, Ambitionbox, etc.)
- If a **target person** is provided, adds 2 additional person-specific queries
- Runs all queries **in parallel** (up to 12 workers)
- **Deep-scrapes** shallow results (< 500 chars) in parallel for richer content
- Deduplicates URLs, assembles chunks in original query order
- Extracts social/contact URLs (LinkedIn, Twitter, emails) via regex
- Truncates to `MAX_CONTEXT` (default 160,000 chars)

**Output:** `<slug>_tavily.json` — list of source URLs + text length

---

### STEP 2 — LLM Extraction
**Module:** `scripts/extractor.py` → `extract()`

- Sends the full research text to a local **Ollama** LLM (default: `deepseek-v3.1:671b-cloud`)
- Uses a strict system prompt with a detailed JSON schema
- The LLM extracts every meaningful data point into structured fields
- `call_ollama()` handles retries (default 2), timeout, and JSON parsing
- Robust JSON parser handles markdown code fences, trailing commas, and partial output

**Output:** `<slug>_extracted.json` — structured company data

---

### STEP 3 — People Enrichment
**Module:** `scripts/people_enricher.py` → `enrich_people()`

- Scans extracted data for `leadership` entries and `target_person`
- For each person, runs **2 targeted queries** on LinkedIn, RocketReach, Apollo, Crunchbase
- Deep-scrapes results if content is thin
- Sends research to LLM to extract a detailed profile per person:
  - Name, title, nationality
  - LinkedIn URL, email, phone
  - Experience (current roles only), education, skills, summary
- **Contact validation** built in:
  - Phone: rejects masked/placeholder numbers (XXX patterns), requires 7+ digits
  - Email: rejects obfuscated, placeholder, or junk domains
- Runs all person enrichments **in parallel** via `ThreadPoolExecutor`
- Merges enriched data back into the company data (base values win, never overwrites real data)

**Output:** `<slug>_people.json` — leadership + target_person profiles with sources

---

### STEP 4 — Company Enrichment (Gap-Filling)
**Module:** `scripts/enricher.py` → `enrich()`

- **Gap detection:** Recursively scans every field for empty/missing/placeholder values
- **Query generation:** LLM generates 5 targeted search queries to fill the most critical gaps, prioritising:
  1. Missing LinkedIn URLs for leaders
  2. Financials (revenue, funding)
  3. Subsidiaries and group companies
  4. Tech stack
  5. Certifications and competitors
- Falls back to hardcoded queries if the LLM fails
- Runs queries sequentially, scrapes results, extracts per-source fragments via LLM
- **Final merge:** LLM fuses all fragments into the original company JSON using conflict resolution:
  - Government sources > Official site > LinkedIn > News
- Deduplicates entities, preserves existing real values

**Output:** `<slug>_enriched.json` — the most complete version of company data

---

### STEP 5 — Combine Final Output
**Module:** `main.py` → `_combine()`

- Merges all pipeline stages with priority: **enriched > people > extracted > tavily**
- Consolidates all source URLs (capped at 50)
- Adds `_pipeline` metadata (counts of URLs, sources, people enriched)
- Runs final `clean_output()` pass

**Output:** `<slug>_final.json` — the combined, definitive output

---

## Project Structure

```
ewww/
├── main.py                    # Pipeline orchestrator — entry point
├── .env                       # API keys (TAVILY_API_KEY, etc.)
├── .gitignore                 # Ignores caches, venv, .env, *.json
├── data_scraped/              # All output JSON files saved here
│   └── <company>/             # Per-company subdirectory
│       ├── <slug>_tavily.json
│       ├── <slug>_extracted.json
│       ├── <slug>_people.json
│       ├── <slug>_enriched.json
│       └── <slug>_final.json
└── scripts/
    ├── config.py              # Loads .env, sets up Tavily client + Ollama config
    ├── search.py              # Tavily search, web scraping, query building
    ├── extractor.py           # Ollama LLM calls + JSON parsing
    ├── enricher.py            # Gap-filling enrichment (detect → search → merge)
    ├── people_enricher.py     # Per-person OSINT enrichment
    └── cleaner.py             # Post-processing: strip bloat & placeholders
```

---

## Module Details

### `scripts/config.py`
Loads environment variables from `.env` and initialises shared clients:

| Variable              | Default                                    | Description                        |
| --------------------- | ------------------------------------------ | ---------------------------------- |
| `OLLAMA_URL`          | `http://localhost:11434/api/generate`       | Ollama API endpoint                |
| `OLLAMA_MODEL`        | `deepseek-v3.1:671b-cloud`                | LLM model name                     |
| `OLLAMA_TIMEOUT`      | `600`                                      | Request timeout in seconds          |
| `TAVILY_API_KEY`      | *(required)*                               | Tavily search API key              |
| `MAX_RESULTS_PER_QUERY` | `10`                                     | Max results per Tavily search      |
| `MAX_CONTEXT`         | `160000`                                   | Max chars of research text to send |

### `scripts/search.py`
- **`scrape_url(url)`** — HTTP scrape with noise tag removal (script, nav, footer, etc.)
- **`extract_social_urls(text)`** — Regex-based LinkedIn, Twitter, Crunchbase, RocketReach, Apollo URL and email extraction
- **`fetch(query)`** — Single Tavily advanced search call
- **`build_queries(company)`** — Generates 12+ targeted company research queries
- **`build_person_queries(company, person)`** — Generates 2 person-specific queries
- **`gather(company, person)`** — Full parallel search + scrape pipeline

### `scripts/extractor.py`
- **`check_ollama()`** — Health-check the Ollama server
- **`call_ollama(system, user, retries)`** — Send prompt to Ollama, parse JSON response
- **`extract(text, company, person, email)`** — Run the full schema extraction
- **`_parse_json(raw)`** — Robust JSON parser handling code fences, trailing commas, partial output

### `scripts/enricher.py`
- **`_find_gaps(data)`** — Recursively find all empty/missing fields
- **`_generate_queries(data)`** — LLM-powered or fallback query generation
- **`_search_and_scrape(queries)`** — Sequential search + deep scrape
- **`enrich(company_data, company_name)`** — Full gap-filling pipeline

### `scripts/people_enricher.py`
- **`_enrich_single(person, company)`** — Research + extract profile for one person
- **`_clean_phones(phones)`** — Validate and filter phone numbers
- **`_clean_emails(emails)`** — Validate and filter email addresses
- **`_clean_contact(enriched)`** — Strip invalid contact data from profile
- **`_merge(base, enriched)`** — Merge enriched profile into base (base wins)
- **`enrich_people(company_data, company_name)`** — Parallel enrichment of all people

### `scripts/cleaner.py`
- **`clean_output(data)`** — Recursively strips:
  - Bloat keys (blog posts, legal docs, web metrics, etc.)
  - Placeholder values (`"..."`, `"n/a"`, `"unknown"`, etc.)
  - Empty containers (`[]`, `{}`, `""`, `null`)
  - Caps `products_and_services` to 5 items
  - Caps `key_competitors` to 5 items

---

## Output Schema

The final JSON follows this structure (fields omitted if unknown):

```json
{
  "company": {
    "name", "legal_name", "founded_year", "cin_number", "gst_number", "pan_number",
    "company_type", "status", "description", "industry", "sub_industry",
    "headquarters": { "address", "city", "state", "country", "zip_code" },
    "other_locations": [...],
    "website", "employee_count_range"
  },
  "contact": {
    "email": [...], "phone": [...],
    "social_media": { "linkedin", "twitter", "facebook", "instagram" }
  },
  "leadership": [{
    "name", "title", "din", "appointment_date", "nationality",
    "linkedin_url", "contact": { "email", "phone" },
    "experience": [{ "title", "company", "duration", "is_current" }],
    "education": [{ "institution", "degree" }],
    "summary"
  }],
  "products_and_services": [{ "name", "type", "category", "description" }],
  "financials": {
    "revenue", "revenue_year", "revenue_source",
    "total_funding", "valuation",
    "authorized_capital", "paid_up_capital"
  },
  "funding": [{ "round_name", "amount", "date", "lead_investors", "source" }],
  "structure": { "parent_company", "subsidiaries", "divisions" },
  "technology": { "tech_stack", "cloud_providers" },
  "market_intelligence": {
    "target_customers", "target_geographies",
    "key_competitors": [{ "name", "website" }],
    "differentiators", "certifications"
  },
  "recent_news_and_updates": [{
    "title", "summary", "key_points", "date", "url"
  }],
  "key_achievements": [{ "achievement", "date", "description" }],
  "news_and_developments": [{ "title", "summary", "date", "source", "url" }],
  "achievements_and_milestones": [{ "title", "description", "year", "source", "url" }],
  "reviews_and_ratings": [{ "platform", "rating", "review_summary", "total_reviews", "source", "url" }],
  "partnerships_and_certifications": [{ "type", "partner_or_certification", "details", "year", "source", "url" }],
  "additional_info": {
    "incorporation_date", "business_model", "data_sources",
    "confidence_scores": { "overall", "financials", "leadership", "contact" }
  },
  "target_person": {
    "name", "current_title", "linkedin_url", "email", "phone",
    "experience": [{ "title", "company", "duration", "is_current" }],
    "education": [{ "institution", "degree" }],
    "skills"
  },
  "sources": ["..."],
  "_pipeline": {
    "tavily_urls_count", "extracted", "people_enriched", "enrichment_sources"
  }
}
```

---

## Setup & Installation

### Prerequisites

- **Python 3.10+**
- **Ollama** running locally (or accessible via network) with a model loaded
- **Tavily API key** (get one at [tavily.com](https://tavily.com))

### 1. Clone and enter the project

```bash
git clone <repo-url>
cd ewww
```

### 2. Create a virtual environment

```bash
python -m venv myenv

# Windows
myenv\Scripts\activate

# Linux/macOS
source myenv/bin/activate
```

### 3. Install dependencies

```bash
pip install requests beautifulsoup4 python-dotenv tavily-python
```

### 4. Set up environment variables

Create a `.env` file in the project root:

```env
TAVILY_API_KEY=tvly-your-api-key-here

# Optional overrides:
# OLLAMA_URL=http://localhost:11434/api/generate
# OLLAMA_MODEL=deepseek-v3.1:671b-cloud
# OLLAMA_TIMEOUT=600
# MAX_RESULTS_PER_QUERY=10
# MAX_CONTEXT=160000
```

### 5. Start Ollama

Make sure Ollama is running and has the configured model loaded:

```bash
ollama serve
ollama pull deepseek-v3.1:671b-cloud    # or your chosen model
```

---

## How to Run

```bash
python main.py
```

You will be prompted:

```
Enter company name, email, or domain:
```

Enter any of:
- A **work email**: `rohit.dey@capsitech.com`
- A **domain**: `capsitech.com`
- A **company name**: `capsitech`

The pipeline runs through all 5 steps and produces the output files in `data_scraped/`.

### Example Console Output

```
  Detected email → company: capsitech  |  person: Rohit Dey

🔍  Researching: capsitech  (target: Rohit Dey)
────────────────────────────────────────────────────────────

STEP 1 — Gathering research (Tavily)
  Dispatching 14 queries in parallel …
  Gathered 89 sources in 12.3s

STEP 2 — Extracting structured data (LLM)
  [extractor] Extracting data for: capsitech …
  Extraction done in 45.2s

STEP 3 — People enrichment
  [people_enricher] Starting for capsitech …
  [people_enricher] Enriching 3 people in parallel …

STEP 4 — Company enrichment
  [enricher] 15 gaps found — generating queries …
  [enricher] 12 sources collected

STEP 5 — Combining all outputs

════════════════════════════════════════════════════════════
  ✅  Pipeline complete for: capsitech
════════════════════════════════════════════════════════════
  📄  Tavily raw    → capsitech_Rohit_Dey_tavily.json     (89 URLs)
  📄  Extracted     → capsitech_Rohit_Dey_extracted.json
  📄  People        → capsitech_Rohit_Dey_people.json     (3 leaders)
  📄  Enriched      → capsitech_Rohit_Dey_enriched.json   (12 sources)
  📄  Final         → capsitech_Rohit_Dey_final.json      ← combined output
════════════════════════════════════════════════════════════

Total time: 142.38 seconds
```

---

## Output Files

All files are saved in the `data_scraped/` directory:

| File                          | Contents                                                |
| ----------------------------- | ------------------------------------------------------- |
| `<slug>_tavily.json`         | Raw Tavily search metadata (URLs list + text length)     |
| `<slug>_extracted.json`      | LLM-extracted structured data (first pass)               |
| `<slug>_people.json`         | People enrichment results (leadership + target person)   |
| `<slug>_enriched.json`       | Gap-filled enriched data (most complete single stage)    |
| `<slug>_final.json`          | **Final combined output** — use this one                 |

Where `<slug>` = sanitised company name + person name (e.g. `capsitech_Rohit_Dey`).

---

## Known Issues & Notes

1. **People enricher schema mismatch:** The extractor sometimes outputs `decision_makers` instead of `leadership`. The people enricher expects `leadership` — if the LLM uses a different key name, Step 3 will skip with "No people to enrich." This can be fixed by normalising the key in `main.py` before calling `enrich_people()`.

2. **Free email rejection:** Gmail, Yahoo, Hotmail, Outlook, and ProtonMail addresses are rejected — only work emails with company domains are accepted.

3. **Rate limits:** Tavily API has usage limits depending on your plan. The pipeline makes 12–14+ queries in Step 1 plus additional queries in Steps 3 and 4.

4. **Ollama timeout:** Large research documents can take 60–120+ seconds for the LLM to process. The default timeout is 600s. Adjust `OLLAMA_TIMEOUT` in `.env` if needed.

5. **SSL warnings:** The scraper uses `verify=False` for HTTPS requests, which generates `InsecureRequestWarning` messages. These are cosmetic and don't affect functionality.

---

## License

*Internal tool — no public license specified.*
