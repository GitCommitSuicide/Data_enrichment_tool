"""
cleaner.py — Strip bloat and placeholder values from final output
"""
from __future__ import annotations
import re
from typing import Any

_STRIP_KEYS = frozenset([
    "blog_categories", "recent_blog_posts", "website_sections", "legal_documents",
    "copyright_notice", "mission_statement", "technical_capabilities",
    "business_approach", "research_and_development", "management_capabilities",
    "educational_offerings", "core_focus_areas", "products_developed",
    "directors_demographics", "directors_compliance", "signatories_count",
    "online_metrics", "jobs_and_hiring", "news_and_events",
    "alexa_rank", "web_rank", "linkedin_followers",
    "available_data_sections", "available_financial_data",
    "last_agm_date", "last_financial_update_date", "data_last_updated",
    "sic_codes", "naics_codes", "tagline", "key_contacts",
    "registration_number", "listing_status", "years_in_business",
    "authorized_capital_lakhs", "paid_up_capital_lakhs",
    # Additional bloat keys from LLM hallucinations
    "ownership_structure", "key_executive_roles", "associated_brands",
    "begin_date", "company_age_years", "years_in_operation",
    "profile_updated_date", "last_updated", "operations_status",
    "date_of_last_agm", "date_of_balance_sheet", "employee_count_year",
    "authorized_capital_rs", "paid_up_capital_rs",
])

_NA_VALUES = frozenset([
    "", "...", "n/a", "not specified", "not available",
    "not found", "not disclosed", "unknown", "none", "null",
    # Extended patterns
    "not specified in sources", "not mentioned", "not publicly available",
    "not publicly disclosed", "not provided", "not listed",
    "no data", "no information", "no data available",
    "to be confirmed", "tbc", "tbd",
])

# Regex for partial-match placeholder values
_NA_PATTERN = re.compile(
    r"^(not\s+(specified|available|found|disclosed|mentioned|provided|listed|publicly)"
    r"|no\s+(data|information|details)"
    r"|unknown|n[\\/]a|tbd|tbc)\b",
    re.IGNORECASE,
)

# UK company number: 8 chars (digits or 2 letters + 6 digits)
_UK_COMPANY_NUM = re.compile(r"^([A-Z]{2}\d{6}|\d{8})$")

# Indian CIN: exactly 21 alphanumeric
_CIN_RE = re.compile(r"^[A-Z]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$")


def _strip_recursive(obj: Any) -> Any:
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if k in _STRIP_KEYS:
                continue
            v = _strip_recursive(v)
            if v is None:
                continue
            if isinstance(v, str) and (
                v.strip().lower() in _NA_VALUES or _NA_PATTERN.match(v.strip())
            ):
                continue
            if isinstance(v, (list, dict)) and not v:
                continue
            if isinstance(v, list) and all(
                isinstance(x, str) and (
                    x.strip().lower() in _NA_VALUES or _NA_PATTERN.match(x.strip())
                )
                for x in v
            ):
                continue
            cleaned[k] = v
        return cleaned
    elif isinstance(obj, list):
        result = [_strip_recursive(i) for i in obj]
        return [r for r in result if r not in (None, {}, "")]
    return obj


def _strip_education_from_leaders(data: dict) -> dict:
    """Remove education from leadership entries — keep only on target_person."""
    for leader in data.get("leadership", []):
        leader.pop("education", None)
    return data


def _deduplicate_title_fields(data: dict) -> dict:
    """
    If both 'title' and 'current_title' exist on a leader, keep only 'title'.
    Prefer current_title value if title is empty.
    """
    for leader in data.get("leadership", []):
        title = leader.get("title", "")
        current_title = leader.get("current_title", "")
        if title and current_title:
            leader.pop("current_title", None)
        elif current_title and not title:
            leader["title"] = current_title
            leader.pop("current_title", None)
    return data


def _normalise_subsidiaries(data: dict) -> dict:
    """Force all subsidiary entries to plain strings."""
    structure = data.get("structure", {})
    for key in ("subsidiaries", "sister_companies"):
        entries = structure.get(key, [])
        if not isinstance(entries, list):
            continue
        normalised = []
        seen: set[str] = set()
        for entry in entries:
            if isinstance(entry, dict):
                name = entry.get("name", "")
            elif isinstance(entry, str):
                name = entry.strip()
            else:
                continue
            # Deduplicate (case-insensitive)
            if name and name.upper() not in seen:
                seen.add(name.upper())
                normalised.append(name)
        if normalised:
            structure[key] = normalised
        else:
            structure.pop(key, None)
    return data


def _validate_registration_numbers(data: dict) -> dict:
    """Validate company_number (UK) and cin_number (India). Remove invalid ones."""
    company = data.get("company", {})

    # UK company number — must be 8 chars
    cn = company.get("company_number", "")
    if cn and not _UK_COMPANY_NUM.match(str(cn).strip()):
        company.pop("company_number", None)

    # Indian CIN — must be 21 chars
    cin = company.get("cin_number", "")
    if cin and not _CIN_RE.match(str(cin).strip()):
        company.pop("cin_number", None)

    return data


def clean_output(data: dict) -> dict:
    """Strip bloat, cap lists, remove placeholder values, normalise schema."""
    data = _strip_recursive(data)

    # Cap products to 5
    if isinstance(data.get("products_and_services"), list):
        data["products_and_services"] = data["products_and_services"][:5]

    # Cap competitors to 5
    mi = data.get("market_intelligence", {})
    if isinstance(mi.get("key_competitors"), list):
        mi["key_competitors"] = mi["key_competitors"][:5]

    # Leadership cleanup
    data = _strip_education_from_leaders(data)
    data = _deduplicate_title_fields(data)

    # Subsidiaries normalisation
    data = _normalise_subsidiaries(data)

    # Registration number validation
    data = _validate_registration_numbers(data)

    return data