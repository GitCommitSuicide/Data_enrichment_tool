"""
cleaner.py — Strip bloat and placeholder values from final output
"""
from __future__ import annotations
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
])

_NA_VALUES = frozenset([
    "", "...", "n/a", "not specified", "not available",
    "not found", "not disclosed", "unknown", "none", "null",
])


def _strip_recursive(obj: Any) -> Any:
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if k in _STRIP_KEYS:
                continue
            v = _strip_recursive(v)
            if v is None:
                continue
            if isinstance(v, str) and v.strip().lower() in _NA_VALUES:
                continue
            if isinstance(v, (list, dict)) and not v:
                continue
            if isinstance(v, list) and all(
                isinstance(x, str) and x.strip().lower() in _NA_VALUES for x in v
            ):
                continue
            cleaned[k] = v
        return cleaned
    elif isinstance(obj, list):
        result = [_strip_recursive(i) for i in obj]
        return [r for r in result if r not in (None, {}, "")]
    return obj


def clean_output(data: dict) -> dict:
    """Strip bloat, cap lists, remove placeholder values."""
    data = _strip_recursive(data)

    # Cap products to 5
    if isinstance(data.get("products_and_services"), list):
        data["products_and_services"] = data["products_and_services"][:5]

    # Cap competitors to 5
    mi = data.get("market_intelligence", {})
    if isinstance(mi.get("key_competitors"), list):
        mi["key_competitors"] = mi["key_competitors"][:5]

    return data