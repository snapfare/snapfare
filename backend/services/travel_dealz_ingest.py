from typing import Any, Dict, List

from scrapers.travel_dealz import get_deals, get_deals_de
from database.supabase_db import save_deals


def ingest_travel_dealz(limit: int = 100) -> Dict[str, Any]:
    """Scrape travel-dealz.com and .de and persist minimal fields to Supabase.

    - Uses the pure scrapers (without touching travel_dealz.py).
    - Reduces each deal to: title, price, link.
    - Inserts into the aggregated 'deals' table in Supabase via save_deals().

    Always returns a dictionary with:
    {
        "status": "ok" | "disabled" | "error",
        "scraped": int,           # total deals scraped (before filtering fields)
        "saved": int | None,      # deals sent to Supabase (or None if not inserted),
        "supabase": Any           # raw response from save_deals()
    }
    """
    deals_com: List[Dict[str, Any]] = get_deals(limit=limit)
    deals_de: List[Dict[str, Any]] = get_deals_de(limit=limit)

    all_deals = deals_com + deals_de

    # Keep only the minimum required fields for now
    payload: List[Dict[str, Any]] = [
        {
            "title": d.get("title"),
            "price": d.get("price"),
            "currency": d.get("currency"),
            "link": d.get("link"),
        }
        for d in all_deals
    ]

    if not payload:
        return {
            "status": "ok",
            "scraped": 0,
            "saved": 0,
            "supabase": {"status": "skipped", "reason": "no deals scraped"},
        }

    supabase_result = save_deals("deals", payload)
    status = supabase_result.get("status", "error")

    return {
        "status": status,
        "scraped": len(all_deals),
        "saved": len(payload) if status == "ok" else None,
        "supabase": supabase_result,
    }
