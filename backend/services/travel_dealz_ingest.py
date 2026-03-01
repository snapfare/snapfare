from typing import Any, Dict, List

from scrapers.travel_dealz import get_deals, get_deals_de
from database.supabase_db import save_deals


def ingest_travel_dealz(limit: int = 100) -> Dict[str, Any]:
    """Scrape travel-dealz.com and .de and persist minimal fields to Supabase.

    - Usa los scrapers puros (sin tocar travel_dealz.py).
    - Reduce cada deal a: title, price, link.
    - Inserta en la tabla agregada 'deals' de Supabase mediante save_deals().

    Devuelve siempre un diccionario con:
    {
        "status": "ok" | "disabled" | "error",
        "scraped": int,           # deals totales scrapeados (antes de filtrar campos)
        "saved": int | None,      # deals enviados a Supabase (o None si no se insertó),
        "supabase": Any           # respuesta cruda de save_deals()
    }
    """
    deals_com: List[Dict[str, Any]] = get_deals(limit=limit)
    deals_de: List[Dict[str, Any]] = get_deals_de(limit=limit)

    all_deals = deals_com + deals_de

    # Mantener solo los campos mínimos requeridos por ahora
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
