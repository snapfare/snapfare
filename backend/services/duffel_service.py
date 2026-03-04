"""Duffel service — thin pipeline wrapper around duffel_api.get_flight_offers.

Mirrors the interface of amadeus_service.py so deals_pipeline.py can use
either backend with minimal changes.
"""

import os
from typing import Any, Dict

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

_ACCESS_TOKEN = os.getenv("DUFFEL_API_KEY")


def search_flights(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str | None = None,
    adults: int = 1,
    cabin_class: str = "economy",
) -> Dict[str, Any]:
    """Search for flight offers via Duffel.

    Returns:
        {"status": "ok", "data": [<normalized offer dicts>]}
        {"status": "disabled", "reason": "..."} if key not configured
        {"status": "error", "error": "..."} on failure
    """
    if not _ACCESS_TOKEN:
        return {"status": "disabled", "reason": "DUFFEL_API_KEY not configured"}

    try:
        from scoring.duffel_api import get_flight_offers  # type: ignore
        offers = get_flight_offers(
            origin_location_code=origin,
            destination_location_code=destination,
            departure_date=departure_date,
            return_date=return_date,
            adults=adults,
            cabin_class=cabin_class,
            access_token=_ACCESS_TOKEN,
        )
        return {"status": "ok", "data": offers}
    except Exception as e:
        return {"status": "error", "error": str(e)}
