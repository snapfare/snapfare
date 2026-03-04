"""Duffel API client for flight offer searches.

Replaces the Amadeus integration with Duffel's offer request API.
Returns offers normalized to the same schema used by scoring.py so that
``get_best_flights()`` in scoring.py can be used without changes.

Uses raw HTTP requests (not the duffel-api SDK) to be compatible with the
current Duffel API v2 schema.

Usage:
    from scoring.duffel_api import get_flight_offers
    offers = get_flight_offers("ZRH", "JFK", "2026-06-01", "2026-06-14", adults=1)

Environment:
    DUFFEL_API_KEY — required
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

_DUFFEL_API_URL = "https://api.duffel.com"
_DUFFEL_API_VERSION = "v2"


def _iso8601_duration(seconds: float) -> str:
    """Convert a duration in seconds to an ISO 8601 duration string (PT#H#M)."""
    total_minutes = int(seconds / 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"PT{hours}H{minutes}M"


def _normalize_offer_dict(offer: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a Duffel v2 offer JSON dict to the Amadeus-compatible schema.

    Amadeus schema expected by scoring.py:
        {
          "price": {"total": <float>},
          "itineraries": [
            {
              "segments": [
                {"duration": "PT10H30M", "aircraftCode": "789", ...}
              ]
            }
          ]
        }
    """
    # Total price
    try:
        total = float(offer.get("total_amount", 0) or 0)
        currency = offer.get("total_currency", "EUR")
    except Exception:
        total = 0.0
        currency = "EUR"

    # Build itinerary segments from Duffel slices
    itineraries: List[Dict[str, Any]] = []
    max_stops: int = 0
    try:
        for sl in offer.get("slices") or []:
            segs_raw = sl.get("segments") or []
            # stops = segments - 1 per slice (0 segments = direct)
            slice_stops = max(0, len(segs_raw) - 1)
            if slice_stops > max_stops:
                max_stops = slice_stops

            segments: List[Dict[str, Any]] = []
            for seg in segs_raw:
                dur_raw = seg.get("duration")
                if isinstance(dur_raw, str):
                    duration_iso = dur_raw  # already ISO 8601
                elif isinstance(dur_raw, (int, float)):
                    duration_iso = _iso8601_duration(dur_raw)
                else:
                    duration_iso = "PT0H"

                origin_code = (seg.get("origin") or {}).get("iata_code") or ""
                dest_code = (seg.get("destination") or {}).get("iata_code") or ""
                carrier = (seg.get("operating_carrier") or {}).get("iata_code") or ""

                # Aircraft: try iata_code first, then name from Duffel aircraft object
                aircraft_obj = seg.get("aircraft") or {}
                aircraft_code = (
                    str(aircraft_obj.get("iata_code") or "").strip()
                    or str(aircraft_obj.get("name") or "").strip()
                )

                segments.append({
                    "duration": duration_iso,
                    "departure": {"iataCode": origin_code},
                    "arrival": {"iataCode": dest_code},
                    "carrierCode": carrier,
                    "number": seg.get("operating_carrier_flight_number", ""),
                    "aircraftCode": aircraft_code,
                })
            itineraries.append({"segments": segments})
    except Exception:
        pass

    return {
        "price": {"total": total, "currency": currency},
        "itineraries": itineraries,
        "stops": max_stops,
        "expires_at": offer.get("expires_at"),
        "_duffel_offer_id": offer.get("id"),
        "_raw": offer,
    }


def get_flight_offers(
    origin_location_code: str,
    destination_location_code: str,
    departure_date: str,
    return_date: Optional[str] = None,
    adults: int = 1,
    cabin_class: str = "economy",
    access_token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Search for flight offers via the Duffel API v2.

    Returns a list of offers normalized to Amadeus-compatible schema so
    that ``scoring.get_best_flights()`` can process them unchanged.

    Args:
        origin_location_code: IATA code for origin (e.g. "ZRH")
        destination_location_code: IATA code for destination (e.g. "JFK")
        departure_date: ISO 8601 date string (e.g. "2026-06-01")
        return_date: ISO 8601 return date. If provided, a round-trip
                     request with two slices is made.
        adults: Number of adult passengers.
        cabin_class: "economy", "premium_economy", "business", "first"
        access_token: Duffel API key (overrides DUFFEL_API_KEY env var).

    Returns:
        List of normalized offer dicts. Empty list on error or no results.
    """
    token = access_token or os.getenv("DUFFEL_API_KEY")
    if not token:
        raise RuntimeError(
            "DUFFEL_API_KEY is not set. Add it to your .env file."
        )

    origin = origin_location_code.strip().upper()
    dest = destination_location_code.strip().upper()

    # Build slices (one for one-way, two for round-trip)
    slices: List[Dict[str, Any]] = [
        {"origin": origin, "destination": dest, "departure_date": departure_date}
    ]
    if return_date:
        slices.append(
            {"origin": dest, "destination": origin, "departure_date": return_date}
        )

    passengers = [{"type": "adult"} for _ in range(max(1, adults))]

    payload = {
        "data": {
            "slices": slices,
            "passengers": passengers,
            "cabin_class": cabin_class,
        }
    }

    try:
        resp = requests.post(
            f"{_DUFFEL_API_URL}/air/offer_requests",
            params={"return_offers": "true"},
            headers={
                "Authorization": f"Bearer {token}",
                "Duffel-Version": _DUFFEL_API_VERSION,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=payload,
            timeout=30,
        )

        if resp.status_code not in (200, 201):
            err_msg = resp.json().get("errors", [{}])[0].get("message", resp.text)
            print(
                f"[duffel_api] offer_request failed {origin}->{dest} "
                f"{departure_date}: HTTP {resp.status_code}: {err_msg}"
            )
            return []

        data = resp.json().get("data") or {}
        raw_offers = data.get("offers") or []
        return [_normalize_offer_dict(o) for o in raw_offers]

    except Exception as e:
        print(
            f"[duffel_api] offer_request failed {origin}->{dest} "
            f"{departure_date}: {e!r}"
        )
        return []
