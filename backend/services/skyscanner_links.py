"""Skyscanner affiliate link builder.

Generates deep-link search URLs for Skyscanner with the configured affiliate ID.
Links open a pre-filled flight search for the given route and dates.
"""

import os
from typing import Optional
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

_AFFILIATE_ID = os.getenv("SKYSCANNER_AFFILIATE_ID", "")
_UTM_MEDIUM = os.getenv("SKYSCANNER_UTM_MEDIUM", "affiliate")
_UTM_SOURCE = os.getenv("SKYSCANNER_UTM_SOURCE", "6511912-basics_db")

# Skyscanner cabin class parameter values
_CABIN_MAP: dict[str, str] = {
    "economy": "economy",
    "y": "economy",
    "premium economy": "premium_economy",
    "premium_economy": "premium_economy",
    "w": "premium_economy",
    "p": "premium_economy",
    "business": "business",
    "j": "business",
    "c": "business",
    "first": "business",  # Skyscanner doesn't distinguish First from Business in deep links
    "f": "business",
}


def build_skyscanner_link(
    origin_iata: str,
    dest_iata: str,
    depart_date: Optional[str] = None,
    return_date: Optional[str] = None,
    cabin_class: Optional[str] = None,
    affiliate_id: Optional[str] = None,
) -> Optional[str]:
    """Build a Skyscanner deep-link search URL.

    Args:
        origin_iata: Departure airport code (e.g. "ZRH").
        dest_iata: Destination airport code (e.g. "BKK").
        depart_date: ISO 8601 departure date (e.g. "2026-05-15"). Optional.
        return_date: ISO 8601 return date (e.g. "2026-05-25"). Optional.
        cabin_class: Cabin class string (e.g. "Economy", "Business"). Optional.
        affiliate_id: Override affiliate ID (uses SKYSCANNER_AFFILIATE_ID env var by default).

    Returns:
        Full Skyscanner URL string, or None if required fields are missing.
    """
    # Skyscanner canonical URLs use lowercase IATA codes
    origin = (origin_iata or "").strip().lower()
    dest = (dest_iata or "").strip().lower()
    if not origin or not dest:
        return None

    aff_id = affiliate_id or _AFFILIATE_ID

    # Format dates: Skyscanner expects YYMMDD in the path
    def _fmt_date(iso_date: Optional[str]) -> Optional[str]:
        if not iso_date:
            return None
        try:
            parts = str(iso_date).strip().split("-")
            if len(parts) == 3:
                yy = parts[0][2:]  # last 2 digits of year
                mm = parts[1].zfill(2)
                dd = parts[2].zfill(2)
                return f"{yy}{mm}{dd}"
        except Exception:
            pass
        return None

    depart_fmt = _fmt_date(depart_date)
    return_fmt = _fmt_date(return_date)

    # Build path: /transport/flights/{origin}/{dest}/{depart}/{return}/
    # Use "anytime" placeholders when dates are not specified
    dep_part = depart_fmt or "anytime"
    ret_part = return_fmt or "anytime"

    path = f"/transport/fluge/{origin}/{dest}/{dep_part}/{ret_part}/"
    base = f"https://www.skyscanner.ch{path}"

    # Query parameters
    params = ["adultsv2=2"]

    cabin_key = (cabin_class or "economy").strip().lower()
    skyscanner_cabin = _CABIN_MAP.get(cabin_key, "economy")
    params.append(f"cabinclass={skyscanner_cabin}")

    params.append("currency=CHF")

    if aff_id:
        params.append(f"associateid={aff_id}")
    if _UTM_MEDIUM:
        params.append(f"utm_medium={_UTM_MEDIUM}")
    if _UTM_SOURCE:
        params.append(f"utm_source={_UTM_SOURCE}")

    return f"{base}?{'&'.join(params)}"


def add_skyscanner_url(deal: dict) -> dict:
    """Return a copy of *deal* with a `skyscanner_url` field added (or updated).

    Does nothing if origin_iata or destination_iata is missing.
    """
    url = build_skyscanner_link(
        origin_iata=deal.get("origin_iata") or "",
        dest_iata=deal.get("destination_iata") or "",
        depart_date=deal.get("departure_date") or deal.get("date_out"),
        return_date=deal.get("return_date") or deal.get("date_in"),
        cabin_class=deal.get("cabin_class"),
    )
    if url:
        return {**deal, "skyscanner_url": url}
    return deal
