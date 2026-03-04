#!/usr/bin/env python
"""Copy Amadeus deals (deals_amadeus) to the deals table.

Usage (from repo root):

    python -m backend.scripts.export_amadeus_to_deals \
        --origins ZRH,BSL

If --origins is not provided, ORIGIN_IATA_FILTER from .env is used if it exists;
if that is not defined either, all origins are exported.
"""

from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import sys

# Ensure backend/ is on sys.path and load .env before importing Supabase
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

from database.supabase_db import _client, save_deals  # type: ignore  # noqa: E402
from scoring.miles_utils import great_circle_miles, approximate_program_miles  # noqa: E402
from services.deals_pipeline import (
    _resolve_city_name,
    _resolve_airline_name,
    _resolve_aircraft_model,
    _extract_llm_meta,
)
from services.deals_pipeline import _load_amadeus_benchmarks, score_deals  # type: ignore  # noqa: E402
from services.deals_enrichment import enrich_deals_batch  # type: ignore  # noqa: E402


def _estimate_duration_minutes(origin_iata: Any, destination_iata: Any) -> Optional[int]:
    """Estimate duration minutes at ~500 mph great-circle; returns None on failure."""

    if not origin_iata or not destination_iata:
        return None
    try:
        gc_miles = great_circle_miles(str(origin_iata).strip().upper(), str(destination_iata).strip().upper())
    except Exception:
        gc_miles = None
    if not gc_miles or gc_miles <= 0:
        return None
    mins = int(round((gc_miles / 500.0) * 60))
    return max(mins, 40)


def _format_duration(minutes: Optional[int]) -> Optional[str]:
    if not isinstance(minutes, (int, float)) or minutes <= 0:
        return None
    h, m = divmod(int(minutes), 60)
    if h and m:
        return f"{h}h {m}m"
    if h:
        return f"{h}h"
    return f"{m}m"


def _parse_origins_arg(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    return {p.strip().upper() for p in raw.split(",") if p.strip()}


def _get_origin_filter_from_env() -> Set[str]:
    raw = os.getenv("ORIGIN_IATA_FILTER", "").strip()
    if not raw:
        return set()
    return {p.strip().upper() for p in raw.split(",") if p.strip()}


def _load_existing_deal_keys() -> Set[Tuple[str, str, str, str]]:
    """Load keys (origin_iata,destination_iata,date_out,date_in) already present in deals.

    This is used to avoid inserting duplicates if the script is run multiple times.
    """

    keys: Set[Tuple[str, str, str, str]] = set()
    if not _client:
        return keys
    try:
        rsp = (
            _client.table("deals")
            .select("origin_iata,destination_iata,date_out,date_in")
            .execute()
        )
    except Exception:
        return keys

    rows = getattr(rsp, "data", []) or []
    for row in rows:
        o = str(row.get("origin_iata") or "").strip().upper()
        d = str(row.get("destination_iata") or "").strip().upper()
        out = str(row.get("date_out") or "").strip()
        inn = str(row.get("date_in") or "").strip()
        if not (o and d and out and inn):
            continue
        keys.add((o, d, out, inn))
    return keys


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export rows from deals_amadeus to deals_all using the same "
            "schema as deals_traveldealz/deals_secretflying, avoiding "
            "basic duplicates."
        ),
    )
    parser.add_argument(
        "--origins",
        help=(
            "Comma-separated list of origin IATA codes to export "
            "(e.g. ZRH,BSL). If omitted, ORIGIN_IATA_FILTER from the "
            "environment is used; if that is not defined either, all origins are exported."
        ),
    )
    args = parser.parse_args()

    if not _client:
        print("[export_amadeus_to_deals] Supabase not configured; aborting.")
        return

    origins = _parse_origins_arg(args.origins)
    if not origins:
        origins = _get_origin_filter_from_env()

    if origins:
        print(f"[export_amadeus_to_deals] Filtering by origins: {sorted(origins)}")
    else:
        print("[export_amadeus_to_deals] No origin filter; exporting all records.")

    try:
        rsp = _client.table("deals_amadeus").select("*").execute()
    except Exception as e:
        print(f"[export_amadeus_to_deals] Error reading deals_amadeus: {e!r}")
        return

    rows = getattr(rsp, "data", []) or []
    if not isinstance(rows, list) or not rows:
        print("[export_amadeus_to_deals] No rows in deals_amadeus; nothing to export.")
        return

    def _keep_row(row: Dict[str, Any]) -> bool:
        if not origins:
            return True
        origin = str(row.get("origin_iata") or row.get("origin") or "").strip().upper()
        return origin in origins if origin else False

    filtered_rows: List[Dict[str, Any]] = [r for r in rows if isinstance(r, dict) and _keep_row(r)]

    if not filtered_rows:
        print("[export_amadeus_to_deals] No rows match the origin filter; nothing to do.")
        return

    existing_keys = _load_existing_deal_keys()
    print(f"[export_amadeus_to_deals] Existing keys in deals: {len(existing_keys)}")

    payload: List[Dict[str, Any]] = []

    for r in filtered_rows:
        origin_iata = str(r.get("origin_iata") or r.get("origin") or "").strip().upper()
        dest_iata = str(r.get("destination_iata") or r.get("destination") or "").strip().upper()
        date_out = str(r.get("date_out") or "").strip()
        date_in = str(r.get("date_in") or "").strip()

        if origin_iata and dest_iata and date_out and date_in:
            key = (origin_iata, dest_iata, date_out, date_in)
            if key in existing_keys:
                continue

        # Build short flight name like "ZRH → JFK" and derive
        # approximate miles and duration from the distance.
        flight_name = None
        approx_miles: Optional[int] = None
        flight_duration_minutes: Optional[int] = None
        flight_duration_display: Optional[str] = None
        itineraries: List[Dict[str, Any]] = []

        one_way = not bool(date_in)

        if origin_iata and dest_iata:
            orig_name = _resolve_city_name(None, origin_iata) or origin_iata
            dest_name = _resolve_city_name(None, dest_iata) or dest_iata
            flight_name = f"{orig_name} ({origin_iata}) → {dest_name} ({dest_iata})"
            gc_miles = great_circle_miles(origin_iata, dest_iata)
            if gc_miles is not None:
                approx_miles = approximate_program_miles(gc_miles)
                # Estimate outbound
                out_mins = _estimate_duration_minutes(origin_iata, dest_iata)
                out_disp = _format_duration(out_mins)
                if out_mins:
                    flight_duration_minutes = out_mins
                    flight_duration_display = out_disp
                    itineraries.append(
                        {
                            "origin_iata": origin_iata,
                            "destination_iata": dest_iata,
                            "flight_duration_minutes": out_mins,
                            "flight_duration_display": out_disp,
                            "direct": True,
                            "date": date_out or None,
                        }
                    )

                # Estimate return only if there is a return date
                if not one_way:
                    ret_mins = _estimate_duration_minutes(dest_iata, origin_iata)
                    ret_disp = _format_duration(ret_mins)
                    if ret_mins:
                        itineraries.append(
                            {
                                "origin_iata": dest_iata,
                                "destination_iata": origin_iata,
                                "flight_duration_minutes": ret_mins,
                                "flight_duration_display": ret_disp,
                                "direct": True,
                                "date": date_in or None,
                            }
                        )

        row_payload: Dict[str, Any] = {
            "title": r.get("title") or flight_name,
            "price": r.get("price"),
            # For Amadeus there is normally no article URL; we keep the link from
            # deals_amadeus (if any) and leave booking_url empty.
            "link": r.get("link"),
            # We do not set booking_url to avoid format conflicts; it can
            # be filled in later if a custom CTA is added.
            "booking_url": None,
            "currency": r.get("currency"),
            "image": None,
            "cabin_baggage": None,
            "aircraft": _resolve_aircraft_model(r.get("aircraft"), r.get("aircraft")) or r.get("aircraft"),
            "airline": _resolve_airline_name(r.get("airline"), r.get("airline")) or r.get("airline"),
            # Fill in city/airport names from IATA codes if not provided.
            "origin": _resolve_city_name(r.get("origin"), origin_iata) or _resolve_city_name(None, origin_iata),
            "destination": _resolve_city_name(r.get("destination"), dest_iata) or _resolve_city_name(None, dest_iata),
            "miles": approx_miles,
            "expires_in": None,
            "date_range": r.get("date_range"),
            "date_out": date_out or None,
            "date_in": date_in or None,
            "cabin_class": r.get("cabin_class"),
            "one_way": one_way,
            "flight": flight_name,
            "origin_iata": origin_iata or None,
            "destination_iata": dest_iata or None,
            "source": "amadeus",
            # Estimated duration based on distance.
            "flight_duration_minutes": flight_duration_minutes,
            "flight_duration_display": flight_duration_display,
            "itineraries": itineraries or None,
            # scoring will be filled below using the same logic as the pipeline.
            "scoring": None,
        }

        payload.append(row_payload)

    if not payload:
        print("[export_amadeus_to_deals] No new rows to insert into deals.")
        return

    # Optional enrichment (OpenAI) for all Amadeus rows
    # when DEALS_ENRICH_DEFAULT=true (or true by default if not defined).
    enrich_default = os.getenv("DEALS_ENRICH_DEFAULT", "true").strip().lower() in {"1", "true", "yes", "on"}
    if enrich_default:
        payload = enrich_deals_batch(payload, max_items=len(payload))

    # Normalize LLM metadata: if enrichment did not happen or failed,
    # llm_enriched=False and null fields are stored; if it did, the
    # returned fields are preserved.
    normalized_payload: List[Dict[str, Any]] = []
    for row in payload:
        row_norm = dict(row)
        row_norm.update(_extract_llm_meta(row))
        normalized_payload.append(row_norm)
    payload = normalized_payload

    # Compute scoring using the same logic as the main pipeline.
    benchmarks = _load_amadeus_benchmarks()
    deals_for_scoring: List[Dict[str, Any]] = []
    for idx, row in enumerate(payload):
        deals_for_scoring.append(
            {
                "id": idx,
                "title": row.get("title"),
                "price": row.get("price"),
                "currency": row.get("currency"),
                "origin_iata": row.get("origin_iata"),
                "destination_iata": row.get("destination_iata"),
                # score_deals looks at `departure_date` or `date_out` to extract the month
                "date_out": row.get("date_out"),
                "departure_date": row.get("date_out"),
            }
        )

    scored = score_deals(deals_for_scoring, benchmarks)
    scores_by_id: Dict[int, Any] = {}
    for d in scored:
        try:
            did = int(d.get("id"))
        except Exception:
            continue
        scores_by_id[did] = d.get("score")

    for idx, row in enumerate(payload):
        if idx in scores_by_id and scores_by_id[idx] is not None:
            row["scoring"] = scores_by_id[idx]

    print(f"[export_amadeus_to_deals] Inserting {len(payload)} new rows into deals...")
    result = save_deals("deals", payload)
    print("[export_amadeus_to_deals] Supabase result:", result)


if __name__ == "__main__":  # pragma: no cover
    main()
