#!/usr/bin/env python
"""Fill route/month gaps using Duffel flight offers and patterns.json.

Usage (from repo root):

    python -m backend.scripts.fill_duffel_gaps_from_patterns \
        --origin ZRH --max-calls 30

This script looks at which (destination, month) pairs are not yet covered
in the unified `deals` table and queries Duffel for the cheapest available
offer. Results are scored and optionally persisted to Supabase.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv, find_dotenv

THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

from scoring.duffel_api import get_flight_offers  # type: ignore  # noqa: E402
from scoring.scoring import get_best_amadeus_flights as get_best_flights, _parse_iso8601_duration_to_minutes  # type: ignore  # noqa: E402
from scoring.miles_utils import great_circle_miles, choose_best_program_for_deal  # type: ignore  # noqa: E402
from services.deals_pipeline import (  # type: ignore  # noqa: E402
    score_deals,
    _load_duffel_benchmarks as _load_benchmarks,
    _resolve_city_name,
    _resolve_airline_name,
    _resolve_aircraft_model,
    _build_travel_period_display,
    _coerce_numeric_fields,
)
from services.baggage_format import get_baggage_defaults  # type: ignore  # noqa: E402
from services.unsplash_service import fetch_destination_image  # type: ignore  # noqa: E402
from services.skyscanner_links import add_skyscanner_url  # type: ignore  # noqa: E402
from database.supabase_db import save_deals, _client  # type: ignore  # noqa: E402


WEEKDAY_MAP = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


def _load_patterns() -> Dict[str, Any]:
    patterns_path = REPO_ROOT / "patterns.json"
    if not patterns_path.exists():
        raise SystemExit(f"patterns.json not found at {patterns_path}")
    with patterns_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _iso_date(d: date) -> str:
    return d.isoformat()


def _compute_future_months(today: date, months_ahead: int) -> Dict[int, int]:
    """Return mapping {month -> year} for the next ``months_ahead`` months.

    Used to restrict Duffel queries to a sliding window (e.g. next 4 months)
    while correctly handling year boundaries (Dec -> Jan).
    """
    if months_ahead <= 0:
        return {}

    month_to_year: Dict[int, int] = {}
    year = today.year
    month = today.month

    for i in range(months_ahead):
        mm = month + i
        yy = year + (mm - 1) // 12
        mm = ((mm - 1) % 12) + 1
        month_to_year[mm] = yy

    return month_to_year


def _first_valid_departure(year: int, month: int, weekday_name: str, today: date) -> Optional[date]:
    if month < today.month and year == today.year:
        return None

    weekday = WEEKDAY_MAP.get(weekday_name, 0)
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)

    if d < today and year == today.year and month == today.month:
        while d < today and d.month == month:
            d += timedelta(days=7)
        if d.month != month:
            return None
    return d


def _load_coverage_by_route_month(origin_iata: str) -> Set[Tuple[str, int]]:
    """Return (destination_iata, month) pairs already covered in the deals table.

    All sources (travel-dealz, secretflying, duffel) are considered covered
    so we don't duplicate benchmark entries.
    """
    covered: Set[Tuple[str, int]] = set()
    if not _client:
        return covered

    try:
        rsp = (
            _client.table("deals")
            .select("origin_iata,destination_iata,date_out")
            .eq("origin_iata", origin_iata)
            .execute()
        )
    except Exception as e:
        print(f"[fill_duffel_gaps] Warning: could not query deals: {e}")
        return covered

    rows = getattr(rsp, "data", []) or []
    for row in rows:
        dest = str(row.get("destination_iata") or "").upper()
        date_out = row.get("date_out")
        if not dest or not date_out:
            continue
        month: Optional[int] = None
        try:
            month = datetime.fromisoformat(str(date_out)).month
        except Exception:
            try:
                parts = str(date_out).split("-")
                if len(parts) >= 2:
                    month = int(parts[1])
            except Exception:
                month = None
        if month is None:
            continue
        covered.add((dest, month))

    return covered


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fill route/month gaps with Duffel flight prices using patterns.json, "
            "skipping destinations/months already covered in the unified deals table."
        ),
    )
    parser.add_argument("--origin", required=True, help="Origin IATA code (e.g. ZRH, GVA)")
    parser.add_argument(
        "--max-calls",
        type=int,
        default=50,
        help="Maximum number of Duffel API calls in this run (to limit costs)",
    )
    parser.add_argument(
        "--months-ahead",
        type=int,
        default=4,
        help="Number of months ahead (including current) to search for gaps. Default: 4.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not persist calculated rows to Supabase; only print a summary.",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help=(
            "Optional path to dump calculated rows as JSON. "
            "Written in both dry-run and persist modes. "
            "Relative paths are resolved from the repo root."
        ),
    )
    args = parser.parse_args()

    deterministic_enabled = os.getenv("DEALS_DETERMINISTIC_ENRICH", "true").strip().lower() in {
        "1", "true", "yes", "on",
    }

    origin = args.origin.strip().upper()
    today = date.today()

    month_to_year = _compute_future_months(today, args.months_ahead)
    patterns = _load_patterns()
    covered = _load_coverage_by_route_month(origin)

    print(
        f"[fill_duffel_gaps] origin={origin}, today={today.isoformat()}, "
        f"months_window={sorted(month_to_year.items())}, covered_pairs={len(covered)}",
    )

    rows_to_save: List[Dict[str, Any]] = []
    calls_done = 0

    for dest_iata, cfg in patterns.items():
        dest = str(dest_iata).upper()
        try:
            cheap_months = cfg.get("cheap_months") or []
            depart_weekdays = cfg.get("depart_weekdays") or ["Tue"]
            trip_lengths = cfg.get("trip_lengths") or [7]
        except Exception:
            continue

        if not cheap_months:
            continue

        depart_weekday = str(depart_weekdays[0])
        trip_length = int(trip_lengths[0])

        for month in cheap_months:
            try:
                month_int = int(month)
            except Exception:
                continue

            year_for_month = month_to_year.get(month_int)
            if year_for_month is None:
                continue

            if (dest, month_int) in covered:
                continue

            dep_date = _first_valid_departure(year_for_month, month_int, depart_weekday, today)
            if not dep_date:
                continue

            ret_date = dep_date + timedelta(days=trip_length)

            print(
                f"[fill_duffel_gaps] GAP {origin}->{dest} month={month_int}; "
                f"querying Duffel for {dep_date}–{ret_date}",
            )

            try:
                offers = get_flight_offers(
                    origin_location_code=origin,
                    destination_location_code=dest,
                    departure_date=_iso_date(dep_date),
                    return_date=_iso_date(ret_date),
                    adults=1,
                )
            except Exception as e:
                print(f"[fill_duffel_gaps] Duffel error for {origin}->{dest}: {e!r}")
                continue

            if not isinstance(offers, list) or not offers:
                print(f"[fill_duffel_gaps] No offers for {origin}->{dest}")
                continue

            ranked = get_best_flights(offers, top_n=1)
            if not ranked:
                print(f"[fill_duffel_gaps] Could not rank offers for {origin}->{dest}")
                continue

            best = ranked[0]
            offer = best.get("offer", {})
            price_total = best.get("price")

            price_info = offer.get("price") or {}
            currency = price_info.get("currency") or "EUR"

            # Extract airline, aircraft, stops, expires_at, and duration from normalized offer
            airline: Optional[str] = None
            aircraft_code: Optional[str] = None
            reported_duration_minutes: Optional[int] = None
            one_way = False
            # stops: use max_stops pre-computed across all slices by _normalize_offer_dict
            stops_count: int = offer.get("stops") or 0
            expires_at: Optional[str] = offer.get("expires_at")

            try:
                itins = offer.get("itineraries") or []
                if itins:
                    segments = itins[0].get("segments") or []
                    if segments:
                        seg0 = segments[0]
                        airline = seg0.get("carrierCode")
                        aircraft_code = seg0.get("aircraftCode") or None

                    # Duration: average per leg (outbound/inbound) instead of total sum
                    durations: List[int] = []
                    for itin in itins:
                        for seg in itin.get("segments", []):
                            dur_str = seg.get("duration")
                            if dur_str:
                                try:
                                    mins = _parse_iso8601_duration_to_minutes(str(dur_str))
                                    if mins and mins > 0:
                                        durations.append(int(mins))
                                except Exception:
                                    pass
                    if durations:
                        reported_duration_minutes = int(round(sum(durations) / max(len(itins), 1)))

                    one_way = len(itins) == 1
            except Exception:
                pass

            cabin_class = "Economy"
            origin_name = _resolve_city_name(None, origin) or origin
            dest_name = _resolve_city_name(None, dest) or dest
            airline_name = _resolve_airline_name(airline, airline)
            aircraft_model = _resolve_aircraft_model(aircraft_code, aircraft_code)

            # Aircraft fallback: if Duffel didn't provide the model, infer from route distance.
            if not aircraft_model:
                # gc_miles may not be computed yet; compute it now for the fallback.
                _fallback_gc = None
                try:
                    _fallback_gc = great_circle_miles(origin, dest)
                except Exception:
                    pass
                if _fallback_gc is not None:
                    if _fallback_gc >= 4000:
                        aircraft_model = "Boeing 777 / A350"
                    elif _fallback_gc >= 2000:
                        aircraft_model = "A330 / Boeing 787"
                    elif _fallback_gc >= 800:
                        aircraft_model = "A320 / Boeing 737"
                    else:
                        aircraft_model = "A220 / Embraer E2"

            title = f"{origin_name} ({origin}) -> {dest_name} ({dest}) ({_iso_date(dep_date)}-{_iso_date(ret_date)})"

            # Miles: deterministic via great-circle distance + program earning rates
            miles_display: Optional[str] = None
            est_oneway_minutes: Optional[int] = None
            gc_miles: Optional[int] = None
            if deterministic_enabled:
                try:
                    gc_miles = great_circle_miles(origin, dest)
                except Exception:
                    gc_miles = None
                if gc_miles is not None and gc_miles > 0:
                    est_oneway_minutes = int(round((gc_miles / 500.0) * 60))
                    best_prog, best_est = choose_best_program_for_deal(
                        gc_miles, airline_name or airline, cabin_class=cabin_class, roundtrip=not one_way
                    )
                    if best_prog and best_est:
                        miles_display = f"{best_prog} · {best_est:,}".replace(",", "'")

            # Adjust duration: if reported value looks like a full round-trip, use one-leg estimate
            final_duration_minutes: Optional[int] = None
            if reported_duration_minutes and est_oneway_minutes:
                if not one_way and reported_duration_minutes > 1.6 * est_oneway_minutes:
                    final_duration_minutes = est_oneway_minutes
                else:
                    final_duration_minutes = reported_duration_minutes
            elif reported_duration_minutes:
                final_duration_minutes = reported_duration_minutes
            elif deterministic_enabled and est_oneway_minutes:
                final_duration_minutes = est_oneway_minutes

            duration_display: Optional[str] = None
            if final_duration_minutes and final_duration_minutes > 0:
                h, m = divmod(int(final_duration_minutes), 60)
                if h and m:
                    duration_display = f"{h}h {m}m"
                elif h:
                    duration_display = f"{h}h"
                else:
                    duration_display = f"{m}m"

            # Baggage: always populate from hardcoded defaults (airline + cabin)
            airline_iata = str(airline or "").strip().upper()
            baggage_defaults = get_baggage_defaults(airline_iata, cabin_class)

            # Unsplash image for the destination
            dest_image: Optional[str] = None
            try:
                dest_image = fetch_destination_image(dest_name)
            except Exception:
                pass

            row: Dict[str, Any] = {
                "title": title,
                "price": price_total,
                "currency": currency,
                "link": None,
                "booking_url": None,
                "origin": origin_name,
                "destination": dest_name,
                "origin_iata": origin,
                "destination_iata": dest,
                "date_out": _iso_date(dep_date),
                "date_in": _iso_date(ret_date),
                "travel_period_display": _build_travel_period_display({
                    "date_out": _iso_date(dep_date),
                    "date_in": _iso_date(ret_date),
                }),
                "airline": airline_name or airline,
                "aircraft": aircraft_model or None,
                "cabin_class": cabin_class,
                "stops": stops_count,
                "expires_in": expires_at,
                "miles": miles_display,
                "flight_duration_minutes": final_duration_minutes,
                "flight_duration_display": duration_display,
                "baggage_included": baggage_defaults.get("baggage_included"),
                "baggage_pieces_included": baggage_defaults.get("baggage_pieces_included"),
                "baggage_allowance_kg": baggage_defaults.get("baggage_allowance_kg"),
                "image": dest_image,
                "source": "duffel",
            }

            # Skyscanner URL — capture the returned copy (add_skyscanner_url is non-mutating)
            try:
                row = add_skyscanner_url(row)
            except Exception:
                pass
            # booking_url stays None for Duffel deals — Duffel offers have no public booking URL.
            # The Skyscanner URL (skyscanner_url) is used as the CTA in HTML output instead.

            rows_to_save.append(row)
            covered.add((dest, month_int))
            calls_done += 1

            if calls_done >= args.max_calls:
                print(f"[fill_duffel_gaps] Reached max-calls={args.max_calls}; stopping.")
                break

        if calls_done >= args.max_calls:
            break

    if not rows_to_save:
        print("[fill_duffel_gaps] No new rows to save; nothing to do.")
        return

    # Score deals using the same logic as the pipeline
    benchmarks = _load_benchmarks()
    deals_for_scoring: List[Dict[str, Any]] = [
        {
            "id": idx,
            "title": row.get("title"),
            "price": row.get("price"),
            "currency": row.get("currency"),
            "origin_iata": row.get("origin_iata"),
            "destination_iata": row.get("destination_iata"),
            "date_out": row.get("date_out"),
            "flight_duration_minutes": row.get("flight_duration_minutes"),
            "stops": row.get("stops"),
        }
        for idx, row in enumerate(rows_to_save)
    ]

    scored = score_deals(deals_for_scoring, benchmarks)
    scores_by_id: Dict[int, Any] = {}
    for d in scored:
        try:
            scores_by_id[int(d.get("id"))] = d.get("score")
        except Exception:
            continue

    for idx, row in enumerate(rows_to_save):
        if idx in scores_by_id and scores_by_id[idx] is not None:
            row["scoring"] = scores_by_id[idx]

    cleaned_rows: List[Dict[str, Any]] = []
    for row in rows_to_save:
        row_clean = _coerce_numeric_fields(dict(row))
        cleaned_rows.append(row_clean)
    rows_to_save = cleaned_rows

    # Optional JSON export
    if getattr(args, "output_json", None):
        out_path = Path(args.output_json)
        if not out_path.is_absolute():
            out_path = REPO_ROOT / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(rows_to_save, f, ensure_ascii=False, indent=2, default=str)
        print(f"[fill_duffel_gaps] Wrote {len(rows_to_save)} rows to {out_path}")

    if args.dry_run:
        print(f"[fill_duffel_gaps] dry-run: would save {len(rows_to_save)} rows (not persisted).")
        for r in rows_to_save[:3]:
            print(f"  {r.get('title')} | price={r.get('price')} {r.get('currency')} | score={r.get('scoring')}")
        return

    result = save_deals("deals", rows_to_save)
    status = result.get("status")
    if status == "ok":
        saved = len((result.get("data") or []))
        print(f"[fill_duffel_gaps] Saved {saved} rows to Supabase (deals table).")
    else:
        print(f"[fill_duffel_gaps] Save error: {result}")


if __name__ == "__main__":
    main()
