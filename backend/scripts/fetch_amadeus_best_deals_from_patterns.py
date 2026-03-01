#!/usr/bin/env python
"""Fetch best Amadeus deals for destinations in patterns.json.

Usage (from repo root):

    python -m backend.scripts.fetch_amadeus_best_deals_from_patterns \
        --origin ZRH --departure-date 2025-11-10 --trip-length 10 --adults 1

This script:
- Loads patterns.json from the project root.
- Picks all destinations whose `cheap_months` include the departure month.
- For each destination, calls Amadeus (round-trip) and ranks offers.
- Stores the best offer per destination into the `deals_amadeus` table
  in Supabase, using the same general schema style as `deals`.

It is intentionally conservative: one Amadeus call per destination and
parameters, to control API usage. You can run it multiple times with
different dates / lengths to build up a benchmark history.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys
from typing import Any, Dict, List

from dotenv import load_dotenv, find_dotenv


# Ensure backend/ is on sys.path when running as a module
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


load_dotenv(find_dotenv())

# Local imports after adjusting sys.path
from scoring.amadeus_api import get_flight_offers  # type: ignore  # noqa: E402
from scoring.scoring import get_best_amadeus_flights  # type: ignore  # noqa: E402
from database.supabase_db import save_deals  # type: ignore  # noqa: E402


def _load_patterns() -> Dict[str, Any]:
    patterns_path = REPO_ROOT / "patterns.json"
    if not patterns_path.exists():
        raise SystemExit(f"patterns.json not found at {patterns_path}")
    with patterns_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _iso_date(d: datetime) -> str:
    return d.date().isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch best Amadeus deals using destinations from patterns.json",
    )
    parser.add_argument(
        "--origin",
        required=True,
        help="Origin IATA airport code (e.g. ZRH, MAD)",
    )
    parser.add_argument(
        "--departure-date",
        required=True,
        help="Departure date in YYYY-MM-DD (local)",
    )
    parser.add_argument(
        "--trip-length",
        type=int,
        required=True,
        help="Trip length in days (used to compute return date)",
    )
    parser.add_argument(
        "--adults",
        type=int,
        default=1,
        help="Number of adults for Amadeus search (default 1)",
    )
    parser.add_argument(
        "--max-destinations",
        type=int,
        default=50,
        help="Max destinations from patterns to query in one run (to limit API calls)",
    )
    args = parser.parse_args()

    try:
        dep_dt = datetime.strptime(args.departure_date, "%Y-%m-%d")
    except ValueError:
        raise SystemExit("--departure-date must be in YYYY-MM-DD format")

    if args.trip_length <= 0:
        raise SystemExit("--trip-length must be a positive integer (days)")

    ret_dt = dep_dt + timedelta(days=args.trip_length)

    month = dep_dt.month

    patterns = _load_patterns()

    # Filter destinations whose cheap_months contain the departure month
    candidates: List[str] = []
    for dest, cfg in patterns.items():
        try:
            cheap_months = cfg.get("cheap_months") or []
            if month in cheap_months:
                candidates.append(dest)
        except Exception:
            continue

    if not candidates:
        print("[amadeus_patterns] No destinations in patterns.json match the departure month.")
        return

    candidates = candidates[: args.max_destinations]

    print(
        f"[amadeus_patterns] Origin={args.origin}, departure={args.departure_date}, "
        f"return={_iso_date(ret_dt)}, month={month}, destinations={len(candidates)}"
    )

    rows_to_save: List[Dict[str, Any]] = []

    for dest in candidates:
        print(f"[amadeus_patterns] Querying Amadeus for {args.origin} -> {dest}…")
        try:
            offers = get_flight_offers(
                origin_location_code=args.origin,
                destination_location_code=dest,
                departure_date=_iso_date(dep_dt),
                returnDate=_iso_date(ret_dt),
                duration=str(args.trip_length),
                adults=args.adults,
            )
        except Exception as e:
            print(f"[amadeus_patterns] Amadeus error for {args.origin}->{dest}: {e!r}")
            continue

        if not isinstance(offers, list) or not offers:
            print(f"[amadeus_patterns] No offers returned for {args.origin}->{dest}")
            continue

        ranked = get_best_amadeus_flights(offers, top_n=1)
        if not ranked:
            print(f"[amadeus_patterns] Could not rank offers for {args.origin}->{dest}")
            continue

        best = ranked[0]
        offer = best.get("offer", {})
        price_total = best.get("price")

        price_info = offer.get("price") or {}
        currency = price_info.get("currency") or "EUR"

        # Try to infer cabin class from first travelerPricing entry (if present)
        cabin_class = None
        try:
            tps = offer.get("travelerPricings") or []
            if tps:
                fd = (tps[0].get("fareDetailsBySegment") or [])
                if fd:
                    cabin_class = fd[0].get("cabin")
        except Exception:
            cabin_class = None

        title = f"{args.origin} → {dest} ({_iso_date(dep_dt)}–{_iso_date(ret_dt)})"

        row: Dict[str, Any] = {
            "title": title,
            "price": price_total,
            "currency": currency,
            # No booking_url / link from Amadeus; they remain NULL.
            "link": None,
            "booking_url": None,
            # Use IATA codes as both city and IATA fields for now.
            "origin": args.origin,
            "destination": dest,
            "origin_iata": args.origin,
            "destination_iata": dest,
            "date_out": _iso_date(dep_dt),
            "date_in": _iso_date(ret_dt),
            "cabin_class": cabin_class,
        }

        rows_to_save.append(row)

    if not rows_to_save:
        print("[amadeus_patterns] No rows to save to deals_amadeus.")
        return

    print(f"[amadeus_patterns] Saving {len(rows_to_save)} rows into deals_amadeus…")

    result = save_deals("deals_amadeus", rows_to_save)
    print("[amadeus_patterns] Supabase result:", result)


if __name__ == "__main__":
    main()
