#!/usr/bin/env python
"""Backfill approximate miles for rows in deals.

- Uses origin_iata/destination_iata with a great-circle distance
  (airportsdata + haversine) to estimate flown miles.
- Only updates rows where miles is NULL (or missing) by default.

Usage (from repo root):

    python -m backend.scripts.recompute_miles_deals_all --limit 0

If --limit=0 (default), processes all rows.
"""

from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List

from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import sys

# Ensure backend/ on sys.path and load .env
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

from database.supabase_db import _client  # type: ignore  # noqa: E402
from scoring.miles_utils import great_circle_miles, approximate_program_miles  # noqa: E402


def _fetch_rows(limit: int | None = None) -> List[Dict[str, Any]]:
    if not _client:
        print("[recompute_miles_deals_all] Supabase no configurado; abortando.")
        return []

    try:
        query = _client.table("deals").select("id,origin_iata,destination_iata,miles")
        # Solo filas sin millas definidas (NULL)
        query = query.is_("miles", "null")
        if limit and limit > 0:
            query = query.limit(limit)
        rsp = query.execute()
    except Exception as e:  # pragma: no cover - defensive
        print("[recompute_miles_deals_all] Error al leer deals:", repr(e))
        return []

    rows = getattr(rsp, "data", []) or []
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recalcular millas aproximadas en deals usando códigos IATA.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Máximo de filas a procesar (0 = todas las filas sin millas).",
    )
    args = parser.parse_args()

    rows = _fetch_rows(limit=None if args.limit == 0 else args.limit)
    if not rows:
        print("[recompute_miles_deals_all] No hay filas sin millas; nada que hacer.")
        return

    print(f"[recompute_miles_deals_all] Recalculando millas para {len(rows)} filas...")

    updates: List[Dict[str, Any]] = []
    for row in rows:
        rid = row.get("id")
        o = (row.get("origin_iata") or "").strip().upper()
        d = (row.get("destination_iata") or "").strip().upper()
        if not rid or not o or not d:
            continue
        gc_miles = great_circle_miles(o, d)
        if gc_miles is None:
            continue
        approx = approximate_program_miles(gc_miles)
        if approx is None:
            continue
        updates.append({"id": rid, "miles": approx})

    if not updates:
        print("[recompute_miles_deals_all] Ninguna fila obtuvo millas válidas; nada que actualizar.")
        return

    try:
        _client.table("deals").upsert(updates, on_conflict="id").execute()
    except Exception as e:  # pragma: no cover
        print("[recompute_miles_deals_all] Error al aplicar updates:", repr(e))
        return

    print(f"[recompute_miles_deals_all] Updates aplicados en deals: {len(updates)}")


if __name__ == "__main__":  # pragma: no cover
    main()
