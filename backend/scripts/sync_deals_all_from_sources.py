#!/usr/bin/env python
"""Sync deals_all with deals_traveldealz and deals_secretflying.

For each row in deals_traveldealz / deals_secretflying whose `link` is
not already present in deals_all, inserts an equivalent row into
`deals_all`, setting `source` and leaving `scoring=None`.

Usage:

    python -m backend.scripts.sync_deals_all_from_sources
"""

from __future__ import annotations

from dotenv import load_dotenv, find_dotenv

import sys
from pathlib import Path
from typing import Any, Dict, List, Set


THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv(), override=True)

from database.supabase_db import _client  # type: ignore


def _load_existing_links() -> Set[str]:
    if not _client:
        return set()
    links: Set[str] = set()
    try:
        rsp = _client.table("deals_all").select("link").execute()
    except Exception as e:  # pragma: no cover - diagnostic only
        print("[sync_deals_all] Error reading deals_all:", repr(e))
        return set()
    rows = getattr(rsp, "data", []) or []
    for r in rows:
        link = (r.get("link") or "").strip()
        if link:
            links.add(link)
    return links


def _build_deals_all_row_from_source(row: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Adapt a row from deals_traveldealz / deals_secretflying to deals_all."""

    return {
        "title": row.get("title"),
        "price": row.get("price"),
        "currency": row.get("currency"),
        "link": row.get("link"),
        "booking_url": row.get("booking_url"),
        "origin": row.get("origin"),
        "destination": row.get("destination"),
        "origin_iata": row.get("origin_iata"),
        "destination_iata": row.get("destination_iata"),
        "date_out": row.get("date_out"),
        "date_in": row.get("date_in"),
        "cabin_class": row.get("cabin_class"),
        "one_way": row.get("one_way"),
        "flight": row.get("flight"),
        "source": source,
        "scoring": None,
    }


def _sync_table(table: str, source_label: str, existing_links: Set[str]) -> int:
    if not _client:
        return 0

    try:
        rsp = _client.table(table).select("*").execute()
    except Exception as e:
        print(f"[sync_deals_all] Error reading {table}:", repr(e))
        return 0

    rows = getattr(rsp, "data", []) or []
    to_insert: List[Dict[str, Any]] = []

    for r in rows:
        link = (r.get("link") or "").strip()
        if not link or link in existing_links:
            continue
        to_insert.append(_build_deals_all_row_from_source(r, source_label))

    if not to_insert:
        print(f"[sync_deals_all] Nothing to insert from {table} (everything already in deals_all)")
        return 0

    try:
        ins = _client.table("deals_all").insert(to_insert).execute()
        inserted = len(getattr(ins, "data", []) or [])
        print(f"[sync_deals_all] Inserted {inserted} new rows from {table}")
        return inserted
    except Exception as e:
        print(f"[sync_deals_all] Error inserting into deals_all from {table}:", repr(e))
        return 0


def main() -> None:
    if not _client:
        print("[sync_deals_all] Supabase client not configured")
        return

    existing = _load_existing_links()
    print(f"[sync_deals_all] deals_all tiene actualmente {len(existing)} links únicos")

    total_inserted = 0
    total_inserted += _sync_table("deals_traveldealz", "Travel-Dealz.de", existing)
    # Recalcular links existentes tras la primera inserción
    existing = _load_existing_links()
    total_inserted += _sync_table("deals_secretflying", "secretflying", existing)

    print(f"[sync_deals_all] Total insertadas en esta ejecución: {total_inserted}")


if __name__ == "__main__":  # pragma: no cover
    main()
