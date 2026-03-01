#!/usr/bin/env python
"""Batch checker/cleaner for dead deal URLs.

Usage (from project root):

    # Dry-run: list suspected dead URLs in deals
    python -m backend.scripts.cleanup_dead_deals_auto --table deals --limit 200

    # Actually delete dead deals (and related rows)
    python -m backend.scripts.cleanup_dead_deals_auto --table deals --limit 200 --apply

This script:
- Loads environment variables from the project .env
- Fetches recent rows from a Supabase deals table (deals_traveldealz / deals / ...)
- Checks each link (or booking_url fallback) via HTTP
- For URLs that look dead (4xx / network error), optionally calls
  cleanup_dead_deal --no-http-check to remove them from deals/deals_* + source_articles.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv, find_dotenv

# Ensure `backend/` is on sys.path
THIS_DIR = os.path.dirname(__file__)
BACKEND_ROOT = os.path.dirname(THIS_DIR)
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

# Load env vars (Supabase, etc.) from project root .env
load_dotenv(find_dotenv())

from database.supabase_db import _client  # type: ignore  # noqa: E402


def _http_status(url: str, timeout: int = 8) -> Optional[int]:
    try:
        resp = requests.head(url, allow_redirects=True, timeout=timeout)
        # Some sites don't support HEAD; fall back to GET once
        if resp.status_code >= 400 or resp.status_code == 405:
            resp = requests.get(url, allow_redirects=True, timeout=timeout)
        return resp.status_code
    except Exception:
        return None


def _iter_rows(table: str, limit: int, offset: int = 0) -> List[Dict[str, Any]]:
    if not _client:
        print("[cleanup_dead_deals_auto] Supabase client not configured (SUPABASE_URL/KEY missing).")
        return []
    try:
        # Intentar ordenar por created_at si existe; si falla, caer a una
        # consulta simple sin ordenación.
        try:
            q = _client.table(table).select("*")
            q = q.order("created_at", desc=True)
            if offset:
                q = q.range(offset, offset + limit - 1)
            else:
                q = q.limit(limit)
            rsp = q.execute()
            return getattr(rsp, "data", []) or []
        except Exception:
            q2 = _client.table(table).select("*")
            if offset:
                q2 = q2.range(offset, offset + limit - 1)
            else:
                q2 = q2.limit(limit)
            rsp2 = q2.execute()
            return getattr(rsp2, "data", []) or []
    except Exception as e:
        print(f"[cleanup_dead_deals_auto] Error fetching from {table}: {e!r}")
        return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch-check and optionally cleanup dead deal URLs in Supabase.",
    )
    parser.add_argument(
        "--table",
        default="deals",
        help="Supabase deals table to scan (e.g. deals)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max number of rows to check (from most recent).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Offset for pagination when scanning large tables.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually delete dead deals using cleanup_dead_deal. "
            "Without this flag the script only prints a report."
        ),
    )

    args = parser.parse_args()

    if not _client:
        print("[cleanup_dead_deals_auto] Supabase client not configured; aborting.")
        sys.exit(1)

    print(
        f"[cleanup_dead_deals_auto] Scanning table={args.table}, "
        f"limit={args.limit}, offset={args.offset} (apply={args.apply})...",
    )

    rows = _iter_rows(args.table, limit=args.limit, offset=args.offset)
    if not rows:
        print("[cleanup_dead_deals_auto] No rows fetched; nothing to do.")
        return

    dead: List[Dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        url = (row.get("link") or row.get("booking_url") or "").strip()
        if not url:
            continue

        status = _http_status(url)
        if status is None or status >= 400:
            dead.append({"url": url, "status": status})
            print(f"[cleanup_dead_deals_auto] DEAD? status={status} url={url}")
        else:
            print(f"[cleanup_dead_deals_auto] OK status={status} url={url}")

    print(
        f"[cleanup_dead_deals_auto] Finished scan. Suspected dead URLs: {len(dead)} / {len(rows)} checked.",
    )

    if not dead or not args.apply:
        if not args.apply:
            print("[cleanup_dead_deals_auto] Dry-run only; rerun with --apply to actually clean dead URLs.")
        return

    # Apply cleanup via cleanup_dead_deal for each dead URL
    for item in dead:
        url = item["url"]
        print(f"[cleanup_dead_deals_auto] Cleaning dead URL via cleanup_dead_deal: {url}")
        # Decide which source to clean based on table name
        if "secretflying" in args.table.lower():
            clean_arg = "secretflying"
        elif "traveldealz" in args.table.lower():
            clean_arg = "travel-dealz"
        else:
            clean_arg = "all"

        cmd = [
            sys.executable,
            "-m",
            "backend.scripts.cleanup_dead_deal",
            "--url",
            url,
            "--clean",
            clean_arg,
            "--no-http-check",
        ]
        print("[cleanup_dead_deals_auto] Running:", " ".join(cmd))
        os.system(" ".join(cmd))


if __name__ == "__main__":  # pragma: no cover
    main()
