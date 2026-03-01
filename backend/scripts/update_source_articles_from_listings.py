#!/usr/bin/env python
"""Populate public.source_articles from listing pages.

Usage (from repo root):

    python -m backend.scripts.update_source_articles_from_listings \
        --source travel-dealz --limit 100

This script reads the normal listing pages (Travel-Dealz and/or
SecretFlying), takes the article URLs, and upserts them into the
`source_articles` table in Supabase, keyed by `article_url`.

Later you can have another job that looks at `source_articles` with
status = 'pending' and scrapes only those articles, skipping ones that
already have rows in deals_traveldealz / deals_secretflying.
"""

import argparse
import os
import sys
from typing import Any, Dict, List, Set

from pathlib import Path

from dotenv import load_dotenv, find_dotenv


# Ensure backend/ is on sys.path when running as a module
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


load_dotenv(find_dotenv())

# Local imports after adjusting sys.path
from scrapers.travel_dealz import get_deals as get_travel_dealz, get_deals_de as get_travel_dealz_de  # type: ignore  # noqa: E402
# Para SecretFlying, en modo "discovery" de URLs usamos solo el
# fallback ligero basado en requests (_get_deals_requests), evitando
# ScraperAPI / Playwright. Si falla, simplemente no añadimos URLs de
# SecretFlying en esta ejecución, pero no rompemos el script.
from scrapers.secretflying import _get_deals_requests as get_secretflying_light  # type: ignore  # noqa: E402
from database.supabase_db import _client  # type: ignore  # noqa: E402


def _collect_travel_dealz_urls(limit: int) -> List[Dict[str, Any]]:
    deals: List[Dict[str, Any]] = []

    td_com = get_travel_dealz(limit=limit)
    td_de = get_travel_dealz_de(limit=limit)
    deals.extend(td_com)
    deals.extend(td_de)

    rows: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for d in deals:
        url = str(d.get("link") or "").strip()
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        # Marcamos explícitamente como "pending" para que otros jobs
        # puedan distinguir URLs aún no procesadas.
        rows.append({"article_url": url, "source": "travel-dealz", "status": "pending"})
    return rows


def _collect_secretflying_urls(limit: int) -> List[Dict[str, Any]]:
    try:
        deals = get_secretflying_light(limit=limit)
    except Exception as e:  # pragma: no cover - entorno con bloqueos/anti-bots
        print(f"[source_articles] Warning: failed to collect SecretFlying URLs via requests-only path: {e!r}")
        return []
    rows: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for d in deals:
        url = str(d.get("link") or "").strip()
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        # Igual que en Travel-Dealz, inicializamos como "pending".
        rows.append({"article_url": url, "source": "secretflying", "status": "pending"})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate public.source_articles from listing pages"
    )
    parser.add_argument(
        "--source",
        choices=["travel-dealz", "secretflying", "both"],
        default="both",
        help="Which source to use for discovery",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of deals to read from listings per source",
    )
    args = parser.parse_args()

    if not _client:
        raise SystemExit(
            "Supabase client is not configured (check SUPABASE_URL / SUPABASE_KEY)"
        )

    rows: List[Dict[str, Any]] = []

    if args.source in {"travel-dealz", "both"}:
        print("[source_articles] Collecting Travel-Dealz article URLs…")
        rows.extend(_collect_travel_dealz_urls(args.limit))

    if args.source in {"secretflying", "both"}:
        print("[source_articles] Collecting SecretFlying article URLs…")
        rows.extend(_collect_secretflying_urls(args.limit))

    if not rows:
        print("[source_articles] No article URLs collected; nothing to upsert.")
        return

    # Deduplicate again across sources just in case
    deduped: List[Dict[str, Any]] = []
    seen_urls: Set[str] = set()
    for r in rows:
        url = r.get("article_url")
        if not isinstance(url, str) or not url:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(r)

    print(f"[source_articles] Upserting {len(deduped)} rows into public.source_articles…")

    try:
        rsp = (
            _client.table("source_articles")
            .upsert(deduped, on_conflict="article_url")
            .execute()
        )
    except Exception as e:  # pragma: no cover - runtime failure path
        print(f"[source_articles] Error upserting into Supabase: {e!r}")
        raise SystemExit(1)

    inserted = len(getattr(rsp, "data", []) or [])
    print(f"[source_articles] Upsert completed. Supabase returned {inserted} rows.")


if __name__ == "__main__":
    main()
