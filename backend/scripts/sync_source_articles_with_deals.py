#!/usr/bin/env python
"""Sync public.source_articles with the unified deals table.

Usage (from repo root):

        python -m backend.scripts.sync_source_articles_with_deals --limit 5000

This script reads distinct article links from the unified `deals` table in
Supabase and upserts them into the `source_articles` table with
status = 'done'.

The goal is to enforce the invariant:

- If an article URL appears in the deals table, `source_articles` must have
    a row with that URL marked as status = 'done'.

Together with update_source_articles_from_listings.py (which seeds
`source_articles` with status = 'pending' from listing pages), this allows
other jobs to:

- Only scrape articles whose status is 'pending'.
- Skip URLs already processed (status = 'done').
"""

import argparse
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
from database.supabase_db import _client  # type: ignore  # noqa: E402


def _collect_article_urls_from_deals(limit: int) -> Set[str]:
    """Return a set of distinct non-empty `link` URLs from the deals table.

    We assume that `link` stores the canonical article URL. Only a single
    page of `limit` rows is fetched, which should be sufficient for
    moderate-sized datasets. Increase `--limit` if needed.
    """

    urls: Set[str] = set()

    if not _client:
        return urls

    try:
        rsp = (
            _client.table("deals")
            .select("link, source")
            .neq("link", None)
            .limit(limit)
            .execute()
        )
    except Exception as e:  # pragma: no cover - runtime failure path
        print("[sync_source_articles] Error querying deals:", f"{e!r}")
        return urls

    data = getattr(rsp, "data", []) or []
    for row in data:
        url = str(row.get("link") or "").strip()
        if not url:
            continue
        urls.add(url)

    return urls


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync public.source_articles status='done' from the deals table",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum number of rows to read per deals table",
    )
    args = parser.parse_args()

    if not _client:
        raise SystemExit(
            "Supabase client is not configured (check SUPABASE_URL / SUPABASE_KEY)"
        )

    urls_with_source: List[Dict[str, Any]] = []

    print("[sync_source_articles] Collecting article URLs from deals…")
    urls = _collect_article_urls_from_deals(args.limit)
    for url in urls:
        # Conservatively no longer infer the source from per-table names; we
        # just mark the URL as done, preserving any existing source when
        # possible via ON CONFLICT in Supabase.
        urls_with_source.append(
            {"article_url": url, "status": "done"}
        )

    if not urls_with_source:
        print("[sync_source_articles] No article URLs collected from deals tables; nothing to sync.")
        return

    # Deduplicate by article_url to avoid hitting ON CONFLICT multiple times
    deduped: List[Dict[str, Any]] = []
    seen_urls: Set[str] = set()
    for row in urls_with_source:
        url = row.get("article_url")
        if not isinstance(url, str) or not url:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(row)

    print(f"[sync_source_articles] Upserting {len(deduped)} rows into public.source_articles…")

    try:
        rsp = (
            _client.table("source_articles")
            .upsert(deduped, on_conflict="article_url")
            .execute()
        )
    except Exception as e:  # pragma: no cover - runtime failure path
        print(f"[sync_source_articles] Error upserting into Supabase: {e!r}")
        raise SystemExit(1)

    inserted = len(getattr(rsp, "data", []) or [])
    print(f"[sync_source_articles] Upsert completed. Supabase returned {inserted} rows.")


if __name__ == "__main__":
    main()
