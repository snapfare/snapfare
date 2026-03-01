#!/usr/bin/env python
"""Generate a full newsletter HTML using deals already stored in Supabase.

Usage (from project root):

    python -m backend.scripts.generate_newsletter_from_deals_all \
        --limit 20 --output snippets/newsletter_from_db.html

This script:
- Loads environment variables from the project .env
- Reads top-scored deals from the `deals` table in Supabase
- Optionally filters by origin using ORIGIN_IATA_FILTER (e.g. ZRH,BSL)
- Converts each deal into a newsletter-style <tr>...</tr> block
- Uses the existing newsletter template (build_full_html) to wrap rows
- Writes a complete HTML document ready to send via an ESP
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
from scoring.html_output import build_full_html  # noqa: E402
from scripts.generate_newsletter_html import _deal_to_row_html  # noqa: E402


def _http_status(url: str, timeout: int = 8) -> Optional[int]:
    """Check URL liveness via HEAD with GET fallback.

    Returns the final HTTP status code, or None on network errors.
    """

    try:
        resp = requests.head(url, allow_redirects=True, timeout=timeout)
        if resp.status_code >= 400 or resp.status_code == 405:
            resp = requests.get(url, allow_redirects=True, timeout=timeout)
        return resp.status_code
    except Exception:
        return None


def _mark_source_article_dead(url: str) -> None:
    """Mark matching source_articles rows as status='dead' (best-effort)."""

    if not _client:
        return
    try:
        (
            _client.table("source_articles")
            .update({"status": "dead"})
            .eq("article_url", url)
            .execute()
        )
    except Exception:
        return


def _filter_alive_deals(deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only deals whose primary URL responds without a hard error.

    The URL used is the same as in the newsletter CTA: booking_url or link.
    Dead URLs are skipped and corresponding source_articles rows (if any)
    are marked as status='dead'.
    """

    filtered: List[Dict[str, Any]] = []
    for d in deals:
        if not isinstance(d, dict):
            continue

        primary_url = str(d.get("booking_url") or d.get("link") or "").strip()
        if not primary_url or primary_url == "#":
            filtered.append(d)
            continue

        status = _http_status(primary_url)
        if status is None or status >= 400:
            print(
                f"[generate_newsletter_from_deals_all] Skipping dead URL (status={status}): {primary_url}",
            )
            _mark_source_article_dead(primary_url)
            continue

        filtered.append(d)

    return filtered


def _get_origin_filter() -> List[str] | None:
    """Parse ORIGIN_IATA_FILTER env var into a list of IATA codes.

    Example: "ZRH,BSL,GVA" -> ["ZRH", "BSL", "GVA"].
    Returns None if no filter is configured.
    """

    raw = os.getenv("ORIGIN_IATA_FILTER", "").strip()
    if not raw:
        return None
    parts = [p.strip().upper() for p in raw.split(",") if p.strip()]
    return parts or None


def _fetch_deals_from_db(limit: int) -> List[Dict[str, Any]]:
    """Fetch top-scored deals from deals in Supabase.

    Orders by `scoring` descending and applies origin filter when configured.
    """

    if not _client:
        print("[generate_newsletter_from_deals_all] Supabase client not configured (missing SUPABASE_URL/KEY).")
        return []

    origins = _get_origin_filter()

    try:
        query = _client.table("deals").select("*")
        if origins:
            query = query.in_("origin_iata", origins)
        query = query.order("scoring", desc=True).limit(limit)
        rsp = query.execute()
        rows = getattr(rsp, "data", []) or []
        print(
            f"[generate_newsletter_from_deals_all] Retrieved {len(rows)} rows from deals "
            f"(limit={limit}, origin_filter={origins})",
        )
        return rows
    except Exception as e:  # pragma: no cover - defensive
        print("[generate_newsletter_from_deals_all] Error fetching from Supabase:", repr(e))
        return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate full newsletter HTML from deals already stored in Supabase (deals).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("DEALS_DEFAULT_LIMIT", "20")),
        help="Max number of deals to fetch and render.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=os.getenv("DEALS_NEWSLETTER_DB_OUTPUT", "snippets/newsletter_from_db.html"),
        help="Relative path (from backend/) where the full newsletter HTML will be written.",
    )

    args = parser.parse_args()

    print(
        f"[generate_newsletter_from_deals_all] Building newsletter from deals "
        f"with limit={args.limit}...",
    )

    deals = _fetch_deals_from_db(limit=args.limit)
    if not deals:
        print("[generate_newsletter_from_deals_all] No deals found in deals; writing empty newsletter body.")

    # Filter out deals whose CTA URL is no longer reachable; mark their
    # source_articles as dead so they are not reused in future runs.
    alive_deals = _filter_alive_deals(deals[: args.limit])

    rows_html: List[Dict[str, Any]] = []
    for d in alive_deals:
        try:
            rows_html.append(_deal_to_row_html(d))
        except Exception as e:  # best-effort, skip bad rows
            print("[generate_newsletter_from_deals_all] Skipping deal due to error:", repr(e))

    full_html = build_full_html(rows_html)

    output_rel = args.output
    output_path = os.path.join(BACKEND_ROOT, output_rel)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    print(f"[generate_newsletter_from_deals_all] Wrote newsletter HTML to: {output_path}")
    print(f"[generate_newsletter_from_deals_all] Deals count used: {len(rows_html)}")


if __name__ == "__main__":  # pragma: no cover
    main()
