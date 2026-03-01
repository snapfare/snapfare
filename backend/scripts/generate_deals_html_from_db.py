#!/usr/bin/env python
"""Generate an HTML snippet of deals already stored in Supabase.

Usage (from project root):

    python -m backend.scripts.generate_deals_html_from_db \
        --table deals_traveldealz --limit 20 \
        --output snippets/deals_from_db.html

This script:
- Loads environment variables from the project .env
- Reads recent deals from a Supabase table (deals_traveldealz / deals / ...)
- Maps them to the same structure used by the pipeline snippet renderer
- Writes a self-contained HTML snippet with deal cards
"""

from __future__ import annotations

import argparse
import json
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

from database.supabase_db import get_deals as get_deals_from_db, _client  # type: ignore  # noqa: E402
from services.deals_pipeline import render_html_snippet  # noqa: E402


def _parse_json_dict(val: Any) -> Dict[str, Any]:
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return {}
        if not (s.startswith("{") and s.endswith("}")):
            return {}
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


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
        # Non-critical path: failures here should not break snippet generation.
        return


def _filter_alive_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return only rows whose primary URL responds without a hard error.

    If a URL is detected as dead (status is None or >= 400), the row is
    skipped and any matching source_articles entry is marked as dead.
    """

    filtered: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        primary_url = str(row.get("link") or row.get("booking_url") or "").strip()
        if not primary_url or primary_url == "#":
            filtered.append(row)
            continue

        status = _http_status(primary_url)
        if status is None or status >= 400:
            print(
                f"[generate_deals_html_from_db] Skipping dead URL (status={status}): {primary_url}",
            )
            _mark_source_article_dead(primary_url)
            continue

        filtered.append(row)

    return filtered


def _rows_to_deals(rows: List[Dict[str, Any]], table: str) -> List[Dict[str, Any]]:
    """Map Supabase rows to the lightweight deal dict used by render_html_snippet.

    Expected keys per row (when available):
    - title, price, currency, link, image, source, scoring
    """

    deals: List[Dict[str, Any]] = []
    default_source = "Travel-Dealz" if "traveldealz" in table else (
        "SecretFlying" if "secretflying" in table else "supabase"
    )

    for row in rows:
        if not isinstance(row, dict):
            continue
        title = row.get("title") or "Untitled deal"
        price = row.get("price")
        currency = row.get("currency") or "EUR"
        link = row.get("link") or row.get("booking_url") or "#"
        source = row.get("source") or default_source
        scoring_raw = row.get("scoring")
        score = None
        if scoring_raw is not None:
            try:
                score = float(scoring_raw)
            except Exception:
                score = None

        deal: Dict[str, Any] = {
            "title": title,
            "price": price,
            "currency": currency,
            "link": link,
            "source": source,
            "score": score,
            # Optional extra fields (not currently used by render_html_snippet
            # but kept for future extensions):
            "image_url": row.get("image"),
            "origin": row.get("origin"),
            "destination": row.get("destination"),
            "miles": row.get("miles") or row.get("miles_estimate"),
            "cabin_class": row.get("cabin_class"),
            "flight": row.get("flight"),
            "llm_enriched_fields": _parse_json_dict(row.get("llm_enriched_fields")),
            # Campos para mostrar duración y equipaje en el snippet
            "flight_duration_minutes": row.get("flight_duration_minutes"),
            "flight_duration_display": row.get("flight_duration_display"),
            "baggage_allowance_display": row.get("baggage_allowance_display")
            or row.get("cabin_baggage")
            or row.get("baggage_summary"),
        }
        deals.append(deal)

    return deals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate HTML snippet from deals already stored in Supabase.",
    )
    parser.add_argument(
        "--input-json",
        type=str,
        default=None,
        help=(
            "Optional: render from a local JSON file (list of deal rows) instead of Supabase. "
            "Useful for --no-persist previews."
        ),
    )
    parser.add_argument(
        "--table",
        type=str,
        default="deals_traveldealz",
        help=(
            "Supabase table to read deals from (e.g. deals_traveldealz, "
            "deals, deals_secretflying)."
        ),
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
        default=os.getenv("DEALS_HTML_DB_OUTPUT", "snippets/deals_from_db.html"),
        help="Relative path (from backend/) where the HTML snippet will be written.",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Optional: only include rows whose row['source'] matches this value (case-insensitive).",
    )

    args = parser.parse_args()

    if args.input_json:
        print(
            f"[generate_deals_html_from_db] Building HTML snippet from JSON={args.input_json} "
            f"with limit={args.limit}...",
        )
        try:
            with open(args.input_json, "r", encoding="utf-8") as f:
                data = json.load(f)
            rows = data if isinstance(data, list) else []
        except Exception as e:
            print("[generate_deals_html_from_db] Error reading --input-json:", e)
            rows = []
    else:
        print(
            f"[generate_deals_html_from_db] Building HTML snippet from table={args.table} "
            f"with limit={args.limit}...",
        )

        db_result = get_deals_from_db(args.table, limit=args.limit)
        if db_result.get("status") != "ok":
            print("[generate_deals_html_from_db] Error fetching deals:", db_result)
            rows = []
        else:
            rows = db_result.get("deals") or []

    # Optional source filter (e.g., only amadeus rows)
    if args.source:
        want = str(args.source).strip().lower()
        rows = [
            r
            for r in rows
            if isinstance(r, dict) and str(r.get("source") or "").strip().lower() == want
        ]

    # Before mapping to lightweight deals, check that URLs are still alive.
    # Dead URLs are skipped and their source_articles (if any) are marked as
    # status='dead' so future jobs can avoid them.
    alive_rows = _filter_alive_rows(rows[: args.limit])

    deals = _rows_to_deals(alive_rows, args.table)
    if not deals:
        print("[generate_deals_html_from_db] No deals found; writing empty snippet.")

    html = render_html_snippet(deals, max_items=args.limit)

    output_rel = args.output
    output_path = os.path.join(BACKEND_ROOT, output_rel)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[generate_deals_html_from_db] Wrote HTML snippet to: {output_path}")
    print(f"[generate_deals_html_from_db] Deals count used: {len(deals)}")


if __name__ == "__main__":  # pragma: no cover
    main()
