#!/usr/bin/env python
"""Generate a full newsletter HTML using the newsletter template.

Usage (from project root):

    python -m backend.scripts.generate_newsletter_html \
        --limit 20 --output snippets/newsletter.html

This script:
- Loads environment variables from the project .env
- Runs the deals pipeline (scraping + scoring, optional persist)
- Converts each deal into a newsletter-style <tr>...</tr> block
- Uses the existing newsletter template (build_full_html) to wrap rows
- Writes a complete HTML document ready to send via an ESP
"""

from __future__ import annotations

import argparse
import os
import sys
from html import escape
from typing import Any, Dict, List

from dotenv import load_dotenv, find_dotenv

# Ensure `backend/` is on sys.path
THIS_DIR = os.path.dirname(__file__)
BACKEND_ROOT = os.path.dirname(THIS_DIR)
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

# Load env vars (Supabase, etc.) from project root .env
load_dotenv(find_dotenv())

from services.deals_pipeline import run_deals_pipeline  # noqa: E402
from scoring.html_output import build_full_html, deal_to_newsletter_row  # noqa: E402


def _deal_to_row_html(deal: Dict[str, Any]) -> str:
    """Thin wrapper around deal_to_newsletter_row for backwards compat."""
    return deal_to_newsletter_row(deal)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate full newsletter HTML with scored flight deals.")
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("DEALS_DEFAULT_LIMIT", "20")),
        help="Max number of deals to fetch and render.",
    )
    parser.add_argument("--persist", action="store_true", help="Persist scraped deals into Supabase.")
    parser.add_argument("--no-persist", action="store_true", help="Force disabling persistence even if enabled by default.")
    parser.add_argument(
        "--output",
        type=str,
        default=os.getenv("DEALS_NEWSLETTER_OUTPUT", "snippets/newsletter.html"),
        help="Relative path (from backend/) where the full newsletter HTML will be written.",
    )

    args = parser.parse_args()

    persist_default = os.getenv("DEALS_PERSIST_DEFAULT", "true").strip().lower() in {"1", "true", "yes", "on"}
    if args.persist:
        persist = True
    elif args.no_persist:
        persist = False
    else:
        persist = persist_default

    print(f"[generate_newsletter_html] Running pipeline with limit={args.limit}, persist={persist}...")
    result = run_deals_pipeline(limit=args.limit, persist=persist, max_items_html=args.limit)

    deals: List[Dict[str, Any]] = result.get("deals") or []
    if not deals:
        print("[generate_newsletter_html] No deals returned; writing empty newsletter body.")

    rows_html: List[str] = []
    for d in deals[: args.limit]:
        try:
            rows_html.append(_deal_to_row_html(d))
        except Exception as e:  # best-effort, skip bad rows
            print("[generate_newsletter_html] Skipping deal due to error:", repr(e))

    full_html = build_full_html(rows_html)

    output_rel = args.output
    output_path = os.path.join(BACKEND_ROOT, output_rel)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    print(f"[generate_newsletter_html] Wrote newsletter HTML to: {output_path}")
    print(f"[generate_newsletter_html] Deals count used: {len(rows_html)} | Sources: {result.get('sources_enabled')}")


if __name__ == "__main__":  # pragma: no cover
    main()
