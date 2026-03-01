#!/usr/bin/env python
"""Generate an HTML snippet of scored flight deals.

Usage (from project root):

    python -m backend.scripts.generate_deals_html --limit 30 --output snippets/deals.html

The script will:
- Load environment variables from the project .env
- Run the deals pipeline (scraping + scoring, optional persist)
- Write a self-contained HTML snippet with deal cards
"""

import argparse
import os
import sys

from dotenv import load_dotenv, find_dotenv

# Ensure `backend/` is on sys.path
THIS_DIR = os.path.dirname(__file__)
BACKEND_ROOT = os.path.dirname(THIS_DIR)
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

# Load env vars (Supabase, etc.) from project root .env
load_dotenv(find_dotenv())

from services.deals_pipeline import run_deals_pipeline  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate HTML snippet with scored flight deals.")
    parser.add_argument("--limit", type=int, default=int(os.getenv("DEALS_DEFAULT_LIMIT", "30")), help="Max number of deals to fetch and render.")
    parser.add_argument("--persist", action="store_true", help="Persist scraped deals into Supabase.")
    parser.add_argument("--no-persist", action="store_true", help="Force disabling persistence even if enabled by default.")
    parser.add_argument(
        "--output",
        type=str,
        default=os.getenv("DEALS_HTML_OUTPUT", "snippets/deals.html"),
        help="Relative path (from backend/) where the HTML snippet will be written.",
    )

    args = parser.parse_args()

    # Decide persistence flag
    persist_default = os.getenv("DEALS_PERSIST_DEFAULT", "true").strip().lower() in {"1", "true", "yes", "on"}
    if args.persist:
        persist = True
    elif args.no_persist:
        persist = False
    else:
        persist = persist_default

    print(f"[generate_deals_html] Running pipeline with limit={args.limit}, persist={persist}...")
    result = run_deals_pipeline(limit=args.limit, persist=persist, max_items_html=args.limit)

    html = result.get("html_snippet") or "<!-- No deals available -->"

    # Compute output path under backend/
    output_rel = args.output
    output_path = os.path.join(BACKEND_ROOT, output_rel)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[generate_deals_html] Wrote HTML snippet to: {output_path}")
    print(f"[generate_deals_html] Deals count: {result.get('count')} | Sources: {result.get('sources_enabled')}")


if __name__ == "__main__":
    main()
