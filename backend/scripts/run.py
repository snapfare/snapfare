#!/usr/bin/env python
"""Unified launcher for snapcore server and jobs.

Examples (from the repo root):

  # Start FastAPI server with uvicorn
  python -m backend.scripts.run server

  # Run the deals pipeline in "swiss_newsletter" mode
  python -m backend.scripts.run pipeline --mode swiss_newsletter

"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

# Ensure subprocess stdout/stderr handles Unicode on Windows (e.g. → in route strings)
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable

from dotenv import load_dotenv, find_dotenv

# Ensure correct paths
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Load .env from the root
load_dotenv(find_dotenv())

# Supabase client (for optional cleanup/cascade operations)
try:  # Lazy-safe import: if Supabase is unavailable, _client will be None
    from database.supabase_db import _client  # type: ignore
except Exception:  # pragma: no cover - environment without Supabase
    _client = None


def _load_run_config() -> Dict[str, Any]:
    cfg_path = REPO_ROOT / "run_config.json"
    if not cfg_path.exists():
        raise SystemExit(f"run_config.json not found at {cfg_path}")
    # On Windows it's common for UTF-8 files to be saved with BOM.
    # json.load() will fail unless we strip it, so use utf-8-sig.
    with cfg_path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _get_mode(cfg: Dict[str, Any], mode: str) -> Dict[str, Any]:
    modes = cfg.get("modes") or {}
    if mode not in modes:
        available = ", ".join(sorted(modes.keys()))
        raise SystemExit(f"Mode '{mode}' not defined in run_config.json. Available modes: {available}")
    return modes[mode]


def _parse_duffel_origins(value: Any) -> list[str]:
    """Parse a single, list, or comma-separated list of IATA origins.

    Accepted formats:
    - "ZRH" -> ["ZRH"]
    - "ZRH,BSL" -> ["ZRH", "BSL"]
    - ["ZRH", "BSL"] -> ["ZRH", "BSL"]
    - None / "" -> ["ZRH"] (default)
    """

    if value is None:
        return ["ZRH"]

    # If it's already a list/tuple/set, normalize items
    if isinstance(value, (list, tuple, set)):
        parts = [str(v).strip().upper() for v in value if str(v).strip()]
        return parts or ["ZRH"]

    raw = str(value).strip()
    if not raw:
        return ["ZRH"]

    parts = [p.strip().upper() for p in raw.split(",") if p.strip()]
    return parts or ["ZRH"]


def cmd_server(args: argparse.Namespace) -> None:
    """Start uvicorn on backend.app:app."""

    # Optional: apply a run_config mode before starting the server.
    # This allows `default_command=server` with `default_mode=...` and
    # makes `backend.app` read the right env vars at import time.
    mode_name = getattr(args, "mode", None)
    if mode_name:
        try:
            cfg = _load_run_config()
            mode_cfg = _get_mode(cfg, str(mode_name))
            _apply_mode_env(mode_cfg)
            print(f"[run] Server mode applied: {mode_name}")
        except SystemExit as e:
            raise
        except Exception as e:
            raise SystemExit(f"Failed to apply server mode '{mode_name}': {e!r}")

    try:
        import uvicorn  # type: ignore
    except Exception:
        raise SystemExit(
            "uvicorn is not installed in the current environment. "
            "Install 'uvicorn[standard]' or add uvicorn to requirements.txt."
        )

    host = args.host or "127.0.0.1"
    port = int(args.port or 8000)
    reload_flag = bool(args.reload)

    uvicorn.run("backend.app:app", host=host, port=port, reload=reload_flag)


def _apply_mode_env(mode_cfg: Dict[str, Any]) -> None:
    """Apply environment variables derived from the selected mode."""

    # Compatibility: some run_config.json (milestone) use keys like:
    # - sources (list) instead of scraping_sources
    # - origin instead of origin_filter
    # - limit instead of pipeline_limit
    # - done_limit instead of source_articles_done_limit
    # - scrape.traveldealz/secretflying instead of scraping_limit_*
    # - llm.enrich/enrich_max/validate_* instead of enrich_default/enrich_max_items/llm_validate_*
    def _mode_get(*keys: str, default: Any = None) -> Any:
        for k in keys:
            if k in mode_cfg:
                return mode_cfg.get(k)
        return default

    def _mode_get_nested(path: str, default: Any = None) -> Any:
        cur: Any = mode_cfg
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur.get(part)
        return cur

    # Source activation no longer depends on SCRAPING_URL, but on the
    # per-source limits (scraping_limit_*, duffel_max_calls). We keep
    # SCRAPING_URL solely for backward compatibility if still defined in the mode.
    sources = _mode_get("scraping_sources", "sources")
    if sources:
        os.environ["SCRAPING_URL"] = ",".join(sources)
    else:
        os.environ.pop("SCRAPING_URL", None)

    origin_filter = _mode_get("origin_filter", "origin")
    if origin_filter:
        os.environ["ORIGIN_IATA_FILTER"] = str(origin_filter)
    else:
        # For modes without an explicit filter, remove any previous value
        # inherited from the environment (e.g. ORIGIN_IATA_FILTER in .env).
        os.environ.pop("ORIGIN_IATA_FILTER", None)

    # Allow setting the source_articles.done load limit from the mode
    # (useful for disabling the duplicate filter in test modes).
    if "source_articles_done_limit" in mode_cfg or "done_limit" in mode_cfg:
        os.environ["SOURCE_ARTICLES_DONE_LIMIT"] = str(_mode_get("source_articles_done_limit", "done_limit") or 0)

    # Optional per-source scraping limits (max number of deals to
    # attempt to collect from Travel-Dealz and SecretFlying). If not
    # defined in the mode, they are removed from the environment to
    # avoid carrying over values from previous runs.
    td_limit = _mode_get("scraping_limit_travel_dealz")
    if td_limit is None:
        td_limit = _mode_get_nested("scrape.traveldealz")
    if td_limit is not None:
        os.environ["SCRAPING_LIMIT_TRAVEL_DEALZ"] = str(td_limit)
    else:
        os.environ.pop("SCRAPING_LIMIT_TRAVEL_DEALZ", None)

    # Optional: choose which Travel-Dealz domain to scrape.
    # Values: "de" | "com" | "both" (default).
    td_domain = _mode_get("scraping_travel_dealz_domain")
    if td_domain is None:
        td_domain = _mode_get_nested("scrape.traveldealz_domain")
    if td_domain is not None:
        os.environ["SCRAPING_TRAVEL_DEALZ_DOMAIN"] = str(td_domain)
    else:
        os.environ.pop("SCRAPING_TRAVEL_DEALZ_DOMAIN", None)

    # Optional: tuning for "done" filtering. These control how many listing
    # candidates we fetch to find N new deals.
    td_overfetch_min = _mode_get("scraping_overfetch_travel_dealz_min")
    if td_overfetch_min is None:
        td_overfetch_min = _mode_get_nested("scrape.overfetch_min")
    if td_overfetch_min is not None:
        os.environ["SCRAPING_OVERFETCH_TRAVEL_DEALZ_MIN"] = str(td_overfetch_min)
    else:
        os.environ.pop("SCRAPING_OVERFETCH_TRAVEL_DEALZ_MIN", None)

    td_overfetch_max = _mode_get("scraping_overfetch_travel_dealz_max")
    if td_overfetch_max is None:
        td_overfetch_max = _mode_get_nested("scrape.overfetch_max")
    if td_overfetch_max is not None:
        os.environ["SCRAPING_OVERFETCH_TRAVEL_DEALZ_MAX"] = str(td_overfetch_max)
    else:
        os.environ.pop("SCRAPING_OVERFETCH_TRAVEL_DEALZ_MAX", None)

    sf_limit = _mode_get("scraping_limit_secretflying")
    if sf_limit is None:
        sf_limit = _mode_get_nested("scrape.secretflying")
    if sf_limit is not None:
        os.environ["SCRAPING_LIMIT_SECRETFLYING"] = str(sf_limit)
    else:
        os.environ.pop("SCRAPING_LIMIT_SECRETFLYING", None)

    # -------------------------
    # Deterministic (no-LLM) fills
    # -------------------------
    # Some steps in the pipeline can fill/guess missing fields locally (e.g., miles estimation)
    # even when llm.action=off. For truly raw scraping runs, allow disabling these.
    det_action_raw = _mode_get_nested("deterministic.action")
    det_action = str(det_action_raw).strip().lower() if det_action_raw is not None else None
    if det_action is not None:
        if det_action in {"0", "false", "none", "off", "disabled"}:
            det_action = "off"
        elif det_action in {"1", "true", "on", "enabled"}:
            det_action = "on"

    if det_action == "off":
        os.environ["DEALS_DETERMINISTIC_ENRICH"] = "false"
    elif det_action == "on":
        os.environ["DEALS_DETERMINISTIC_ENRICH"] = "true"
    else:
        # Not specified in mode: don't carry over from the parent shell.
        os.environ.pop("DEALS_DETERMINISTIC_ENRICH", None)

    # OpenAI client throttling / retry knobs (optional per-mode)
    # These map directly to the env vars used by services.deals_enrichment.
    openai_min_s = _mode_get_nested("openai.min_seconds_between_calls")
    if openai_min_s in (None, ""):
        os.environ.pop("OPENAI_MIN_SECONDS_BETWEEN_CALLS", None)
    else:
        os.environ["OPENAI_MIN_SECONDS_BETWEEN_CALLS"] = str(openai_min_s).strip()

    openai_max_retries = _mode_get_nested("openai.max_retries")
    if openai_max_retries in (None, ""):
        os.environ.pop("OPENAI_MAX_RETRIES", None)
    else:
        os.environ["OPENAI_MAX_RETRIES"] = str(openai_max_retries).strip()

    openai_backoff = _mode_get_nested("openai.retry_backoff_seconds")
    if openai_backoff in (None, ""):
        os.environ.pop("OPENAI_RETRY_BACKOFF_SECONDS", None)
    else:
        os.environ["OPENAI_RETRY_BACKOFF_SECONDS"] = str(openai_backoff).strip()

    # HTML display settings (currency)
    html_display_currency = _mode_get_nested("html.display_currency")
    if html_display_currency in (None, ""):
        # Backward compat: allow top-level display_currency
        html_display_currency = _mode_get("display_currency")
    if html_display_currency in (None, ""):
        os.environ.pop("DISPLAY_CURRENCY", None)
        os.environ.pop("FX_EUR_TO_CHF", None)
    else:
        os.environ["DISPLAY_CURRENCY"] = str(html_display_currency).strip().upper()

        # Optional FX rate for conversions used by newsletter/snippet rendering.
        # Example: html.fx_eur_to_chf: 0.95
        fx = _mode_get_nested("html.fx_eur_to_chf")
        if fx in (None, ""):
            os.environ.pop("FX_EUR_TO_CHF", None)
        else:
            os.environ["FX_EUR_TO_CHF"] = str(fx).strip()


def _setup_logging() -> None:
    """Configure file+console logging for the CLI runner."""
    logger = logging.getLogger()
    if logger.handlers:
        return

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)
    logger.setLevel(level)

    logs_dir = REPO_ROOT / "logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_path = logs_dir / "snapcore.log"

    file_handler = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)



def cmd_pipeline(args: argparse.Namespace) -> None:
    """Run the deals pipeline for a specific mode."""

    from services.deals_pipeline import run_deals_pipeline

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter"
    mode_cfg = _get_mode(cfg, mode_name)

    _apply_mode_env(mode_cfg)

    if getattr(args, "no_persist", False):
        persist = False
    else:
        persist = bool(mode_cfg.get("persist", True))

    # If the mode declares 'duffel' in scraping_sources, fill Duffel
    # gaps first according to patterns.json and the duffel_* parameters
    # of the mode itself. This way, the user only needs to add "duffel"
    # to the sources list in run_config.
    _run_duffel_for_mode_if_enabled(mode_name, mode_cfg, persist=persist)

    limit = int(args.limit or mode_cfg.get("pipeline_limit") or mode_cfg.get("limit") or 40)

    # Allow overriding sources from CLI (travel-dealz,secretflying).
    sources = None
    if getattr(args, "sources", None):
        raw_sources = [s.strip().lower() for s in str(args.sources).split(",") if s.strip()]
        mapped: set[str] = set()
        for s in raw_sources:
            if "travel-dealz" in s:
                mapped.add("travel-dealz")
            if "secretflying" in s:
                mapped.add("secretflying")
        if mapped:
            sources = mapped

    print(f"[run] Running pipeline in mode='{mode_name}' with limit={limit}...")
    if sources:
        print(f"[run] Sources overridden from CLI: {sorted(sources)}")

    result = run_deals_pipeline(
        limit=limit,
        persist=persist,
        max_items_html=limit,
        sources=sources,
    )

    print(f"[run] Deals generated: {result.get('count')} | sources: {result.get('sources_enabled')}")
    deals = (result.get("deals") or [])[:5]
    for d in deals:
        print(f" - {d.get('score')} | {d.get('price')} {d.get('currency')} | {d.get('title')}")


def cmd_duffel_refresh(args: argparse.Namespace) -> None:
    """Fill Duffel gaps based on patterns.json according to the mode."""

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter"
    mode_cfg = _get_mode(cfg, mode_name)

    duffel_cfg = mode_cfg.get("duffel") if isinstance(mode_cfg.get("duffel"), dict) else {}

    raw_origin = args.origin or mode_cfg.get("duffel_origin") or duffel_cfg.get("origins") or "ZRH"
    origins = _parse_duffel_origins(raw_origin)
    months_ahead = args.months_ahead or mode_cfg.get("duffel_months_ahead") or duffel_cfg.get("months") or 4
    max_calls = args.max_calls or mode_cfg.get("duffel_max_calls") or duffel_cfg.get("calls") or 72

    for origin in origins:
        cmd = [
            sys.executable,
            "-m",
            "backend.scripts.fill_duffel_gaps_from_patterns",
            "--origin",
            str(origin),
            "--months-ahead",
            str(months_ahead),
            "--max-calls",
            str(max_calls),
        ]

        print("[run] Running:", " ".join(cmd))
        subprocess.run(cmd, check=False)


def cmd_html_snippet(args: argparse.Namespace) -> None:
    """Generate an HTML deals snippet using the selected mode.

    This command first applies the mode configuration (scrapers,
    ORIGIN_IATA_FILTER, etc.) and then delegates to
    `backend.scripts.generate_deals_html` to write the snippet to
    disk.
    """

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter"
    mode_cfg = _get_mode(cfg, mode_name)

    _apply_mode_env(mode_cfg)

    limit = int(args.limit or mode_cfg.get("pipeline_limit") or 40)

    cmd = [
        sys.executable,
        "-m",
        "backend.scripts.generate_deals_html",
        "--limit",
        str(limit),
    ]

    if args.output:
        cmd.extend(["--output", str(args.output)])

    if args.persist:
        cmd.append("--persist")
    if args.no_persist:
        cmd.append("--no-persist")

    print("[run] Running:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_newsletter_html(args: argparse.Namespace) -> None:
    """Generate the complete newsletter HTML for a mode.

    First applies the selected mode configuration and then delegates
    to `backend.scripts.generate_newsletter_html` to generate the
    complete HTML based on the newsletter template.
    """

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter"
    mode_cfg = _get_mode(cfg, mode_name)

    _apply_mode_env(mode_cfg)

    limit = int(args.limit or mode_cfg.get("pipeline_limit") or 40)

    cmd = [
        sys.executable,
        "-m",
        "backend.scripts.generate_newsletter_html",
        "--limit",
        str(limit),
    ]

    if args.output:
        cmd.extend(["--output", str(args.output)])

    if args.persist:
        cmd.append("--persist")
    if args.no_persist:
        cmd.append("--no-persist")

    print("[run] Running:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_cleanup_deal(args: argparse.Namespace) -> None:
    """Wrapper to clean up a "dead" deal (URL that no longer exists).

    Delegates to backend.scripts.cleanup_dead_deal to delete the
    corresponding rows in deals_* and source_articles.
    """

    cmd = [
        sys.executable,
        "-m",
        "backend.scripts.cleanup_dead_deal",
        "--url",
        str(args.url),
    ]

    if args.no_http_check:
        cmd.append("--no-http-check")
    if args.clean:
        cmd.extend(["--clean", str(args.clean)])

    print("[run] Running:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_html_from_db(args: argparse.Namespace) -> None:
    """Generate HTML snippet directly from Supabase (deals table).

    This is useful when live scraping returns 0 deals but you already
    have historical data in the aggregated deals table.
    """

    cmd = [
        sys.executable,
        "-m",
        "backend.scripts.generate_deals_html_from_db",
        "--table",
        str(args.table),
        "--limit",
        str(args.limit),
        "--output",
        str(args.output),
    ]

    print("[run] Running:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_duffel_html(args: argparse.Namespace) -> None:
        """Launch Duffel (according to the mode) and generate a snippet from deals.

        Flow:
        - Read mode configuration (duffel_origin, duffel_months_ahead,
            duffel_max_calls).
        - Run fill_duffel_gaps_from_patterns to fill deals with
            Duffel rows (source="duffel").
        - Generate an HTML snippet from the deals table using
            generate_deals_html_from_db.
        """

        cfg = _load_run_config()
        mode_name = args.mode or "duffel_test"
        mode_cfg = _get_mode(cfg, mode_name)

        _apply_mode_env(mode_cfg)

        # Persist control (mode default can be overridden)
        if getattr(args, "persist", False):
            persist = True
        elif getattr(args, "no_persist", False):
            persist = False
        else:
            persist = bool(mode_cfg.get("persist", True))

        raw_origin = args.origin or mode_cfg.get("duffel_origin") or "ZRH"
        origins = _parse_duffel_origins(raw_origin)
        months_ahead = args.months_ahead or mode_cfg.get("duffel_months_ahead") or 4
        max_calls = args.max_calls or mode_cfg.get("duffel_max_calls") or 10

        # 1) Fill deals with Duffel rows for each origin
        # In no-persist mode we still want a preview snippet, so we ask the
        # script to dump computed rows to JSON.
        safe_mode = "".join(ch if ch.isalnum() else "_" for ch in str(mode_name))
        preview_json = f"backend/snippets/duffel_preview_{safe_mode}.json"
        for origin in origins:
            cmd_fill = [
                sys.executable,
                "-m",
                "backend.scripts.fill_duffel_gaps_from_patterns",
                "--origin",
                str(origin),
                "--months-ahead",
                str(months_ahead),
                "--max-calls",
                str(max_calls),
            ]

            if not persist:
                cmd_fill.append("--dry-run")
            # Always dump JSON so the subsequent HTML step can be deterministic.
            cmd_fill.extend(["--output-json", preview_json])

            print("[run] Running (Duffel fill):", " ".join(cmd_fill))
            subprocess.run(cmd_fill, check=False)

        # 2) Generate HTML snippet from deals
        limit = int(args.limit or mode_cfg.get("pipeline_limit") or 10)
        output_rel = args.output or "snippets/deal_duffel.html"

        cmd_snip = [
            sys.executable,
            "-m",
            "backend.scripts.generate_deals_html_from_db",
            "--limit",
            str(limit),
            "--output",
            output_rel,
        ]

        # Only show Duffel rows in this command.
        cmd_snip.extend(["--source", "duffel"])

        if not persist:
            cmd_snip.extend(["--input-json", preview_json])
        else:
            cmd_snip.extend(["--table", "deals"])

        print("[run] Running (Duffel snippet):", " ".join(cmd_snip))
        subprocess.run(cmd_snip, check=False)


def cmd_demo_html(args: argparse.Namespace) -> None:
    """Generate a minimal set of reference HTML files for quick visual QA.

    Outputs (under backend/snippets/demo/):
      - deal_traveldealz.html
      - newsletter_traveldealz.html
      - deal_duffel.html
      - newsletter_duffel.html

    This command is designed to avoid creating extra artifacts and to work
    without persisting anything to Supabase.
    """

    from services.deals_pipeline import run_deals_pipeline
    from scoring.html_output import deal_to_newsletter_row, build_full_html

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter_llm_3"
    mode_cfg = _get_mode(cfg, mode_name)
    _apply_mode_env(mode_cfg)

    # Ensure Duffel demo rows always show baggage (assumed) even if Duffel omits it.
    os.environ.setdefault("DUFFEL_ASSUME_BAGGAGE", "true")

    demo_dir = BACKEND_ROOT / "snippets" / "demo"
    demo_dir.mkdir(parents=True, exist_ok=True)

    td_limit = int(getattr(args, "traveldealz_limit", None) or 3)
    duffel_limit = int(getattr(args, "duffel_limit", None) or 3)
    max_calls = int(getattr(args, "max_calls", None) or 1)

    # 1) Travel-Dealz: run pipeline in-memory only, travel-dealz source only
    td_result = run_deals_pipeline(
        limit=td_limit,
        persist=False,
        max_items_html=min(td_limit, 10),
        sources={"travel-dealz"},
    )
    td_deals_all = td_result.get("deals") or []
    td_deals = [d for d in td_deals_all if isinstance(d, dict) and "travel" in str(d.get("source") or "").lower()]

    td_deal_path = demo_dir / "deal_traveldealz.html"
    td_news_path = demo_dir / "newsletter_traveldealz.html"
    if not td_deals and _client:
        # Fallback: use recent Travel-Dealz rows from Supabase so demo outputs
        # exist even when live scraping fails (timeouts, bans, etc.).
        try:
            rsp = (
                _client.table("deals")
                .select(
                    "title,price,currency,link,booking_url,image,source,origin,destination,origin_iata,destination_iata,airline,cabin_class,aircraft,miles,date_out,date_in,stops,baggage_included,baggage_pieces_included,baggage_allowance_kg,flight_duration_minutes,flight_duration_display,skyscanner_url,scoring"
                )
                .ilike("source", "%travel%dealz%")
                .order("id", desc=True)
                .limit(td_limit)
                .execute()
            )
            td_deals = getattr(rsp, "data", []) or []
        except Exception as e:
            td_deals = []

    if td_deals:
        td_rows = [deal_to_newsletter_row(d) for d in td_deals[: td_limit]]
        td_deal_path.write_text(td_rows[0], encoding="utf-8")
        td_news_path.write_text(build_full_html(td_rows), encoding="utf-8")
        print(f"[run] demo-html wrote: {td_deal_path} and {td_news_path} (n={len(td_rows)})")
    else:
        # Don't create placeholders; just remove stale demo outputs.
        try:
            if td_deal_path.exists():
                td_deal_path.unlink()
            if td_news_path.exists():
                td_news_path.unlink()
        except Exception:
            pass
        print("[run] demo-html: no Travel-Dealz deals produced (no files written)")

    # 2) Duffel: run gap filler in dry-run and render from the produced JSON
    raw_origin = args.origin or (mode_cfg.get("duffel_origin") or "ZRH")
    origins = _parse_duffel_origins(raw_origin)
    months_ahead = int(getattr(args, "months_ahead", None) or (mode_cfg.get("duffel_months_ahead") or 4))

    preview_json = str(demo_dir / "duffel_preview.json")
    for origin in origins[:1]:
        cmd_fill = [
            sys.executable,
            "-m",
            "backend.scripts.fill_duffel_gaps_from_patterns",
            "--origin",
            str(origin),
            "--months-ahead",
            str(months_ahead),
            "--max-calls",
            str(max_calls),
            "--dry-run",
            "--output-json",
            preview_json,
        ]
        print("[run] demo-html executing (Duffel dry-run):", " ".join(cmd_fill))
        subprocess.run(cmd_fill, check=False)
        break

    duffel_deal_path = demo_dir / "deal_duffel.html"
    duffel_news_path = demo_dir / "newsletter_duffel.html"
    try:
        import json as _json

        rows = _json.loads(Path(preview_json).read_text(encoding="utf-8"))
        rows = rows if isinstance(rows, list) else []
    except Exception:
        rows = []

    duffel_deals: list[dict[str, Any]] = [r for r in rows[: duffel_limit] if isinstance(r, dict)]

    if duffel_deals:
        duffel_rows = [deal_to_newsletter_row(d) for d in duffel_deals[: duffel_limit]]
        duffel_deal_path.write_text(duffel_rows[0], encoding="utf-8")
        duffel_news_path.write_text(build_full_html(duffel_rows), encoding="utf-8")
        print(f"[run] demo-html wrote: {duffel_deal_path} and {duffel_news_path} (n={len(duffel_rows)})")
    else:
        try:
            if duffel_deal_path.exists():
                duffel_deal_path.unlink()
            if duffel_news_path.exists():
                duffel_news_path.unlink()
        except Exception:
            pass
        print("[run] demo-html: no Duffel deals produced (no files written)")

    # Keep demo folder clean: remove intermediate JSON.
    try:
        pj = Path(preview_json)
        if pj.exists():
            pj.unlink()
    except Exception:
        pass


def cmd_scan_dead(args: argparse.Namespace) -> None:
    """Scan deals tables in Supabase for dead URLs.

    Convenient wrapper around backend.scripts.cleanup_dead_deals_auto in
    "manual review" mode: by default does NOT delete anything, only prints
    which URLs appear to be dead (status >= 400 or network error).
    """

    table = str(args.table)
    limit = int(args.limit)

    cmd = [
        sys.executable,
        "-m",
        "backend.scripts.cleanup_dead_deals_auto",
        "--table",
        table,
        "--limit",
        str(limit),
    ]

    # Only if --apply is passed explicitly are the actual deletions added.
    if args.apply:
        cmd.append("--apply")

    print("[run] Running dead URL scan:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def _cascade_delete_for_source(source_key: str) -> None:
    """Cascade-delete related rows in deals and source_articles.

    Intended for bulk resets by source (travel-dealz, secretflying, ...)
    when the specific table (deals_traveldealz, deals_secretflying) is emptied.
    """

    if not _client:
        print(f"[run] Supabase not configured; skipping cascade delete for source={source_key}.")
        return

    try:
        # 1) Delete rows in deals associated with this source.
        try:
            rsp = _client.table("deals").select("id,source").execute()
            rows = getattr(rsp, "data", []) or []
            ids = [
                r["id"]
                for r in rows
                if isinstance(r, dict)
                and "id" in r
                and source_key in str(r.get("source") or "").lower()
            ]
            if ids:
                _client.table("deals").delete().in_("id", ids).execute()
                print(
                    f"[run] Deleted {len(ids)} rows from deals for source~{source_key}.",
                )
        except Exception as e:
            print(f"[run] Error deleting from deals for source={source_key}: {e!r}")

        # 2) Delete rows in source_articles associated with this source.
        try:
            rsp_sa = _client.table("source_articles").select("id,source").execute()
            rows_sa = getattr(rsp_sa, "data", []) or []
            ids_sa = [
                r["id"]
                for r in rows_sa
                if isinstance(r, dict)
                and "id" in r
                and str(r.get("source") or "").lower() == source_key
            ]
            if ids_sa:
                _client.table("source_articles").delete().in_("id", ids_sa).execute()
                print(
                    f"[run] Deleted {len(ids_sa)} rows from source_articles for source={source_key}.",
                )
        except Exception as e:
            print(f"[run] Error deleting from source_articles for source={source_key}: {e!r}")
    except Exception as e:  # pragma: no cover - unexpected error
        print(f"[run] Unexpected error in cascade delete for source={source_key}: {e!r}")


def _run_duffel_for_mode_if_enabled(mode_name: str, mode_cfg: Dict[str, Any], persist: bool = True) -> Dict[str, Any]:
    """Run Duffel gap-filling for a mode if it is enabled.

    Activation is driven by Duffel-specific parameters (for example,
    duffel_max_calls > 0) instead of relying on scraping_sources.

    The ``persist`` flag controls whether the underlying script is allowed
    to write rows into Supabase. When ``persist`` is False, the script is
    executed in dry-run mode so that it only logs what it *would* save
    without performing any database writes. This is important for preview
    modes where we want to avoid touching Supabase at all.
    """

    # Support both legacy top-level keys (duffel_*) and nested config:
    #   "duffel": {"origins": ["ZRH"], "months": 4, "calls": 3}
    duffel_cfg = mode_cfg.get("duffel") if isinstance(mode_cfg.get("duffel"), dict) else {}

    max_calls_cfg = mode_cfg.get("duffel_max_calls")
    if max_calls_cfg is None:
        max_calls_cfg = duffel_cfg.get("calls")

    try:
        if max_calls_cfg is None or int(max_calls_cfg) <= 0:
            return {"ran": False}
    except Exception:
        return {"ran": False}

    raw_origin = mode_cfg.get("duffel_origin")
    if raw_origin in (None, ""):
        raw_origin = duffel_cfg.get("origins")
    raw_origin = raw_origin or "ZRH"
    origins = _parse_duffel_origins(raw_origin)

    months_ahead = mode_cfg.get("duffel_months_ahead")
    if months_ahead in (None, ""):
        months_ahead = duffel_cfg.get("months")
    months_ahead = months_ahead or 4

    try:
        remaining_calls = int(max_calls_cfg or 72)
    except Exception:
        remaining_calls = 72

    started = time.perf_counter()
    ran_any = False
    timeout_count = 0

    for origin in origins:
        if remaining_calls <= 0:
            break

        calls_for_origin = remaining_calls

        cmd = [
            sys.executable,
            "-m",
            "backend.scripts.fill_duffel_gaps_from_patterns",
            "--origin",
            str(origin),
            "--months-ahead",
            str(months_ahead),
            "--max-calls",
            str(calls_for_origin),
        ]

        # In preview modes (persist=False) we run the script in dry-run mode
        # so nothing is written to Supabase.
        if not persist:
            cmd.append("--dry-run")

        print(
            f"[run] Mode '{mode_name}' has Duffel enabled; "
            "running Duffel refresh before the pipeline:",
            " ".join(cmd),
        )
        # Avoid hanging runs when Duffel/network stalls.
        # Priority: per-mode run_config.json -> env var -> default.
        timeout_s: float
        timeout_raw = None
        if isinstance(mode_cfg, dict):
            timeout_raw = mode_cfg.get("timeout_duffel")

        if timeout_raw not in (None, ""):
            try:
                timeout_s = float(timeout_raw)
            except Exception:
                timeout_s = 120.0
        else:
            try:
                timeout_s = float(os.getenv("DUFFEL_REFRESH_TIMEOUT_SECONDS", "120"))
            except Exception:
                timeout_s = 120.0

        try:
            subprocess.run(cmd, check=False, timeout=timeout_s)
            ran_any = True
        except subprocess.TimeoutExpired:
            print(f"[run] Duffel refresh timed out after {timeout_s:.0f}s; continuing without waiting.")
            ran_any = True
            timeout_count += 1

        # Consume total budget (not per-origin). To distribute it, adjust
        # duffel_max_calls or the list of origins.
        remaining_calls -= calls_for_origin

    elapsed_s = time.perf_counter() - started
    return {
        "ran": bool(ran_any),
        "origins": origins,
        "months_ahead": months_ahead,
        "max_calls": max_calls_cfg,
        "timeout_s": timeout_s,
        "timeouts": timeout_count,
        "elapsed_s": elapsed_s,
    }


def cmd_deals_html(args: argparse.Namespace) -> None:
    """End-to-end run: scrape, persist and render HTML snippets.

    Flow:
    - Apply the selected mode (scrapers, filters, etc.).
    - Run the deals pipeline (scraping + scoring + optional enrichment).
    - Persist results to Supabase if enabled.
    - Write a main HTML snippet (top deals) to disk.
    - Optionally, if requested, fall back to Supabase when the pipeline
        returns 0 deals.
    - Additionally write a 1-deal snippet for quick visual inspection
        of the latest deal.
    """

    from services.deals_pipeline import run_deals_pipeline, render_html_snippet
    from scoring.html_output import deal_to_newsletter_row, build_full_html

    cfg = _load_run_config()
    mode_name = args.mode or cfg.get("default_mode") or "full-no-llm"
    mode_cfg = _get_mode(cfg, mode_name)

    _apply_mode_env(mode_cfg)

    if getattr(args, "no_persist", False):
        persist = False
    else:
        persist = bool(mode_cfg.get("persist", True))

    # Same as in cmd_pipeline: if the mode includes 'duffel' as a source,
    # we first run the Duffel refresh based on patterns.json so that
    # Duffel benchmarks/rows are ready before the pipeline.
    _run_duffel_for_mode_if_enabled(mode_name, mode_cfg, persist=persist)

    limit = int(args.limit or mode_cfg.get("pipeline_limit") or mode_cfg.get("limit") or 40)

    # Optional sources override
    sources = None
    if getattr(args, "sources", None):
        raw_sources = [s.strip().lower() for s in str(args.sources).split(",") if s.strip()]
        mapped: set[str] = set()
        for s in raw_sources:
            if "travel-dealz" in s:
                mapped.add("travel-dealz")
            if "secretflying" in s:
                mapped.add("secretflying")
        if mapped:
            sources = mapped

    # Optional reset of deals tables before running the pipeline.
    # Allows choosing the source to clear: travel-dealz, secretflying,
    # duffel, deals-all or all.
    reset_target = getattr(args, "reset_deals", None)
    if reset_target:
        if reset_target == "travel-dealz":
            tables = "deals"
            # In the current schema, travel-dealz is stored in the aggregated
            # deals table; we also delete the corresponding rows in
            # source_articles.
            _cascade_delete_for_source("travel-dealz")
        elif reset_target == "secretflying":
            tables = "deals"
            _cascade_delete_for_source("secretflying")
        elif reset_target == "duffel":
            tables = "deals_duffel"
        elif reset_target == "deals-all":
            tables = "deals"
        else:  # "all"
            tables = "deals,deals_duffel"
            _cascade_delete_for_source("travel-dealz")
            _cascade_delete_for_source("secretflying")

        reset_cmd = [
            sys.executable,
            "-m",
            "backend.scripts.truncate_deals_tables",
            "--tables",
            tables,
            "--force",
        ]
        print("[run] Running deals table reset:", " ".join(reset_cmd))
        subprocess.run(reset_cmd, check=False)

    # Optional cleanup of dead URLs before running the pipeline.
    clean_dead = getattr(args, "clean_dead", None)
    if clean_dead:
        # In the current schema all sources are stored in `deals`,
        # so we always use that table as the base for cleanup.
        table = "deals"

        clean_cmd = [
            sys.executable,
            "-m",
            "backend.scripts.cleanup_dead_deals_auto",
            "--table",
            table,
            "--limit",
            str(getattr(args, "clean_limit", 200)),
            "--apply",
        ]
        print("[run] Running dead URL cleanup:", " ".join(clean_cmd))
        subprocess.run(clean_cmd, check=False)

    print(f"[run] Running deals-html in mode='{mode_name}' with limit={limit}...")
    if sources:
        print(f"[run] Sources overridden from CLI: {sorted(sources)}")

    result = run_deals_pipeline(
        limit=limit,
        persist=persist,
        # Render at most 10 items in the main HTML snippet for readability
        max_items_html=min(limit, 10),
        sources=sources,
    )

    count = int(result.get("count") or 0)
    if persist:
        print(
            f"[run] Persist summary -> persisted={result.get('persisted')} "
            f"persisted_count={result.get('persisted_count')} "
            f"sources={result.get('sources_enabled')}"
        )
        if result.get("persist_info"):
            print(f"[run] persist_info: {result.get('persist_info')}")

    # Note: we no longer generate the legacy "deals.html" snippet or the
    # DB fallback here, because the example snippets are based on the
    # newsletter template.
    print(f"[run] Deals generated by pipeline: {count} | sources: {result.get('sources_enabled')}")

    # Per-source HTML outputs (deal_*.html + newsletter_*.html)
    # Rules:
    # - No "empty" files or placeholders are created.
    # - If persist=True, rendered from Supabase (real deals + image).
    # - If persist=False, rendered from the pipeline result.
    # - If a source was not requested by the mode, its files are deleted
    #   if they exist (avoids confusion from artifacts of previous runs).
    deals_list = result.get("deals") or []

    def _sources_requested_for_mode() -> set[str]:
        requested: set[str] = set()

        # Priority: CLI override (if present)
        if sources:
            requested |= set(sources)
        else:
            for s in (mode_cfg.get("sources") or []):
                sv = str(s).strip().lower()
                if "travel" in sv and "deal" in sv:
                    requested.add("travel-dealz")
                if "secret" in sv and "flying" in sv:
                    requested.add("secretflying")

        scrape_cfg = mode_cfg.get("scrape") or {}
        try:
            if int(scrape_cfg.get("traveldealz") or 0) > 0:
                requested.add("travel-dealz")
        except Exception:
            pass
        try:
            if int(scrape_cfg.get("secretflying") or 0) > 0:
                requested.add("secretflying")
        except Exception:
            pass

        # Duffel only if the mode explicitly requests it and we are persisting.
        # With persist=False the refresh runs in dry-run, so there are no
        # deals in the DB and generating HTML makes no sense.
        duffel_cfg = mode_cfg.get("duffel") or {}
        try:
            duffel_calls = int(duffel_cfg.get("calls") or 0)
        except Exception:
            duffel_calls = 0
        if persist and duffel_calls > 0:
            requested.add("duffel")

        return requested

    snippet_filename = {
        "travel-dealz": "snippets/deal_traveldealz.html",
        "secretflying": "snippets/deal_secretflying.html",
        "duffel": "snippets/deal_duffel.html",
    }
    newsletter_filename = {
        "travel-dealz": "snippets/newsletter_traveldealz.html",
        "secretflying": "snippets/newsletter_secretflying.html",
        "duffel": "snippets/newsletter_duffel.html",
    }

    # Per-source snippet/newsletter size (how many deals to include)
    try:
        per_source_max = int(((mode_cfg.get("html") or {}).get("max_items")) or 10)
    except Exception:
        per_source_max = 10
    if per_source_max <= 0:
        per_source_max = 10

    def _delete_if_exists(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass

    def _fetch_deals_from_supabase(source_key: str, limit_rows: int = 10) -> list[dict[str, Any]]:
        if not _client:
            return []

        # In the DB, `source` is not 100% normalised (e.g. "Travel-Dealz").
        # We use ilike to match robustly.
        if source_key == "travel-dealz":
            pattern = "%travel%dealz%"
        elif source_key == "secretflying":
            pattern = "%secret%flying%"
        else:
            pattern = f"%{source_key}%"

        try:
            rsp = (
                _client.table("deals")
                .select(
                    "title,price,currency,link,booking_url,image,source,origin,destination,origin_iata,destination_iata,airline,cabin_class,aircraft,miles,date_out,date_in,stops,baggage_included,baggage_pieces_included,baggage_allowance_kg,flight_duration_minutes,flight_duration_display,skyscanner_url,scoring"
                )
                .ilike("source", pattern)
                .order("id", desc=True)
                .limit(limit_rows)
                .execute()
            )
            return getattr(rsp, "data", []) or []
        except Exception as e:
            print(f"[run] Error fetching deals from Supabase for source={source_key!r}: {e!r}")
            return []

    def _write_source_outputs(source_key: str, deals_src: list[dict[str, Any]]) -> None:
        snip_path = BACKEND_ROOT / snippet_filename[source_key]
        nl_path = BACKEND_ROOT / newsletter_filename[source_key]

        if not deals_src:
            _delete_if_exists(snip_path)
            _delete_if_exists(nl_path)
            return

        # Snippet: 1 deal (the most recent)
        os.makedirs(os.path.dirname(snip_path), exist_ok=True)
        with open(snip_path, "w", encoding="utf-8") as f_snip:
            f_snip.write(deal_to_newsletter_row(deals_src[0]))

        # Newsletter: up to per_source_max deals
        rows_html = [deal_to_newsletter_row(d) for d in deals_src[:per_source_max]]
        os.makedirs(os.path.dirname(nl_path), exist_ok=True)
        with open(nl_path, "w", encoding="utf-8") as f_nl:
            f_nl.write(build_full_html(rows_html))

        print(f"[run] Wrote HTML outputs for {source_key}: {snip_path}, {nl_path} (n={min(len(deals_src), per_source_max)})")

    requested_sources = _sources_requested_for_mode()
    known_sources = {"travel-dealz", "secretflying", "duffel"}

    # Cleanup: if a source was NOT requested, delete its files.
    for s in sorted(known_sources - requested_sources):
        _delete_if_exists(BACKEND_ROOT / snippet_filename[s])
        _delete_if_exists(BACKEND_ROOT / newsletter_filename[s])

    # Render: we always prefer Supabase (if configured) so that
    # newsletters show up to 10 existing deals and do not depend on the
    # last run. This is read-only and does NOT violate persist=False.
    if _client:
        for s in sorted(requested_sources):
            deals_src = _fetch_deals_from_supabase(s, limit_rows=per_source_max)
            _write_source_outputs(s, deals_src)
    else:
        # Group pipeline deals by source (simple normalisation).
        by_source: dict[str, list[dict[str, Any]]] = {"travel-dealz": [], "secretflying": [], "duffel": []}
        for d in deals_list:
            src_label = str(d.get("source") or "").lower()
            if "duffel" in src_label or "amadeus" in src_label:
                by_source["duffel"].append(d)
            elif "secret" in src_label and "flying" in src_label:
                by_source["secretflying"].append(d)
            elif "travel" in src_label and "deal" in src_label:
                by_source["travel-dealz"].append(d)

        for s in sorted(requested_sources):
            _write_source_outputs(s, by_source.get(s) or [])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Unified launcher for snapcore backend")
    _setup_logging()
    # If no subcommand is passed, read the default command/mode from
    # run_config.json (default_command/default_mode) so that
    # `python -m backend.scripts.run` is controlled solely by config.
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        try:
            cfg = _load_run_config()
        except SystemExit:
            cfg = {"default_command": "deals-html", "default_mode": "normal"}

        default_cmd = str(cfg.get("default_command") or "deals-html")
        default_mode = cfg.get("default_mode")

        argv = [default_cmd]
        if default_mode:
            # Only add --mode if the subcommand supports it; otherwise it
            # will be ignored during parsing.
            argv.extend(["--mode", str(default_mode)])

    sub = parser.add_subparsers(dest="command", required=True)

    # server
    p_server = sub.add_parser("server", help="Start uvicorn server (backend.app:app)")
    p_server.add_argument(
        "--mode",
        help="Apply a mode from run_config.json before starting the server",
    )
    p_server.add_argument("--host", help="Listening host (default 127.0.0.1)")
    p_server.add_argument("--port", help="Listening port (default 8000)")
    p_server.add_argument("--reload", action="store_true", help="Enable auto-reload in development")
    p_server.set_defaults(func=cmd_server)

    # pipeline
    p_pipeline = sub.add_parser("pipeline", help="Run the deals pipeline for a given mode")
    p_pipeline.add_argument("--mode", help="Mode name defined in run_config.json", default="swiss_newsletter")
    p_pipeline.add_argument("--limit", help="Override the max number of deals to process")
    p_pipeline.add_argument(
        "--sources",
        help=(
            "Override scraping sources (default from the selected mode). "
            "Values: travel-dealz,secretflying (comma-separated)"
        ),
    )
    p_pipeline.add_argument(
        "--no-persist",
        action="store_true",
        help="Do not persist deals to Supabase (scraping + scoring in memory only)",
    )
    p_pipeline.set_defaults(func=cmd_pipeline)

    # duffel-refresh
    p_duf = sub.add_parser("duffel-refresh", help="Fill Duffel gaps according to patterns.json and the mode")
    p_duf.add_argument("--mode", help="Mode name defined in run_config.json", default="swiss_newsletter")
    p_duf.add_argument("--origin", help="Override the IATA origin (default from the mode)")
    p_duf.add_argument("--months-ahead", type=int, help="Months ahead (override)")
    p_duf.add_argument("--max-calls", type=int, help="Maximum number of Duffel calls (override)")
    p_duf.set_defaults(func=cmd_duffel_refresh)

    # html-snippet
    p_html = sub.add_parser(
        "html-snippet",
        help=(
            "Generate a deals HTML snippet for a mode (uses generate_deals_html "
            "under the hood)"
        ),
    )
    p_html.add_argument("--mode", help="Mode name defined in run_config.json", default="swiss_newsletter")
    p_html.add_argument("--limit", help="Override the deals render limit")
    p_html.add_argument("--output", help="Relative output path (from backend/) for the HTML")
    p_html.add_argument("--persist", action="store_true", help="Force persistence to Supabase")
    p_html.add_argument("--no-persist", action="store_true", help="Force no persistence even if the mode enables it")
    p_html.set_defaults(func=cmd_html_snippet)

    # newsletter-html
    p_nl = sub.add_parser(
        "newsletter-html",
        help=(
            "Generate the complete newsletter HTML for a mode "
            "(uses generate_newsletter_html under the hood)"
        ),
    )
    p_nl.add_argument("--mode", help="Mode name defined in run_config.json", default="swiss_newsletter")
    p_nl.add_argument("--limit", help="Override the deals limit to include in the newsletter")
    p_nl.add_argument("--output", help="Relative output path (from backend/) for the newsletter HTML")
    p_nl.add_argument("--persist", action="store_true", help="Force persistence to Supabase")
    p_nl.add_argument("--no-persist", action="store_true", help="Force no persistence even if the mode enables it")
    p_nl.set_defaults(func=cmd_newsletter_html)

    # cleanup-deal (cleanup by URL)
    p_clean = sub.add_parser(
        "cleanup-deal",
        help=(
            "Clean up a deal by URL (deals_traveldealz/deals_secretflying/"  # noqa: E501
            "deals + source_articles). Uses backend.scripts.cleanup_dead_deal."
        ),
    )
    p_clean.add_argument("--url", required=True, help="Article URL to clean up")
    p_clean.add_argument(
        "--clean",
        choices=["travel-dealz", "secretflying", "all"],
        default="all",
        help=(
            "Restrict cleanup to a specific source (travel-dealz, "
            "secretflying) or all (all)."
        ),
    )
    p_clean.add_argument(
        "--no-http-check",
        action="store_true",
        help="Skip the HTTP request; delete only by URL match.",
    )
    p_clean.set_defaults(func=cmd_cleanup_deal)

    # html-from-db (snippet from Supabase)
    p_html_db = sub.add_parser(
        "html-from-db",
        help=(
            "Generate an HTML snippet directly from Supabase (deals / deals_*) "
            "using generate_deals_html_from_db."
        ),
    )
    p_html_db.add_argument(
        "--table",
        default="deals",
        help="Supabase table to read deals from (e.g. deals)",
    )
    p_html_db.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("DEALS_DEFAULT_LIMIT", "20")),
        help="Maximum number of deals to render",
    )
    p_html_db.add_argument(
        "--output",
        default="snippets/deals_from_db.html",
        help="Relative output path (from backend/) for the HTML",
    )
    p_html_db.set_defaults(func=cmd_html_from_db)

    # duffel-html (Duffel -> deals -> snippet from DB)
    p_duf_html = sub.add_parser(
        "duffel-html",
        help=(
            "Launch Duffel according to the selected mode and generate an HTML snippet "
            "from the deals table in a single pass."
        ),
    )
    p_duf_html.add_argument(
        "--mode",
        help="Mode name defined in run_config.json (default duffel_test)",
        default="duffel_test",
    )
    p_duf_html.add_argument(
        "--limit",
        help="Override the deals limit to include in the snippet (default pipeline_limit of the mode)",
    )
    p_duf_html.add_argument(
        "--output",
        help="Relative output path (from backend/) for the snippet (default snippets/deals_duffel.html)",
    )
    p_duf_html.add_argument(
        "--origin",
        help="Override the IATA origin for Duffel (default duffel_origin of the mode)",
    )
    p_duf_html.add_argument(
        "--months-ahead",
        type=int,
        help="Override months ahead for Duffel gaps (default duffel_months_ahead of the mode)",
    )
    p_duf_html.add_argument(
        "--max-calls",
        type=int,
        help="Override the maximum number of Duffel calls (default duffel_max_calls of the mode)",
    )
    p_duf_html.add_argument(
        "--persist",
        action="store_true",
        help="Force persistence to Supabase (ignores the mode's persist setting)",
    )
    p_duf_html.add_argument(
        "--no-persist",
        action="store_true",
        help="Force no persistence even if the mode enables it (preview)",
    )
    p_duf_html.set_defaults(func=cmd_duffel_html)

    # demo-html (reference outputs only)
    p_demo = sub.add_parser(
        "demo-html",
        help=(
            "Generate ONLY reference HTML outputs (deal + newsletter) for Travel-Dealz and Duffel "
            "under backend/snippets/demo/ without persisting."
        ),
    )
    p_demo.add_argument("--mode", default="swiss_newsletter_llm_3", help="Mode name defined in run_config.json")
    p_demo.add_argument("--origin", default=None, help="Override Duffel origin IATA(s), e.g. ZRH or ZRH,BSL")
    p_demo.add_argument("--months-ahead", type=int, default=None, help="Override Duffel months-ahead window")
    p_demo.add_argument("--max-calls", type=int, default=1, help="Max Duffel calls for the demo")
    p_demo.add_argument("--traveldealz-limit", type=int, default=3, help="How many Travel-Dealz deals to include")
    p_demo.add_argument("--duffel-limit", type=int, default=3, help="How many Duffel deals to include")
    p_demo.set_defaults(func=cmd_demo_html)

    # scan-dead (manual review of dead URLs)
    p_scan_dead = sub.add_parser(
        "scan-dead",
        help=(
            "Scan the aggregated deals table in Supabase for dead URLs "
            "using cleanup_dead_deals_auto (dry-run by default)."
        ),
    )
    p_scan_dead.add_argument(
        "--table",
        default="deals",
        help=(
            "Supabase table to scan (normally 'deals').",
        ),
    )
    p_scan_dead.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of recent rows to review (default 200)",
    )
    p_scan_dead.add_argument(
        "--apply",
        action="store_true",
        help=(
            "If passed, also applies automatic cleanup (same as "
            "cleanup_dead_deals_auto --apply). Without this flag only prints."
        ),
    )
    p_scan_dead.set_defaults(func=cmd_scan_dead)

    # deals-html (pipeline + optional HTML + DB fallback)
    p_deals_html = sub.add_parser(
        "deals-html",
        help=(
            "Run the pipeline and generate an HTML snippet in a single "
            "pass, with optional DB fallback when there are no deals."
        ),
    )
    p_deals_html.add_argument(
        "--mode",
        help="Mode name defined in run_config.json",
        default=None,
    )
    p_deals_html.add_argument(
        "--limit",
        help="Override the deals limit to process/render",
    )
    p_deals_html.add_argument(
        "--sources",
        help=(
            "Override scraping sources (default from the mode). "
            "Values: travel-dealz,secretflying (comma-separated)"
        ),
    )
    p_deals_html.add_argument(
        "--no-persist",
        action="store_true",
        help="Do not persist deals to Supabase (scraping + scoring in memory only)",
    )
    p_deals_html.add_argument(
        "--output",
        help=(
            "Relative output path (from backend/) for the snippet. "
            "Default snippets/deals.html"
        ),
    )
    p_deals_html.add_argument(
        "--reset-deals",
        choices=["travel-dealz", "secretflying", "duffel", "deals-all", "all"],
        help=(
            "Clear deals tables before running the pipeline. "
            "Values: travel-dealz, secretflying, duffel, deals-all, all. "
            "Uses truncate_deals_tables with --force."
        ),
    )
    p_deals_html.add_argument(
        "--clean-dead",
        choices=["travel-dealz", "secretflying", "all"],
        help=(
            "Before running the pipeline, check for dead URLs in the "
            "corresponding deals table and clean them up automatically."
        ),
    )
    p_deals_html.add_argument(
        "--clean-limit",
        type=int,
        default=200,
        help="Maximum number of rows to review when using --clean-dead (default 200)",
    )
    p_deals_html.add_argument(
        "--from-db-if-empty",
        action="store_true",
        help=(
            "If the pipeline returns 0 deals, generate the HTML from the DB "
            "(deals table by default)."
        ),
    )
    p_deals_html.add_argument(
        "--table",
        default="deals",
        help="Supabase table for the DB fallback (e.g. deals)",
    )
    p_deals_html.set_defaults(func=cmd_deals_html)

    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if not func:
        parser.print_help()
        return
    func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
