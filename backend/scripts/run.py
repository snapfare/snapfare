#!/usr/bin/env python
"""Launcher unificado para servidor y jobs de snapcore.

Ejemplos (desde la raíz del repo):

  # Arrancar servidor FastAPI con uvicorn
  python -m backend.scripts.run server

  # Ejecutar el pipeline de deals en modo "swiss_newsletter"
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

# Asegurar paths correctos
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Cargar .env de la raíz
load_dotenv(find_dotenv())

# Cliente Supabase (para operaciones opcionales de limpieza/cascada)
try:  # Import lazy-safe: si no hay Supabase, _client será None
    from database.supabase_db import _client  # type: ignore
except Exception:  # pragma: no cover - entorno sin Supabase
    _client = None


def _load_run_config() -> Dict[str, Any]:
    cfg_path = REPO_ROOT / "run_config.json"
    if not cfg_path.exists():
        raise SystemExit(f"run_config.json no encontrado en {cfg_path}")
    # On Windows it's common for UTF-8 files to be saved with BOM.
    # json.load() will fail unless we strip it, so use utf-8-sig.
    with cfg_path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def _get_mode(cfg: Dict[str, Any], mode: str) -> Dict[str, Any]:
    modes = cfg.get("modes") or {}
    if mode not in modes:
        available = ", ".join(sorted(modes.keys()))
        raise SystemExit(f"Modo '{mode}' no definido en run_config.json. Modos disponibles: {available}")
    return modes[mode]


def _parse_amadeus_origins(value: Any) -> list[str]:
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
    """Arrancar uvicorn sobre backend.app:app."""

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
            "uvicorn no está instalado en el entorno actual. "
            "Instala 'uvicorn[standard]' o añade uvicorn a requirements.txt."
        )

    host = args.host or "127.0.0.1"
    port = int(args.port or 8000)
    reload_flag = bool(args.reload)

    uvicorn.run("backend.app:app", host=host, port=port, reload=reload_flag)


def _apply_mode_env(mode_cfg: Dict[str, Any]) -> None:
    """Aplicar variables de entorno derivadas del modo seleccionado."""

    # Compatibilidad: algunos run_config.json (milestone) usan claves como:
    # - sources (lista) en vez de scraping_sources
    # - origin en vez de origin_filter
    # - limit en vez de pipeline_limit
    # - done_limit en vez de source_articles_done_limit
    # - scrape.traveldealz/secretflying en vez de scraping_limit_*
    # - llm.enrich/enrich_max/validate_* en vez de enrich_default/enrich_max_items/llm_validate_*
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

    # La activación de fuentes ya no depende de SCRAPING_URL, sino de los
    # límites por fuente (scraping_limit_*, amadeus_max_calls). Mantenemos
    # SCRAPING_URL únicamente por compatibilidad si aún se define en el modo.
    sources = _mode_get("scraping_sources", "sources")
    if sources:
        os.environ["SCRAPING_URL"] = ",".join(sources)
    else:
        os.environ.pop("SCRAPING_URL", None)

    origin_filter = _mode_get("origin_filter", "origin")
    if origin_filter:
        os.environ["ORIGIN_IATA_FILTER"] = str(origin_filter)
    else:
        # Para modos sin filtro explícito, eliminamos cualquier valor
        # previo heredado del entorno (p.ej. ORIGIN_IATA_FILTER en .env).
        os.environ.pop("ORIGIN_IATA_FILTER", None)

    # Permitir fijar el límite de carga de source_articles.done desde el modo
    # (útil para desactivar el filtro de duplicados en modos de prueba).
    if "source_articles_done_limit" in mode_cfg or "done_limit" in mode_cfg:
        os.environ["SOURCE_ARTICLES_DONE_LIMIT"] = str(_mode_get("source_articles_done_limit", "done_limit") or 0)

    # Límites opcionales de scraping por fuente (nº máximo de deals
    # a intentar recoger por Travel-Dealz y SecretFlying). Si no se
    # definen en el modo, se eliminan del entorno para no arrastrar
    # valores de ejecuciones anteriores.
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

    # --------------------
    # LLM / OpenAI controls
    # --------------------
    # New simplified schema (preferred):
    #   llm.action: "off" | "fill" | "correct"
    #     - off:     disables OpenAI enrichment entirely
    #     - fill:    fill ONLY miles + baggage
    #     - correct: validate/correct ALL fields (includes baggage + miles)
    # Optional:
    #   llm.max_items: cap number of items to enrich
    #   llm.allow_amadeus: allow OpenAI for Amadeus rows (default false)
    llm_action_raw = _mode_get_nested("llm.action")
    llm_action = str(llm_action_raw).strip().lower() if llm_action_raw is not None else None

    llm_action_applied = False
    if llm_action:
        # Normalize common aliases
        if llm_action in {"0", "false", "none", "off", "disabled"}:
            llm_action = "off"
        elif llm_action in {"fill", "miles", "miles_baggage", "miles+baggage"}:
            llm_action = "fill"
        elif llm_action in {"correct", "validate", "all", "all_fields"}:
            llm_action = "correct"

        llm_action_applied = llm_action in {"off", "fill", "correct"}

        if llm_action == "off":
            os.environ["DEALS_ENRICH_DEFAULT"] = "false"
            os.environ.pop("DEALS_ENRICH_MAX_ITEMS", None)
            os.environ.pop("DEALS_LLM_ENRICH_MILES", None)
            os.environ.pop("DEALS_LLM_VALIDATE_ALL", None)
            os.environ.pop("DEALS_LLM_VALIDATE_BAGGAGE", None)
            os.environ.pop("DEALS_LLM_ENRICH_AMADEUS", None)
            os.environ.pop("DEALS_LLM_ALLUCINATE", None)

        else:
            # Any LLM action implies we run the enrichment step.
            os.environ["DEALS_ENRICH_DEFAULT"] = "true"

            # Cap items (new key llm.max_items). Fall back to legacy keys.
            enrich_max = _mode_get_nested("llm.max_items")
            if enrich_max in (None, ""):
                enrich_max = _mode_get("enrich_max_items")
            if enrich_max in (None, ""):
                enrich_max = _mode_get_nested("llm.enrich_max")
            if enrich_max in (None, ""):
                os.environ.pop("DEALS_ENRICH_MAX_ITEMS", None)
            else:
                os.environ["DEALS_ENRICH_MAX_ITEMS"] = str(enrich_max)

            allow_amadeus = _mode_get_nested("llm.allow_amadeus")
            if allow_amadeus is None:
                allow_amadeus = _mode_get_nested("llm.enrich_amadeus")
            if allow_amadeus is None:
                # When LLM is enabled, default to applying it uniformly across sources,
                # including Amadeus (can still be disabled per-mode via llm.allow_amadeus=false).
                allow_amadeus = True
            os.environ["DEALS_LLM_ENRICH_AMADEUS"] = "true" if bool(allow_amadeus) else "false"

            # Keep hallucination off by default (can still be enabled via legacy key).
            allucinate = _mode_get_nested("llm.allucinate")
            if allucinate is None:
                allucinate = _mode_get_nested("llm.hallucinate")
            if allucinate is None:
                allucinate = False
            os.environ["DEALS_LLM_ALLUCINATE"] = "true" if bool(allucinate) else "false"

            if llm_action == "fill":
                os.environ["DEALS_LLM_ENRICH_MILES"] = "true"
                os.environ["DEALS_LLM_VALIDATE_BAGGAGE"] = "true"
                os.environ["DEALS_LLM_VALIDATE_ALL"] = "false"
            elif llm_action == "correct":
                os.environ["DEALS_LLM_ENRICH_MILES"] = "true"
                os.environ["DEALS_LLM_VALIDATE_BAGGAGE"] = "true"
                os.environ["DEALS_LLM_VALIDATE_ALL"] = "true"

    if not llm_action_applied:
        # Legacy schema: keep backward compatibility with old run_config.json keys.
        # Enrichment default: legacy key enrich_default; new key llm.enrich
        enrich_default_val = None
        if "enrich_default" in mode_cfg:
            enrich_default_val = bool(mode_cfg.get("enrich_default"))
        else:
            llm_enrich = _mode_get_nested("llm.enrich")
            if llm_enrich is not None:
                enrich_default_val = bool(llm_enrich)
        if enrich_default_val is not None:
            os.environ["DEALS_ENRICH_DEFAULT"] = "true" if enrich_default_val else "false"

        # Si el modo usa el esquema nuevo (llm.enrich), lo reutilizamos también
        # para habilitar específicamente el enriquecimiento de millas vía LLM.
        # Esto evita activar llamadas a OpenAI en modos legacy por sorpresa.
        llm_enrich_flag = _mode_get_nested("llm.enrich")
        if llm_enrich_flag is None:
            os.environ.pop("DEALS_LLM_ENRICH_MILES", None)
        else:
            os.environ["DEALS_LLM_ENRICH_MILES"] = "true" if bool(llm_enrich_flag) else "false"

        # Permitir (o no) enriquecimiento LLM para filas de Amadeus.
        # Por defecto, si llm.enrich está definido, seguimos ese valor.
        llm_enrich_amadeus = _mode_get_nested("llm.enrich_amadeus")
        if llm_enrich_amadeus is None:
            llm_enrich_amadeus = llm_enrich_flag
        if llm_enrich_amadeus is None:
            os.environ.pop("DEALS_LLM_ENRICH_AMADEUS", None)
        else:
            os.environ["DEALS_LLM_ENRICH_AMADEUS"] = "true" if bool(llm_enrich_amadeus) else "false"

        # Cap opcional de items a enriquecer con LLM; si no se define en el modo
        # se elimina del entorno para dejarlo ilimitado.
        enrich_max = _mode_get("enrich_max_items")
        if enrich_max in (None, ""):
            enrich_max = _mode_get_nested("llm.enrich_max")
        if enrich_max in (None, ""):
            os.environ.pop("DEALS_ENRICH_MAX_ITEMS", None)
        else:
            os.environ["DEALS_ENRICH_MAX_ITEMS"] = str(enrich_max)

        # Revisión opcional de campos vía OpenAI
        validate_all = None
        if "llm_validate_all" in mode_cfg:
            validate_all = bool(mode_cfg.get("llm_validate_all"))
        else:
            v = _mode_get_nested("llm.validate_all")
            if v is not None:
                validate_all = bool(v)
        if validate_all is None:
            os.environ.pop("DEALS_LLM_VALIDATE_ALL", None)
        else:
            os.environ["DEALS_LLM_VALIDATE_ALL"] = "true" if validate_all else "false"

        # Revisión opcional de equipaje vía OpenAI
        validate_baggage = None
        if "llm_validate_baggage" in mode_cfg:
            validate_baggage = bool(mode_cfg.get("llm_validate_baggage"))
        else:
            v = _mode_get_nested("llm.validate_baggage")
            if v is not None:
                validate_baggage = bool(v)
        if validate_baggage is None:
            os.environ.pop("DEALS_LLM_VALIDATE_BAGGAGE", None)
        else:
            os.environ["DEALS_LLM_VALIDATE_BAGGAGE"] = "true" if validate_baggage else "false"

    # Allow "hallucinated" fills (best-effort guesses) when fields are not explicitly stated.
    # The project historically used conservative prompts; this flag intentionally relaxes that.
    llm_allucinate = _mode_get_nested("llm.allucinate")
    if llm_allucinate is None:
        # Accept common misspelling/alias
        llm_allucinate = _mode_get_nested("llm.hallucinate")
    if llm_allucinate is None:
        os.environ.pop("DEALS_LLM_ALLUCINATE", None)
    else:
        os.environ["DEALS_LLM_ALLUCINATE"] = "true" if bool(llm_allucinate) else "false"

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
    """Ejecutar el pipeline de deals para un modo concreto."""

    from services.deals_pipeline import run_deals_pipeline

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter"
    mode_cfg = _get_mode(cfg, mode_name)

    _apply_mode_env(mode_cfg)

    if getattr(args, "no_persist", False):
        persist = False
    else:
        persist = bool(mode_cfg.get("persist", True))

    # Si el modo declara 'amadeus' en scraping_sources, rellenar primero
    # los gaps de Amadeus según patterns.json y los parámetros amadeus_*
    # del propio modo. De este modo, para el usuario basta con añadir
    # "amadeus" a la lista de fuentes en run_config.
    _run_amadeus_for_mode_if_enabled(mode_name, mode_cfg, persist=persist)

    limit = int(args.limit or mode_cfg.get("pipeline_limit") or mode_cfg.get("limit") or 40)

    # Permitir override de fuentes desde CLI (travel-dealz,secretflying).
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

    print(f"[run] Ejecutando pipeline en modo='{mode_name}' con limit={limit}...")
    if sources:
        print(f"[run] Fuentes overrideadas desde CLI: {sorted(sources)}")

    result = run_deals_pipeline(
        limit=limit,
        persist=persist,
        max_items_html=limit,
        enrich=None,
        sources=sources,
    )

    print(f"[run] Deals generados: {result.get('count')} | fuentes: {result.get('sources_enabled')}")
    deals = (result.get("deals") or [])[:5]
    for d in deals:
        print(f" - {d.get('score')} | {d.get('price')} {d.get('currency')} | {d.get('title')}")


def cmd_amadeus_refresh(args: argparse.Namespace) -> None:
    """Rellenar gaps de Amadeus basados en patterns.json según el modo."""

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter"
    mode_cfg = _get_mode(cfg, mode_name)

    amadeus_cfg = mode_cfg.get("amadeus") if isinstance(mode_cfg.get("amadeus"), dict) else {}

    raw_origin = args.origin or mode_cfg.get("amadeus_origin") or amadeus_cfg.get("origins") or "ZRH"
    origins = _parse_amadeus_origins(raw_origin)
    months_ahead = args.months_ahead or mode_cfg.get("amadeus_months_ahead") or amadeus_cfg.get("months") or 4
    max_calls = args.max_calls or mode_cfg.get("amadeus_max_calls") or amadeus_cfg.get("calls") or 72

    for origin in origins:
        cmd = [
            sys.executable,
            "-m",
            "backend.scripts.fill_amadeus_gaps_from_patterns_v2",
            "--origin",
            str(origin),
            "--months-ahead",
            str(months_ahead),
            "--max-calls",
            str(max_calls),
        ]

        print("[run] Ejecutando:", " ".join(cmd))
        subprocess.run(cmd, check=False)


def cmd_html_snippet(args: argparse.Namespace) -> None:
    """Generar un snippet HTML de deals usando el modo seleccionado.

    Este comando aplica primero la configuración del modo (scrapers,
    ORIGIN_IATA_FILTER, etc.) y luego delega en
    `backend.scripts.generate_deals_html` para escribir el snippet en
    disco.
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

    print("[run] Ejecutando:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_newsletter_html(args: argparse.Namespace) -> None:
    """Generar el HTML completo del newsletter para un modo.

    Aplica primero la configuración del modo seleccionado y luego
    delega en `backend.scripts.generate_newsletter_html` para generar el
    HTML completo basado en la plantilla de newsletter.
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

    print("[run] Ejecutando:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_cleanup_deal(args: argparse.Namespace) -> None:
    """Wrapper para limpiar un deal "muerto" (URL que ya no existe).

    Delegamos en backend.scripts.cleanup_dead_deal para que borre las
    filas correspondientes en deals_* y source_articles.
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

    print("[run] Ejecutando:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_html_from_db(args: argparse.Namespace) -> None:
    """Generar snippet HTML directamente desde Supabase (tabla deals).

    Esto es útil cuando el scraping en vivo devuelve 0 deals pero ya
    tienes un histórico en la tabla agregada deals.
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

    print("[run] Ejecutando:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def cmd_amadeus_html(args: argparse.Namespace) -> None:
        """Lanzar Amadeus (según el modo) y generar un snippet desde deals.

        Flujo:
        - Leer configuración del modo (amadeus_origin, amadeus_months_ahead,
            amadeus_max_calls).
        - Ejecutar fill_amadeus_gaps_from_patterns_v2 para rellenar deals con
            filas de Amadeus (source="amadeus").
        - Generar un snippet HTML desde la tabla deals usando
            generate_deals_html_from_db.
        """

        cfg = _load_run_config()
        mode_name = args.mode or "amadeus_test"
        mode_cfg = _get_mode(cfg, mode_name)

        _apply_mode_env(mode_cfg)

        # Persist control (mode default can be overridden)
        if getattr(args, "persist", False):
            persist = True
        elif getattr(args, "no_persist", False):
            persist = False
        else:
            persist = bool(mode_cfg.get("persist", True))

        raw_origin = args.origin or mode_cfg.get("amadeus_origin") or "ZRH"
        origins = _parse_amadeus_origins(raw_origin)
        months_ahead = args.months_ahead or mode_cfg.get("amadeus_months_ahead") or 4
        max_calls = args.max_calls or mode_cfg.get("amadeus_max_calls") or 10

        # 1) Rellenar deals con filas de Amadeus para cada origen
        # In no-persist mode we still want a preview snippet, so we ask the
        # script to dump computed rows to JSON.
        safe_mode = "".join(ch if ch.isalnum() else "_" for ch in str(mode_name))
        preview_json = f"backend/snippets/amadeus_preview_{safe_mode}.json"
        for origin in origins:
            cmd_fill = [
                sys.executable,
                "-m",
                "backend.scripts.fill_amadeus_gaps_from_patterns_v2",
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

            print("[run] Ejecutando (Amadeus fill):", " ".join(cmd_fill))
            subprocess.run(cmd_fill, check=False)

        # 2) Generar snippet HTML desde deals
        limit = int(args.limit or mode_cfg.get("pipeline_limit") or 10)
        output_rel = args.output or "snippets/deal_amadeus.html"

        cmd_snip = [
            sys.executable,
            "-m",
            "backend.scripts.generate_deals_html_from_db",
            "--limit",
            str(limit),
            "--output",
            output_rel,
        ]

        # Only show Amadeus rows in this command.
        cmd_snip.extend(["--source", "amadeus"])

        if not persist:
            cmd_snip.extend(["--input-json", preview_json])
        else:
            cmd_snip.extend(["--table", "deals"])

        print("[run] Ejecutando (Amadeus snippet):", " ".join(cmd_snip))
        subprocess.run(cmd_snip, check=False)


def cmd_demo_html(args: argparse.Namespace) -> None:
    """Generate a minimal set of reference HTML files for quick visual QA.

    Outputs (under backend/snippets/demo/):
      - deal_traveldealz.html
      - newsletter_traveldealz.html
      - deal_amadeus.html
      - newsletter_amadeus.html

    This command is designed to avoid creating extra artifacts and to work
    without persisting anything to Supabase.
    """

    from services.deals_pipeline import run_deals_pipeline
    from scoring.html_output import deal_to_newsletter_row, build_full_html

    cfg = _load_run_config()
    mode_name = args.mode or "swiss_newsletter_llm_3"
    mode_cfg = _get_mode(cfg, mode_name)
    _apply_mode_env(mode_cfg)

    # Ensure Amadeus demo rows always show baggage (assumed) even if Amadeus omits it.
    os.environ.setdefault("AMADEUS_ASSUME_BAGGAGE", "true")

    demo_dir = BACKEND_ROOT / "snippets" / "demo"
    demo_dir.mkdir(parents=True, exist_ok=True)

    td_limit = int(getattr(args, "traveldealz_limit", None) or 3)
    ama_limit = int(getattr(args, "amadeus_limit", None) or 3)
    max_calls = int(getattr(args, "max_calls", None) or 1)

    # 1) Travel-Dealz: run pipeline in-memory only, travel-dealz source only
    td_result = run_deals_pipeline(
        limit=td_limit,
        persist=False,
        max_items_html=min(td_limit, 10),
        enrich=None,
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
                    "title,price,currency,link,booking_url,image,source,origin,destination,origin_iata,destination_iata,airline,cabin_class,cabin_baggage,aircraft,miles,date_out,date_in,date_range,baggage_included,baggage_pieces_included,baggage_allowance_kg,baggage_allowance_display,llm_enriched_fields"
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

    # 2) Amadeus: run gap filler in dry-run and render from the produced JSON
    raw_origin = args.origin or (mode_cfg.get("amadeus_origin") or "ZRH")
    origins = _parse_amadeus_origins(raw_origin)
    months_ahead = int(getattr(args, "months_ahead", None) or (mode_cfg.get("amadeus_months_ahead") or 4))

    preview_json = str(demo_dir / "amadeus_preview.json")
    for origin in origins[:1]:
        cmd_fill = [
            sys.executable,
            "-m",
            "backend.scripts.fill_amadeus_gaps_from_patterns_v2",
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
        print("[run] demo-html executing (Amadeus dry-run):", " ".join(cmd_fill))
        subprocess.run(cmd_fill, check=False)
        break

    ama_deal_path = demo_dir / "deal_amadeus.html"
    ama_news_path = demo_dir / "newsletter_amadeus.html"
    try:
        import json as _json

        rows = _json.loads(Path(preview_json).read_text(encoding="utf-8"))
        rows = rows if isinstance(rows, list) else []
    except Exception:
        rows = []

    # Ensure the newsletter template shows program miles display when present.
    ama_deals: list[dict[str, Any]] = []
    for r in rows[: ama_limit]:
        if not isinstance(r, dict):
            continue
        llm_fields = r.get("llm_enriched_fields")
        if isinstance(llm_fields, dict):
            mpd = llm_fields.get("miles_programs_display")
            if mpd and r.get("miles") not in (None, ""):
                # Prefer the richer display if it exists (newsletter renderer will display strings),
                # but keep only one valid program for the airline.
                try:
                    from scoring.miles_utils import filter_miles_programs_display

                    airline_name = r.get("airline") or (llm_fields.get("airline_name") if isinstance(llm_fields, dict) else None)
                    r["miles"] = filter_miles_programs_display(str(mpd), str(airline_name) if airline_name else None) or mpd
                except Exception:
                    r["miles"] = mpd
        ama_deals.append(r)

    if ama_deals:
        ama_rows = [deal_to_newsletter_row(d) for d in ama_deals[: ama_limit]]
        ama_deal_path.write_text(ama_rows[0], encoding="utf-8")
        ama_news_path.write_text(build_full_html(ama_rows), encoding="utf-8")
        print(f"[run] demo-html wrote: {ama_deal_path} and {ama_news_path} (n={len(ama_rows)})")
    else:
        try:
            if ama_deal_path.exists():
                ama_deal_path.unlink()
            if ama_news_path.exists():
                ama_news_path.unlink()
        except Exception:
            pass
        print("[run] demo-html: no Amadeus deals produced (no files written)")

    # Keep demo folder clean: remove intermediate JSON.
    try:
        pj = Path(preview_json)
        if pj.exists():
            pj.unlink()
    except Exception:
        pass


def cmd_scan_dead(args: argparse.Namespace) -> None:
    """Escanear tablas de deals en Supabase en busca de URLs muertas.

    Wrapper cómodo sobre backend.scripts.cleanup_dead_deals_auto en modo
    "repaso manual": por defecto NO borra nada, solo imprime qué URLs
    parecen muertas (status >= 400 o error de red).
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

    # Solo si se pasa --apply explícitamente se añaden los borrados reales.
    if args.apply:
        cmd.append("--apply")

    print("[run] Ejecutando escaneo de URLs muertas:", " ".join(cmd))
    subprocess.run(cmd, check=False)


def _cascade_delete_for_source(source_key: str) -> None:
    """Borrar en cascada filas relacionadas en deals y source_articles.

    Pensado para resets masivos por fuente (travel-dealz, secretflying, ...)
    cuando se vacía la tabla específica (deals_traveldealz, deals_secretflying).
    """

    if not _client:
        print(f"[run] Supabase no configurado; omitiendo borrado en cascada para source={source_key}.")
        return

    try:
        # 1) Borrar filas en deals asociadas a esta fuente.
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
                    f"[run] Borradas {len(ids)} filas de deals para source~{source_key}.",
                )
        except Exception as e:
            print(f"[run] Error borrando de deals para source={source_key}: {e!r}")

        # 2) Borrar filas en source_articles asociadas a esta fuente.
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
                    f"[run] Borradas {len(ids_sa)} filas de source_articles para source={source_key}.",
                )
        except Exception as e:
            print(f"[run] Error borrando de source_articles para source={source_key}: {e!r}")
    except Exception as e:  # pragma: no cover - error inesperado
        print(f"[run] Error inesperado en borrado en cascada para source={source_key}: {e!r}")


def _run_amadeus_for_mode_if_enabled(mode_name: str, mode_cfg: Dict[str, Any], persist: bool = True) -> Dict[str, Any]:
    """Run Amadeus gap-filling for a mode if it is enabled.

    Activation is driven by Amadeus-specific parameters (for example,
    amadeus_max_calls > 0) instead of relying on scraping_sources.

    The ``persist`` flag controls whether the underlying script is allowed
    to write rows into Supabase. When ``persist`` is False, the script is
    executed in dry-run mode so that it only logs what it *would* save
    without performing any database writes. This is important for preview
    modes where we want to avoid touching Supabase at all.
    """

    # Support both legacy top-level keys (amadeus_*) and nested config:
    #   "amadeus": {"origins": ["ZRH"], "months": 4, "calls": 3}
    amadeus_cfg = mode_cfg.get("amadeus") if isinstance(mode_cfg.get("amadeus"), dict) else {}

    max_calls_cfg = mode_cfg.get("amadeus_max_calls")
    if max_calls_cfg is None:
        max_calls_cfg = amadeus_cfg.get("calls")

    try:
        if max_calls_cfg is None or int(max_calls_cfg) <= 0:
            return {"ran": False}
    except Exception:
        return {"ran": False}

    raw_origin = mode_cfg.get("amadeus_origin")
    if raw_origin in (None, ""):
        raw_origin = amadeus_cfg.get("origins")
    raw_origin = raw_origin or "ZRH"
    origins = _parse_amadeus_origins(raw_origin)

    months_ahead = mode_cfg.get("amadeus_months_ahead")
    if months_ahead in (None, ""):
        months_ahead = amadeus_cfg.get("months")
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
            "backend.scripts.fill_amadeus_gaps_from_patterns_v2",
            "--origin",
            str(origin),
            "--months-ahead",
            str(months_ahead),
            "--max-calls",
            str(calls_for_origin),
        ]

        # En modos de previsualización (persist=False) ejecutamos el script
        # en modo dry-run para no escribir nada en Supabase.
        if not persist:
            cmd.append("--dry-run")

        print(
            f"[run] Mode '{mode_name}' has Amadeus enabled; "
            "running Amadeus refresh before the pipeline:",
            " ".join(cmd),
        )
        # Avoid hanging runs when Amadeus/network stalls.
        # Priority: per-mode run_config.json -> env var -> default.
        timeout_s: float
        timeout_raw = None
        if isinstance(mode_cfg, dict):
            timeout_raw = mode_cfg.get("timeout_amadeus")

        if timeout_raw not in (None, ""):
            try:
                timeout_s = float(timeout_raw)
            except Exception:
                timeout_s = 120.0
        else:
            try:
                timeout_s = float(os.getenv("AMADEUS_REFRESH_TIMEOUT_SECONDS", "120"))
            except Exception:
                timeout_s = 120.0

        try:
            subprocess.run(cmd, check=False, timeout=timeout_s)
            ran_any = True
        except subprocess.TimeoutExpired:
            print(f"[run] Amadeus refresh timed out after {timeout_s:.0f}s; continuing without waiting.")
            ran_any = True
            timeout_count += 1

        # Consumir presupuesto total (no per-origin). Si quieres repartirlo, ajusta
        # amadeus_max_calls o la lista de orígenes.
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
    mode_name = args.mode or "swiss_newsletter"
    mode_cfg = _get_mode(cfg, mode_name)

    _apply_mode_env(mode_cfg)

    if getattr(args, "no_persist", False):
        persist = False
    else:
        persist = bool(mode_cfg.get("persist", True))

    # Igual que en cmd_pipeline: si el modo incluye 'amadeus' como fuente,
    # ejecutamos primero el refresco Amadeus basado en patterns.json para
    # que los benchmarks/filas de Amadeus estén listos antes del pipeline.
    _run_amadeus_for_mode_if_enabled(mode_name, mode_cfg, persist=persist)

    limit = int(args.limit or mode_cfg.get("pipeline_limit") or mode_cfg.get("limit") or 40)

    # Override opcional de fuentes
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

    # Reset opcional de tablas de deals antes de ejecutar el pipeline.
    # Permite elegir la fuente a vaciar: travel-dealz, secretflying,
    # amadeus, deals-all o all.
    reset_target = getattr(args, "reset_deals", None)
    if reset_target:
        if reset_target == "travel-dealz":
            tables = "deals"
            # En el esquema actual, travel-dealz se almacena en la tabla
            # agregada deals; también borramos las filas correspondientes en
            # source_articles.
            _cascade_delete_for_source("travel-dealz")
        elif reset_target == "secretflying":
            tables = "deals"
            _cascade_delete_for_source("secretflying")
        elif reset_target == "amadeus":
            tables = "deals_amadeus"
        elif reset_target == "deals-all":
            tables = "deals"
        else:  # "all"
            tables = "deals,deals_amadeus"
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
        print("[run] Ejecutando reset de tablas de deals:", " ".join(reset_cmd))
        subprocess.run(reset_cmd, check=False)

    # Limpieza opcional de URLs muertas antes de ejecutar el pipeline.
    clean_dead = getattr(args, "clean_dead", None)
    if clean_dead:
        # En el esquema actual todas las fuentes se almacenan en `deals`,
        # así que siempre usamos esa tabla como base para la limpieza.
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
        print("[run] Ejecutando limpieza de URLs muertas:", " ".join(clean_cmd))
        subprocess.run(clean_cmd, check=False)

    print(f"[run] Running deals-html in mode='{mode_name}' with limit={limit}...")
    if sources:
        print(f"[run] Sources overridden from CLI: {sorted(sources)}")

    result = run_deals_pipeline(
        limit=limit,
        persist=persist,
        # Render at most 10 items in the main HTML snippet for readability
        max_items_html=min(limit, 10),
        enrich=None,
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

    # Nota: ya no generamos el snippet legacy "deals.html" ni el
    # fallback desde BD aquí, porque los snippets de ejemplo se basan
    # en la plantilla de newsletter.
    print(f"[run] Deals generated by pipeline: {count} | sources: {result.get('sources_enabled')}")

    # Per-source HTML outputs (deal_*.html + newsletter_*.html)
    # Reglas:
    # - No se crean archivos "vacíos" ni placeholders.
    # - Si persist=True, se renderiza desde Supabase (deals reales + imagen).
    # - Si persist=False, se renderiza desde el resultado del pipeline.
    # - Si una fuente no fue solicitada por el modo, se eliminan sus archivos
    #   si existen (evita confusión por artefactos de ejecuciones anteriores).
    deals_list = result.get("deals") or []

    def _sources_requested_for_mode() -> set[str]:
        requested: set[str] = set()

        # Prioridad: override de CLI (si existe)
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

        # Amadeus sólo si el modo lo pide explícitamente y estamos persistiendo.
        # En persist=False el refresco se ejecuta en dry-run, así que no hay
        # deals en BD y no tiene sentido generar HTML.
        amadeus_cfg = mode_cfg.get("amadeus") or {}
        try:
            amadeus_calls = int(amadeus_cfg.get("calls") or 0)
        except Exception:
            amadeus_calls = 0
        if persist and amadeus_calls > 0:
            requested.add("amadeus")

        return requested

    snippet_filename = {
        "travel-dealz": "snippets/deal_traveldealz.html",
        "secretflying": "snippets/deal_secretflying.html",
        "amadeus": "snippets/deal_amadeus.html",
    }
    newsletter_filename = {
        "travel-dealz": "snippets/newsletter_traveldealz.html",
        "secretflying": "snippets/newsletter_secretflying.html",
        "amadeus": "snippets/newsletter_amadeus.html",
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

        # En BD, `source` no está 100% normalizado (p.ej. "Travel-Dealz").
        # Usamos ilike para matchear de forma robusta.
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
                    "title,price,currency,link,booking_url,image,source,origin,destination,origin_iata,destination_iata,airline,cabin_class,cabin_baggage,aircraft,miles,date_out,date_in,date_range,baggage_included,baggage_pieces_included,baggage_allowance_kg,baggage_allowance_display,llm_enriched_fields"
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

        # Snippet: 1 deal (el más reciente)
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
    known_sources = {"travel-dealz", "secretflying", "amadeus"}

    # Limpieza: si una fuente NO fue solicitada, eliminamos sus archivos.
    for s in sorted(known_sources - requested_sources):
        _delete_if_exists(BACKEND_ROOT / snippet_filename[s])
        _delete_if_exists(BACKEND_ROOT / newsletter_filename[s])

    # Render: preferimos siempre Supabase (si está configurado) para que
    # las newsletters muestren hasta 10 deals existentes y no dependan de
    # la última ejecución. Esto es solo lectura y NO viola persist=False.
    if _client:
        for s in sorted(requested_sources):
            deals_src = _fetch_deals_from_supabase(s, limit_rows=per_source_max)
            _write_source_outputs(s, deals_src)
    else:
        # Agrupar deals del pipeline por fuente (normalización simple).
        by_source: dict[str, list[dict[str, Any]]] = {"travel-dealz": [], "secretflying": [], "amadeus": []}
        for d in deals_list:
            src_label = str(d.get("source") or "").lower()
            if "amadeus" in src_label:
                by_source["amadeus"].append(d)
            elif "secret" in src_label and "flying" in src_label:
                by_source["secretflying"].append(d)
            elif "travel" in src_label and "deal" in src_label:
                by_source["travel-dealz"].append(d)

        for s in sorted(requested_sources):
            _write_source_outputs(s, by_source.get(s) or [])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Launcher unificado para snapcore backend")
    _setup_logging()
    # Si no se pasa ningún subcomando, leemos el comando/modo por defecto
    # de run_config.json (default_command/default_mode) para que
    # `python -m backend.scripts.run` se controle solo con config.
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
            # Solo añadimos --mode si el subcomando lo soporta; si no, se
            # ignorará en el parseo.
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

    # amadeus-refresh
    p_ama = sub.add_parser("amadeus-refresh", help="Rellenar gaps Amadeus según patterns.json y modo")
    p_ama.add_argument("--mode", help="Nombre del modo definido en run_config.json", default="swiss_newsletter")
    p_ama.add_argument("--origin", help="Override del origen IATA (por defecto del modo)")
    p_ama.add_argument("--months-ahead", type=int, help="Meses hacia adelante (override)")
    p_ama.add_argument("--max-calls", type=int, help="Máximo de llamadas a Amadeus (override)")
    p_ama.set_defaults(func=cmd_amadeus_refresh)

    # html-snippet
    p_html = sub.add_parser(
        "html-snippet",
        help=(
            "Generar snippet HTML de deals para un modo (usa generate_deals_html "
            "bajo el capó)"
        ),
    )
    p_html.add_argument("--mode", help="Nombre del modo definido en run_config.json", default="swiss_newsletter")
    p_html.add_argument("--limit", help="Override del límite de deals a renderizar")
    p_html.add_argument("--output", help="Ruta de salida relativa (desde backend/) para el HTML")
    p_html.add_argument("--persist", action="store_true", help="Forzar persistencia en Supabase")
    p_html.add_argument("--no-persist", action="store_true", help="Forzar no persistir aunque el modo lo habilite")
    p_html.set_defaults(func=cmd_html_snippet)

    # newsletter-html
    p_nl = sub.add_parser(
        "newsletter-html",
        help=(
            "Generar el HTML completo del newsletter para un modo "
            "(usa generate_newsletter_html bajo el capó)"
        ),
    )
    p_nl.add_argument("--mode", help="Nombre del modo definido en run_config.json", default="swiss_newsletter")
    p_nl.add_argument("--limit", help="Override del límite de deals a incluir en el newsletter")
    p_nl.add_argument("--output", help="Ruta de salida relativa (desde backend/) para el HTML del newsletter")
    p_nl.add_argument("--persist", action="store_true", help="Forzar persistencia en Supabase")
    p_nl.add_argument("--no-persist", action="store_true", help="Forzar no persistir aunque el modo lo habilite")
    p_nl.set_defaults(func=cmd_newsletter_html)

    # cleanup-deal (limpieza por URL)
    p_clean = sub.add_parser(
        "cleanup-deal",
        help=(
            "Limpiar un deal por URL (deals_traveldealz/deals_secretflying/"  # noqa: E501
            "deals + source_articles). Usa backend.scripts.cleanup_dead_deal."
        ),
    )
    p_clean.add_argument("--url", required=True, help="Article URL a limpiar")
    p_clean.add_argument(
        "--clean",
        choices=["travel-dealz", "secretflying", "all"],
        default="all",
        help=(
            "Limitar la limpieza a una fuente concreta (travel-dealz, "
            "secretflying) o todas (all)."
        ),
    )
    p_clean.add_argument(
        "--no-http-check",
        action="store_true",
        help="No hacer petición HTTP previa; borrar sólo por URL match.",
    )
    p_clean.set_defaults(func=cmd_cleanup_deal)

    # html-from-db (snippet desde Supabase)
    p_html_db = sub.add_parser(
        "html-from-db",
        help=(
            "Generar snippet HTML directamente desde Supabase (deals / deals_*) "
            "usando generate_deals_html_from_db."
        ),
    )
    p_html_db.add_argument(
        "--table",
        default="deals",
        help="Tabla de Supabase de la que leer deals (p.ej. deals)",
    )
    p_html_db.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("DEALS_DEFAULT_LIMIT", "20")),
        help="Número máximo de deals a renderizar",
    )
    p_html_db.add_argument(
        "--output",
        default="snippets/deals_from_db.html",
        help="Ruta de salida relativa (desde backend/) para el HTML",
    )
    p_html_db.set_defaults(func=cmd_html_from_db)

    # amadeus-html (Amadeus -> deals -> snippet desde BD)
    p_ama_html = sub.add_parser(
        "amadeus-html",
        help=(
            "Lanzar Amadeus según el modo seleccionado y generar un snippet "
            "HTML desde la tabla deals en una sola pasada."
        ),
    )
    p_ama_html.add_argument(
        "--mode",
        help="Nombre del modo definido en run_config.json (por defecto amadeus_test)",
        default="amadeus_test",
    )
    p_ama_html.add_argument(
        "--limit",
        help="Override del límite de deals a incluir en el snippet (por defecto pipeline_limit del modo)",
    )
    p_ama_html.add_argument(
        "--output",
        help="Ruta de salida relativa (desde backend/) para el snippet (por defecto snippets/deals_amadeus.html)",
    )
    p_ama_html.add_argument(
        "--origin",
        help="Override del origen IATA para Amadeus (por defecto amadeus_origin del modo)",
    )
    p_ama_html.add_argument(
        "--months-ahead",
        type=int,
        help="Override de meses hacia adelante para gaps de Amadeus (por defecto amadeus_months_ahead del modo)",
    )
    p_ama_html.add_argument(
        "--max-calls",
        type=int,
        help="Override del máximo de llamadas a Amadeus (por defecto amadeus_max_calls del modo)",
    )
    p_ama_html.add_argument(
        "--persist",
        action="store_true",
        help="Forzar persistencia en Supabase (ignora el persist del modo)",
    )
    p_ama_html.add_argument(
        "--no-persist",
        action="store_true",
        help="Forzar no persistir aunque el modo lo habilite (preview)",
    )
    p_ama_html.set_defaults(func=cmd_amadeus_html)

    # demo-html (reference outputs only)
    p_demo = sub.add_parser(
        "demo-html",
        help=(
            "Generate ONLY reference HTML outputs (deal + newsletter) for Travel-Dealz and Amadeus "
            "under backend/snippets/demo/ without persisting."
        ),
    )
    p_demo.add_argument("--mode", default="swiss_newsletter_llm_3", help="Mode name defined in run_config.json")
    p_demo.add_argument("--origin", default=None, help="Override Amadeus origin IATA(s), e.g. ZRH or ZRH,BSL")
    p_demo.add_argument("--months-ahead", type=int, default=None, help="Override Amadeus months-ahead window")
    p_demo.add_argument("--max-calls", type=int, default=1, help="Max Amadeus calls for the demo")
    p_demo.add_argument("--traveldealz-limit", type=int, default=3, help="How many Travel-Dealz deals to include")
    p_demo.add_argument("--amadeus-limit", type=int, default=3, help="How many Amadeus deals to include")
    p_demo.set_defaults(func=cmd_demo_html)

    # scan-dead (repaso manual de URLs muertas)
    p_scan_dead = sub.add_parser(
        "scan-dead",
        help=(
            "Escanear la tabla agregada de deals en Supabase en busca de "
            "URLs muertas usando cleanup_dead_deals_auto (dry-run por "
            "defecto)."
        ),
    )
    p_scan_dead.add_argument(
        "--table",
        default="deals",
        help=(
            "Tabla de Supabase a escanear (normalmente 'deals').",
        ),
    )
    p_scan_dead.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Número máximo de filas recientes a revisar (por defecto 200)",
    )
    p_scan_dead.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Si se pasa, aplica también la limpieza automática (igual que "
            "cleanup_dead_deals_auto --apply). Sin este flag solo imprime."
        ),
    )
    p_scan_dead.set_defaults(func=cmd_scan_dead)

    # deals-html (pipeline + HTML opcional + fallback a BD)
    p_deals_html = sub.add_parser(
        "deals-html",
        help=(
            "Ejecutar el pipeline y generar un snippet HTML en una sola "
            "pasada, con fallback opcional a BD si no hay deals."
        ),
    )
    p_deals_html.add_argument(
        "--mode",
        help="Mode name defined in run_config.json",
        default="normal",
    )
    p_deals_html.add_argument(
        "--limit",
        help="Override del límite de deals a procesar/renderizar",
    )
    p_deals_html.add_argument(
        "--sources",
        help=(
            "Override de fuentes de scraping (por defecto las del modo). "
            "Valores: travel-dealz,secretflying (separados por comas)"
        ),
    )
    p_deals_html.add_argument(
        "--no-persist",
        action="store_true",
        help="No persistir deals en Supabase (solo scraping + scoring en memoria)",
    )
    p_deals_html.add_argument(
        "--output",
        help=(
            "Ruta de salida relativa (desde backend/) para el snippet. "
            "Por defecto snippets/deals.html"
        ),
    )
    p_deals_html.add_argument(
        "--reset-deals",
        choices=["travel-dealz", "secretflying", "amadeus", "deals-all", "all"],
        help=(
            "Vaciar tablas de deals antes de ejecutar el pipeline. "
            "Valores: travel-dealz, secretflying, amadeus, deals-all, all. "
            "Usa truncate_deals_tables con --force."
        ),
    )
    p_deals_html.add_argument(
        "--clean-dead",
        choices=["travel-dealz", "secretflying", "all"],
        help=(
            "Antes de ejecutar el pipeline, revisar URLs muertas en la "
            "tabla de deals correspondiente y limpiarlas automáticamente."
        ),
    )
    p_deals_html.add_argument(
        "--clean-limit",
        type=int,
        default=200,
        help="Número máximo de filas a revisar al usar --clean-dead (por defecto 200)",
    )
    p_deals_html.add_argument(
        "--from-db-if-empty",
        action="store_true",
        help=(
            "Si el pipeline devuelve 0 deals, generar el HTML desde BD "
            "(tabla deals por defecto)."
        ),
    )
    p_deals_html.add_argument(
        "--table",
        default="deals",
        help="Tabla de Supabase para el fallback desde BD (p.ej. deals)",
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
