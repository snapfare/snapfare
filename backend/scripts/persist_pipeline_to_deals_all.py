#!/usr/bin/env python
"""Ejecuta run_deals_pipeline con persist=True para rellenar deals_all.

Uso (desde la raíz del repo):

    python -m backend.scripts.persist_pipeline_to_deals_all

Respeta la configuración actual del entorno (.env):
- SCRAPING_URL
- ORIGIN_IATA_FILTER (si existe)
- DEALS_ENRICH_DEFAULT, etc.
"""

from __future__ import annotations

from dotenv import load_dotenv, find_dotenv

import os
import sys
from pathlib import Path

# Asegurar paths correctos para poder importar services.*
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Cargar .env de la raíz
load_dotenv(find_dotenv(), override=True)

from services.deals_pipeline import run_deals_pipeline  # type: ignore


def main() -> None:
    limit = int(os.getenv("PIPELINE_LIMIT", "40"))
    print(f"[persist_pipeline] Ejecutando run_deals_pipeline(limit={limit}, persist=True)...")

    result = run_deals_pipeline(limit=limit, persist=True, max_items_html=limit, enrich=None, sources=None)

    deals = result.get("deals") or []
    print(f"[persist_pipeline] Deals generados en memoria: {len(deals)}; sources_enabled={result.get('sources_enabled')}")
    print(f"[persist_pipeline] Persisted flag: {result.get('persisted')} | persisted_count={result.get('persisted_count')}")
    if not result.get("persisted"):
        print("[persist_pipeline] Info persistencia:", result.get("persist_info"))

    # Mostrar una pequeña muestra
    for d in deals[:5]:
        print(
            f"  - score={d.get('score')} | {d.get('price')} {d.get('currency')} | "
            f"{d.get('origin_iata')}→{d.get('destination_iata')} | {d.get('title')}",
        )


if __name__ == "__main__":  # pragma: no cover
    main()
