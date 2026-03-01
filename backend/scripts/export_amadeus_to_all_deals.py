#!/usr/bin/env python
"""Copiar deals de Amadeus (deals_amadeus) a all_deals.

Uso (desde la raíz del repo):

    python -m backend.scripts.export_amadeus_to_all_deals \
        --origins ZRH,BSL

Si no se pasa --origins, se usar ORIGIN_IATA_FILTER del .env si existe,
si no, exporta todos los registros de deals_amadeus.
"""

from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List, Optional, Set

from dotenv import load_dotenv, find_dotenv
import sys
from pathlib import Path

# Asegurar que backend/ esté en sys.path y cargar .env antes de importar supabase_db
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

from database.supabase_db import _client, save_deals  # type: ignore  # noqa: E402
from scoring.miles_utils import great_circle_miles  # noqa: E402


def _parse_origins_arg(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    return {p.strip().upper() for p in raw.split(",") if p.strip()}


def _get_origin_filter_from_env() -> Set[str]:
    raw = os.getenv("ORIGIN_IATA_FILTER", "").strip()
    if not raw:
        return set()
    return {p.strip().upper() for p in raw.split(",") if p.strip()}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Exportar filas desde deals_amadeus a all_deals para poder "
            "identificarlas como source='amadeus'."
        ),
    )
    parser.add_argument(
        "--origins",
        help=(
            "Lista separada por comas de códigos IATA de origen a exportar "
            "(por ejemplo ZRH,BSL). Si se omite, se usa ORIGIN_IATA_FILTER "
            "del entorno; si tampoco existe, se exportan todos los orígenes."
        ),
    )
    args = parser.parse_args()

    if not _client:
        print("[export_amadeus_to_all_deals] Supabase no configurado; abortando.")
        return

    origins = _parse_origins_arg(args.origins)
    if not origins:
        origins = _get_origin_filter_from_env()
    if origins:
        print(f"[export_amadeus_to_all_deals] Filtrando por orígenes: {sorted(origins)}")
    else:
        print("[export_amadeus_to_all_deals] Sin filtro de origen; exportando todos los registros.")

    try:
        rsp = _client.table("deals_amadeus").select("*").execute()
    except Exception as e:
        print(f"[export_amadeus_to_all_deals] Error al leer deals_amadeus: {e!r}")
        return

    rows = getattr(rsp, "data", []) or []
    if not isinstance(rows, list) or not rows:
        print("[export_amadeus_to_all_deals] No hay filas en deals_amadeus; nada que exportar.")
        return

    def _keep_row(row: Dict[str, Any]) -> bool:
        if not origins:
            return True
        origin = str(row.get("origin_iata") or row.get("origin") or "").strip().upper()
        return origin in origins if origin else False

    to_export: List[Dict[str, Any]] = [r for r in rows if isinstance(r, dict) and _keep_row(r)]

    if not to_export:
        print("[export_amadeus_to_all_deals] Ninguna fila coincide con el filtro de orígenes; nada que hacer.")
        return

    all_payload: List[Dict[str, Any]] = []

    for r in to_export:
        origin_iata = r.get("origin_iata") or r.get("origin")
        dest_iata = r.get("destination_iata") or r.get("destination")

        miles = None
        if origin_iata and dest_iata:
            miles = great_circle_miles(origin_iata, dest_iata)

        all_payload.append(
            {
                "title": r.get("title"),
                "price": r.get("price"),
                "currency": r.get("currency"),
                # Para all_deals usamos los códigos IATA como origin/destination
                # si están disponibles; en su defecto, los campos de texto.
                "origin": origin_iata or r.get("origin"),
                "destination": dest_iata or r.get("destination"),
                "roundtrip": True,
                "link": r.get("link"),
                "departure_date": r.get("date_out"),
                "return_date": r.get("date_in"),
                "travel_dates_text": r.get("date_range"),
                "airline": r.get("airline"),
                "miles": miles,
                "score": None,
                "aircraft": r.get("aircraft"),
                "cabin_baggage": None,
                "booking_url": r.get("booking_url"),
                "expires_in": None,
                # Campo adicional en all_deals para poder distinguir origen.
                "source": "amadeus",
            }
        )

    print(
        f"[export_amadeus_to_all_deals] Exportando {len(all_payload)} filas desde deals_amadeus a all_deals...",
    )
    result = save_deals("all_deals", all_payload)
    print("[export_amadeus_to_all_deals] Resultado Supabase:", result)


if __name__ == "__main__":  # pragma: no cover
    main()
