#!/usr/bin/env python
"""Copiar deals de Amadeus (deals_amadeus) a la tabla deals.

Uso (desde la raíz del repo):

    python -m backend.scripts.export_amadeus_to_deals \
        --origins ZRH,BSL

Si no se pasa --origins, se usa ORIGIN_IATA_FILTER del .env si existe;
si tampoco está definido, se exportan todos los orígenes.
"""

from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import sys

# Asegurar que backend/ esté en sys.path y cargar .env antes de importar Supabase
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

from database.supabase_db import _client, save_deals  # type: ignore  # noqa: E402
from scoring.miles_utils import great_circle_miles, approximate_program_miles  # noqa: E402
from services.deals_pipeline import (
    _resolve_city_name,
    _resolve_airline_name,
    _resolve_aircraft_model,
    _extract_llm_meta,
)
from services.deals_pipeline import _load_amadeus_benchmarks, score_deals  # type: ignore  # noqa: E402
from services.deals_enrichment import enrich_deals_batch  # type: ignore  # noqa: E402


def _estimate_duration_minutes(origin_iata: Any, destination_iata: Any) -> Optional[int]:
    """Estimate duration minutes at ~500 mph great-circle; returns None on failure."""

    if not origin_iata or not destination_iata:
        return None
    try:
        gc_miles = great_circle_miles(str(origin_iata).strip().upper(), str(destination_iata).strip().upper())
    except Exception:
        gc_miles = None
    if not gc_miles or gc_miles <= 0:
        return None
    mins = int(round((gc_miles / 500.0) * 60))
    return max(mins, 40)


def _format_duration(minutes: Optional[int]) -> Optional[str]:
    if not isinstance(minutes, (int, float)) or minutes <= 0:
        return None
    h, m = divmod(int(minutes), 60)
    if h and m:
        return f"{h}h {m}m"
    if h:
        return f"{h}h"
    return f"{m}m"


def _parse_origins_arg(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    return {p.strip().upper() for p in raw.split(",") if p.strip()}


def _get_origin_filter_from_env() -> Set[str]:
    raw = os.getenv("ORIGIN_IATA_FILTER", "").strip()
    if not raw:
        return set()
    return {p.strip().upper() for p in raw.split(",") if p.strip()}


def _load_existing_deal_keys() -> Set[Tuple[str, str, str, str]]:
    """Cargar claves (origin_iata,destination_iata,date_out,date_in) ya presentes en deals.

    Esto se usa para evitar insertar duplicados si se ejecuta el script varias veces.
    """

    keys: Set[Tuple[str, str, str, str]] = set()
    if not _client:
        return keys
    try:
        rsp = (
            _client.table("deals")
            .select("origin_iata,destination_iata,date_out,date_in")
            .execute()
        )
    except Exception:
        return keys

    rows = getattr(rsp, "data", []) or []
    for row in rows:
        o = str(row.get("origin_iata") or "").strip().upper()
        d = str(row.get("destination_iata") or "").strip().upper()
        out = str(row.get("date_out") or "").strip()
        inn = str(row.get("date_in") or "").strip()
        if not (o and d and out and inn):
            continue
        keys.add((o, d, out, inn))
    return keys


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Exportar filas desde deals_amadeus a deals_all con el mismo "
            "esquema que deals_traveldealz/deals_secretflying, evitando "
            "duplicados básicos."
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
        print("[export_amadeus_to_deals] Supabase no configurado; abortando.")
        return

    origins = _parse_origins_arg(args.origins)
    if not origins:
        origins = _get_origin_filter_from_env()

    if origins:
        print(f"[export_amadeus_to_deals] Filtrando por orígenes: {sorted(origins)}")
    else:
        print("[export_amadeus_to_deals] Sin filtro de origen; exportando todos los registros.")

    try:
        rsp = _client.table("deals_amadeus").select("*").execute()
    except Exception as e:
        print(f"[export_amadeus_to_deals] Error al leer deals_amadeus: {e!r}")
        return

    rows = getattr(rsp, "data", []) or []
    if not isinstance(rows, list) or not rows:
        print("[export_amadeus_to_deals] No hay filas en deals_amadeus; nada que exportar.")
        return

    def _keep_row(row: Dict[str, Any]) -> bool:
        if not origins:
            return True
        origin = str(row.get("origin_iata") or row.get("origin") or "").strip().upper()
        return origin in origins if origin else False

    filtered_rows: List[Dict[str, Any]] = [r for r in rows if isinstance(r, dict) and _keep_row(r)]

    if not filtered_rows:
        print("[export_amadeus_to_deals] Ninguna fila coincide con el filtro de orígenes; nada que hacer.")
        return

    existing_keys = _load_existing_deal_keys()
    print(f"[export_amadeus_to_deals] Claves existentes en deals: {len(existing_keys)}")

    payload: List[Dict[str, Any]] = []

    for r in filtered_rows:
        origin_iata = str(r.get("origin_iata") or r.get("origin") or "").strip().upper()
        dest_iata = str(r.get("destination_iata") or r.get("destination") or "").strip().upper()
        date_out = str(r.get("date_out") or "").strip()
        date_in = str(r.get("date_in") or "").strip()

        if origin_iata and dest_iata and date_out and date_in:
            key = (origin_iata, dest_iata, date_out, date_in)
            if key in existing_keys:
                continue

        # Construir nombre corto del vuelo estilo "ZRH → JFK" y derivar
        # millas y duración aproximadas a partir de la distancia.
        flight_name = None
        approx_miles: Optional[int] = None
        flight_duration_minutes: Optional[int] = None
        flight_duration_display: Optional[str] = None
        itineraries: List[Dict[str, Any]] = []

        one_way = not bool(date_in)

        if origin_iata and dest_iata:
            orig_name = _resolve_city_name(None, origin_iata) or origin_iata
            dest_name = _resolve_city_name(None, dest_iata) or dest_iata
            flight_name = f"{orig_name} ({origin_iata}) → {dest_name} ({dest_iata})"
            gc_miles = great_circle_miles(origin_iata, dest_iata)
            if gc_miles is not None:
                approx_miles = approximate_program_miles(gc_miles)
                # Estimar ida
                out_mins = _estimate_duration_minutes(origin_iata, dest_iata)
                out_disp = _format_duration(out_mins)
                if out_mins:
                    flight_duration_minutes = out_mins
                    flight_duration_display = out_disp
                    itineraries.append(
                        {
                            "origin_iata": origin_iata,
                            "destination_iata": dest_iata,
                            "flight_duration_minutes": out_mins,
                            "flight_duration_display": out_disp,
                            "direct": True,
                            "date": date_out or None,
                        }
                    )

                # Estimar regreso solo si hay fecha de vuelta
                if not one_way:
                    ret_mins = _estimate_duration_minutes(dest_iata, origin_iata)
                    ret_disp = _format_duration(ret_mins)
                    if ret_mins:
                        itineraries.append(
                            {
                                "origin_iata": dest_iata,
                                "destination_iata": origin_iata,
                                "flight_duration_minutes": ret_mins,
                                "flight_duration_display": ret_disp,
                                "direct": True,
                                "date": date_in or None,
                            }
                        )

        row_payload: Dict[str, Any] = {
            "title": r.get("title") or flight_name,
            "price": r.get("price"),
            # Para Amadeus normalmente no hay URL de artículo; dejamos link a
            # lo que venga de deals_amadeus (si existiera) y booking_url vacío.
            "link": r.get("link"),
            # No fijamos booking_url para evitar conflictos de formato; se
            # puede rellenar más adelante si se añade un CTA propio.
            "booking_url": None,
            "currency": r.get("currency"),
            "image": None,
            "cabin_baggage": None,
            "aircraft": _resolve_aircraft_model(r.get("aircraft"), r.get("aircraft")) or r.get("aircraft"),
            "airline": _resolve_airline_name(r.get("airline"), r.get("airline")) or r.get("airline"),
            # Rellenar nombres de ciudad/aeropuerto a partir de IATA si no vienen.
            "origin": _resolve_city_name(r.get("origin"), origin_iata) or _resolve_city_name(None, origin_iata),
            "destination": _resolve_city_name(r.get("destination"), dest_iata) or _resolve_city_name(None, dest_iata),
            "miles": approx_miles,
            "expires_in": None,
            "date_range": r.get("date_range"),
            "date_out": date_out or None,
            "date_in": date_in or None,
            "cabin_class": r.get("cabin_class"),
            "one_way": one_way,
            "flight": flight_name,
            "origin_iata": origin_iata or None,
            "destination_iata": dest_iata or None,
            "source": "amadeus",
            # Duración estimada basada en distancia.
            "flight_duration_minutes": flight_duration_minutes,
            "flight_duration_display": flight_duration_display,
            "itineraries": itineraries or None,
            # scoring se rellenará más abajo usando la misma lógica que el pipeline.
            "scoring": None,
        }

        payload.append(row_payload)

    if not payload:
        print("[export_amadeus_to_deals] No hay nuevas filas que insertar en deals.")
        return

    # Enriquecimiento opcional (OpenAI) para todas las filas de Amadeus
    # cuando DEALS_ENRICH_DEFAULT=true (o por defecto true si no se define).
    enrich_default = os.getenv("DEALS_ENRICH_DEFAULT", "true").strip().lower() in {"1", "true", "yes", "on"}
    if enrich_default:
        payload = enrich_deals_batch(payload, max_items=len(payload))

    # Normalizar metadatos LLM: si no hubo enriquecimiento o falló, se
    # guarda llm_enriched=False y campos nulos; si hubo, se conservan los
    # campos devueltos.
    normalized_payload: List[Dict[str, Any]] = []
    for row in payload:
        row_norm = dict(row)
        row_norm.update(_extract_llm_meta(row))
        normalized_payload.append(row_norm)
    payload = normalized_payload

    # Calcular scoring usando la misma lógica que el pipeline principal.
    benchmarks = _load_amadeus_benchmarks()
    deals_for_scoring: List[Dict[str, Any]] = []
    for idx, row in enumerate(payload):
        deals_for_scoring.append(
            {
                "id": idx,
                "title": row.get("title"),
                "price": row.get("price"),
                "currency": row.get("currency"),
                "origin_iata": row.get("origin_iata"),
                "destination_iata": row.get("destination_iata"),
                # score_deals mira `departure_date` o `date_out` para extraer el mes
                "date_out": row.get("date_out"),
                "departure_date": row.get("date_out"),
            }
        )

    scored = score_deals(deals_for_scoring, benchmarks)
    scores_by_id: Dict[int, Any] = {}
    for d in scored:
        try:
            did = int(d.get("id"))
        except Exception:
            continue
        scores_by_id[did] = d.get("score")

    for idx, row in enumerate(payload):
        if idx in scores_by_id and scores_by_id[idx] is not None:
            row["scoring"] = scores_by_id[idx]

    print(f"[export_amadeus_to_deals] Insertando {len(payload)} filas nuevas en deals...")
    result = save_deals("deals", payload)
    print("[export_amadeus_to_deals] Resultado Supabase:", result)


if __name__ == "__main__":  # pragma: no cover
    main()
