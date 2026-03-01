import json
import os
from pathlib import Path
from typing import Any, Dict, List

import sys
from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv

# Cargar .env ANTES de importar supabase_db, igual que hace app.py
load_dotenv(find_dotenv())

# Hacer importables services/ y database/ cuando ejecutamos desde scripts/
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from services.secretflying_article_parser import _parse_secretflying_html  # type: ignore
from services.deals_enrichment import enrich_deals_batch  # type: ignore
from services.deals_pipeline import (  # type: ignore
    _extract_llm_meta,
    _load_amadeus_benchmarks,
    _normalize_deal_fields,
    score_deals,
)
from database.supabase_db import save_deals  # type: ignore

DEFAULT_DUMP = "secretflying_zurich-switzerland-singapore-e406-roundtrip.html"


def infer_url_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    link = soup.find("link", rel="canonical")
    if link and link.get("href"):
        return str(link["href"])
    return "https://www.secretflying.com/dump"


def build_secretflying_rows_from_deal(deal: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Construir filas para deals_secretflying a partir de un deal ya parseado.

    Replica la lógica principal usada en el pipeline: una fila por ruta, si
    existe bloque "Routes:", o una sola fila si no hay rutas estructuradas.
    """

    routes = deal.get("routes") or []
    rows: List[Dict[str, Any]] = []

    # Valores base comunes al post
    base_title = deal.get("title")
    base_link = deal.get("link")
    base_price = deal.get("price")
    base_currency = deal.get("currency")
    base_image = deal.get("image")
    base_cabin_baggage = deal.get("cabin_baggage")
    base_aircraft = deal.get("aircraft")
    base_airline = deal.get("airline")
    base_miles = deal.get("miles")
    base_flight_duration_minutes = deal.get("flight_duration_minutes")
    base_flight_duration_display = deal.get("flight_duration_display")
    base_expires = deal.get("expires_in")
    base_date_range = (
        deal.get("travel_dates_text")
        or deal.get("travel_dates_summary")
        or deal.get("date_range")
    )
    base_date_out = deal.get("departure_date")
    base_date_in = deal.get("return_date")
    base_cabin_class = deal.get("cabin_class")
    base_oneway = deal.get("one_way")
    base_origin_iata = deal.get("origin_iata")
    base_dest_iata = deal.get("destination_iata")
    base_booking_url = deal.get("booking_url")
    score_val = deal.get("score")
    base_scoring = score_val if score_val is not None else deal.get("scoring")
    base_llm_enriched = deal.get("llm_enriched")
    base_llm_fields = deal.get("llm_enriched_fields")
    base_llm_version = deal.get("llm_enrichment_version")

    def _fmt_place(city: Any, code: Any) -> str | None:
        city_str = str(city).strip() if isinstance(city, str) else None
        code_str = str(code).strip().upper() if isinstance(code, str) else None
        if city_str and code_str and len(code_str) == 3 and code_str.isalpha() and code_str != city_str.upper():
            return f"{city_str} ({code_str})"
        if city_str:
            return city_str
        if code_str:
            return code_str
        return None

    # Si tenemos itinerarios y no hay rutas estructuradas, usamos el primero para completar fechas/IATA/millas
    if (not routes) and isinstance(deal.get("itineraries"), list) and deal["itineraries"]:
        first_it = deal["itineraries"][0]
        base_date_out = first_it.get("departure_date") or base_date_out
        base_date_in = first_it.get("return_date") or base_date_in
        base_origin_iata = first_it.get("origin_iata") or base_origin_iata
        base_dest_iata = first_it.get("destination_iata") or base_dest_iata
        base_booking_url = first_it.get("booking_url") or base_booking_url
        if base_miles is None and first_it.get("miles") is not None:
            base_miles = first_it.get("miles")
        if base_flight_duration_minutes is None and first_it.get("flight_duration_minutes") is not None:
            base_flight_duration_minutes = first_it.get("flight_duration_minutes")
        if base_flight_duration_display is None and first_it.get("flight_duration_display") is not None:
            base_flight_duration_display = first_it.get("flight_duration_display")
        if not base_date_range and base_date_out and base_date_in:
            base_date_range = f"{base_date_out} – {base_date_in}"

    if isinstance(routes, list) and routes:
        for r in routes:
            if not isinstance(r, dict):
                continue
            origin_city = r.get("origin") or deal.get("origin")
            dest_city = r.get("destination") or deal.get("destination")
            price_route = r.get("price_min") or base_price

            route_origin_iata = r.get("origin_iata") or base_origin_iata
            route_dest_iata = r.get("destination_iata") or base_dest_iata

            origin_label = _fmt_place(origin_city, route_origin_iata)
            dest_label = _fmt_place(dest_city, route_dest_iata)
            flight_name = None
            if origin_label and dest_label:
                flight_name = f"{origin_label} → {dest_label}"
            elif origin_label:
                flight_name = origin_label
            elif dest_label:
                flight_name = dest_label

            rows.append(
                {
                    "title": base_title,
                    "price": price_route,
                    "link": base_link,
                    # Intentamos usar booking_url específica de la ruta; si no,
                    # caemos a la principal del post.
                    "booking_url": r.get("booking_url") or base_booking_url,
                    "currency": base_currency,
                    "image": base_image,
                    "cabin_baggage": base_cabin_baggage,
                    "aircraft": base_aircraft,
                    "airline": base_airline,
                    "origin": origin_city,
                    "destination": dest_city,
                    "miles": base_miles,
                    "flight_duration_minutes": base_flight_duration_minutes,
                    "flight_duration_display": base_flight_duration_display,
                    "expires_in": base_expires,
                    "date_range": base_date_range,
                    "date_out": base_date_out,
                    "date_in": base_date_in,
                    "cabin_class": base_cabin_class,
                    "one_way": base_oneway,
                    "flight": flight_name,
                    "origin_iata": route_origin_iata,
                    "destination_iata": route_dest_iata,
                    "scoring": base_scoring,
                    "llm_enriched": base_llm_enriched,
                    "llm_enriched_fields": base_llm_fields,
                    "llm_enrichment_version": base_llm_version,
                }
            )
    else:
        # Sin rutas explícitas, una sola fila basada en origin/destination del deal
        origin_city = deal.get("origin")
        dest_city = deal.get("destination")
        origin_label = _fmt_place(origin_city, base_origin_iata)
        dest_label = _fmt_place(dest_city, base_dest_iata)
        flight_name = None
        if origin_label and dest_label:
            flight_name = f"{origin_label} → {dest_label}"
        elif origin_label:
            flight_name = origin_label
        elif dest_label:
            flight_name = dest_label

        rows.append(
            {
                "title": base_title,
                "price": base_price,
                "link": base_link,
                "booking_url": base_booking_url,
                "currency": base_currency,
                "image": base_image,
                "cabin_baggage": base_cabin_baggage,
                "aircraft": base_aircraft,
                "airline": base_airline,
                "origin": origin_city,
                "destination": dest_city,
                "miles": base_miles,
                "flight_duration_minutes": base_flight_duration_minutes,
                "flight_duration_display": base_flight_duration_display,
                "expires_in": base_expires,
                "date_range": base_date_range,
                "date_out": base_date_out,
                "date_in": base_date_in,
                "cabin_class": base_cabin_class,
                "one_way": base_oneway,
                "flight": flight_name,
                "origin_iata": base_origin_iata,
                "destination_iata": base_dest_iata,
                "scoring": base_scoring,
                "llm_enriched": base_llm_enriched,
                "llm_enriched_fields": base_llm_fields,
                "llm_enrichment_version": base_llm_version,
            }
        )

    return rows


def main() -> None:
    load_dotenv(find_dotenv())

    if len(sys.argv) > 1:
        dump_rel = sys.argv[1]
    else:
        dump_rel = DEFAULT_DUMP

    backend_dir = Path(__file__).resolve().parents[1]
    dump_path = backend_dir / "html_dumps" / dump_rel
    if not dump_path.exists():
        # Backward compatible: allow old location backend/scripts/html_dumps
        dump_path = Path(__file__).resolve().parent / "html_dumps" / dump_rel
    html = dump_path.read_text(encoding="utf-8")
    url = infer_url_from_html(html)

    deal: Dict[str, Any] = _parse_secretflying_html(html, url)

    # Enriquecer opcionalmente con OpenAI si está configurado y permitido por env
    enrich_default = os.getenv("DEALS_ENRICH_DEFAULT", "true").strip().lower() in {"1", "true", "yes", "on"}
    enrich_flag = os.getenv("DUMP_ENRICH", "").strip().lower()
    if enrich_flag:
        enrich = enrich_flag in {"1", "true", "yes", "on"}
    else:
        enrich = enrich_default

    if enrich:
        deal = enrich_deals_batch([deal], max_items=1)[0]

    # Normalizar metadatos LLM y campos derivables igual que el pipeline
    deal = {**deal, **_extract_llm_meta(deal)}
    deal = _normalize_deal_fields(deal)

    # Scoring con benchmarks de Amadeus para tener puntuación y millas derivadas si faltan
    benchmarks = _load_amadeus_benchmarks()
    scored_list = score_deals([deal], benchmarks=benchmarks)
    deal = scored_list[0] if scored_list else deal

    rows = build_secretflying_rows_from_deal(deal)

    print("Rows to insert in deals (mirror):")
    print(json.dumps(rows, indent=2, ensure_ascii=False))

    # Guardamos directamente en deals (tabla agregada), marcando el source
    deals_all_rows = []
    for r in rows:
        deals_all_rows.append(
            {
                "title": r.get("title"),
                "price": r.get("price"),
                "currency": r.get("currency"),
                "link": r.get("link"),
                "booking_url": r.get("booking_url"),
                "origin": r.get("origin"),
                "destination": r.get("destination"),
                "origin_iata": r.get("origin_iata"),
                "destination_iata": r.get("destination_iata"),
                "date_out": r.get("date_out"),
                "date_in": r.get("date_in"),
                "date_range": r.get("date_range"),
                "cabin_class": r.get("cabin_class"),
                "one_way": r.get("one_way"),
                "flight": r.get("flight"),
                "flight_duration_minutes": r.get("flight_duration_minutes"),
                "flight_duration_display": r.get("flight_duration_display"),
                "miles": r.get("miles"),
                "airline": r.get("airline"),
                "source": "secretflying",
                "scoring": r.get("scoring"),
                "llm_enriched": r.get("llm_enriched"),
                "llm_enriched_fields": r.get("llm_enriched_fields"),
                "llm_enrichment_version": r.get("llm_enrichment_version"),
            }
        )

    result_all = save_deals("deals", deals_all_rows)
    print("\nSupabase deals mirror result:", json.dumps(result_all, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
