#!/usr/bin/env python
"""Rellenar huecos con Amadeus en base a patterns.json (versión limpia).

Uso (desde la raíz del repo):

    python -m backend.scripts.fill_amadeus_gaps_from_patterns_v2 \
        --origin MAD --max-calls 30

"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv, find_dotenv

THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

from scoring.amadeus_api import get_flight_offers  # type: ignore  # noqa: E402
from scoring.scoring import get_best_amadeus_flights, _parse_iso8601_duration_to_minutes  # type: ignore  # noqa: E402
from scoring.miles_utils import great_circle_miles, approximate_program_miles  # type: ignore  # noqa: E402
from services.deals_pipeline import (
    score_deals,
    _load_amadeus_benchmarks,
    _resolve_city_name,
    _resolve_airline_name,
    _resolve_aircraft_model,
    _extract_llm_meta,
)
from services.deals_enrichment import enrich_deals_batch  # type: ignore  # noqa: E402
from database.supabase_db import save_deals, _client  # type: ignore  # noqa: E402


WEEKDAY_MAP = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


def _load_patterns() -> Dict[str, Any]:
    patterns_path = REPO_ROOT / "patterns.json"
    if not patterns_path.exists():
        raise SystemExit(f"patterns.json not found at {patterns_path}")
    with patterns_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _iso_date(d: date) -> str:
    return d.isoformat()


def _compute_future_months(today: date, months_ahead: int) -> Dict[int, int]:
    """Return mapping {month -> year} for the next `months_ahead` months.

    This is used to restrict Amadeus queries to a sliding window (e.g. next 4
    months) while still handling year boundaries (Dec -> Jan).
    Each calendar month appears at most once in this window, so month -> year
    is safe.
    """

    if months_ahead <= 0:
        return {}

    month_to_year: Dict[int, int] = {}
    year = today.year
    month = today.month

    for i in range(months_ahead):
        mm = month + i
        yy = year + (mm - 1) // 12
        mm = ((mm - 1) % 12) + 1
        month_to_year[mm] = yy

    return month_to_year


def _first_valid_departure(year: int, month: int, weekday_name: str, today: date) -> Optional[date]:
    if month < today.month and year == today.year:
        return None

    weekday = WEEKDAY_MAP.get(weekday_name, 0)
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)

    if d < today and year == today.year and month == today.month:
        while d < today and d.month == month:
            d += timedelta(days=7)
        if d.month != month:
            return None
    return d


def _load_coverage_by_route_month(origin_iata: str) -> Set[Tuple[str, int]]:
    """Return (destination_iata, month) pairs already covered in deals.

    En el esquema actual solo existe la tabla agregada `deals`. Consideramos
    cubiertos todos los pares origen/destino/mes que ya tengan al menos un
    registro en dicha tabla (sea de Travel-Dealz, SecretFlying o Amadeus).
    """

    covered: Set[Tuple[str, int]] = set()
    if not _client:
        return covered

    try:
        rsp = (
            _client.table("deals")
            .select("origin_iata,destination_iata,date_out")
            .eq("origin_iata", origin_iata)
            .execute()
        )
    except Exception as e:
        print(f"[fill_amadeus_gaps_v2] Warning: could not query deals: {e}")
        return covered

    rows = getattr(rsp, "data", []) or []
    for row in rows:
        dest = str(row.get("destination_iata") or "").upper()
        date_out = row.get("date_out")
        if not dest or not date_out:
            continue
        month: Optional[int] = None
        try:
            dt = datetime.fromisoformat(str(date_out))
            month = dt.month
        except Exception:
            try:
                parts = str(date_out).split("-")
                if len(parts) >= 2:
                    month = int(parts[1])
            except Exception:
                month = None
        if month is None:
            continue
        covered.add((dest, month))

    return covered


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Rellenar huecos de rutas/mes para Amadeus usando patterns.json "
            "y evitando destinos/mes ya cubiertos en la tabla agregada deals."
        ),
    )
    parser.add_argument("--origin", required=True, help="Código IATA de origen (por ejemplo MAD, ZRH)")
    parser.add_argument(
        "--max-calls",
        type=int,
        default=50,
        help="Máximo de llamadas a Amadeus en esta ejecución (para limitar créditos)",
    )
    parser.add_argument(
        "--months-ahead",
        type=int,
        default=4,
        help=(
            "Número de meses hacia adelante (incluyendo el actual) en los que "
            "buscar huecos. Por defecto solo los próximos 4 meses."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "No persistir las filas calculadas en Supabase; solo mostrar "
            "un resumen en consola. Útil para modos de previsualización."
        ),
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help=(
            "Ruta opcional para volcar las filas calculadas (JSON). "
            "Se escribe tanto en dry-run como en persist. "
            "Si es relativa, se resuelve desde la raíz del repo."
        ),
    )
    args = parser.parse_args()

    assume_baggage = os.getenv("AMADEUS_ASSUME_BAGGAGE", "false").strip().lower() in {"1", "true", "yes", "on"}
    deterministic_enabled = os.getenv("DEALS_DETERMINISTIC_ENRICH", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    origin = args.origin.strip().upper()
    today = date.today()

    # Restringimos la búsqueda a una ventana deslizante de meses futuros
    # (por defecto, próximos 4 meses), manejando cambio de año.
    month_to_year = _compute_future_months(today, args.months_ahead)

    patterns = _load_patterns()
    covered = _load_coverage_by_route_month(origin)

    print(
        f"[fill_amadeus_gaps_v2] origin={origin}, today={today.isoformat()}, "
        f"months_window={sorted(month_to_year.items())}, covered_pairs={len(covered)}",
    )

    rows_to_save: List[Dict[str, Any]] = []
    calls_done = 0

    for dest_iata, cfg in patterns.items():
        dest = str(dest_iata).upper()
        try:
            cheap_months = cfg.get("cheap_months") or []
            depart_weekdays = cfg.get("depart_weekdays") or ["Tue"]
            trip_lengths = cfg.get("trip_lengths") or [7]
        except Exception:
            continue

        if not cheap_months:
            continue

        depart_weekday = str(depart_weekdays[0])
        trip_length = int(trip_lengths[0])

        for month in cheap_months:
            try:
                month_int = int(month)
            except Exception:
                continue

            # Solo consideramos meses dentro de la ventana futura definida
            # por --months-ahead (por defecto, próximos 4 meses).
            year_for_month = month_to_year.get(month_int)
            if year_for_month is None:
                continue

            if (dest, month_int) in covered:
                continue

            dep_date = _first_valid_departure(year_for_month, month_int, depart_weekday, today)
            if not dep_date:
                continue

            ret_date = dep_date + timedelta(days=trip_length)

            print(
                f"[fill_amadeus_gaps_v2] GAP {origin}->{dest} month={month_int}; "
                f"querying Amadeus for {dep_date}–{ret_date}",
            )

            try:
                offers = get_flight_offers(
                    origin_location_code=origin,
                    destination_location_code=dest,
                    departure_date=_iso_date(dep_date),
                    returnDate=_iso_date(ret_date),
                    duration=str(trip_length),
                    adults=1,
                )
            except Exception as e:
                print(f"[fill_amadeus_gaps_v2] Amadeus error for {origin}->{dest}: {e!r}")
                continue

            if not isinstance(offers, list) or not offers:
                print(f"[fill_amadeus_gaps_v2] No offers for {origin}->{dest}")
                continue

            ranked = get_best_amadeus_flights(offers, top_n=1)
            if not ranked:
                print(f"[fill_amadeus_gaps_v2] Could not rank offers for {origin}->{dest}")
                continue

            best = ranked[0]
            offer = best.get("offer", {})
            price_total = best.get("price")

            price_info = offer.get("price") or {}
            currency = price_info.get("currency") or "EUR"

            cabin_class: Optional[str] = None
            airline: Optional[str] = None
            aircraft: Optional[str] = None
            reported_duration_minutes: Optional[int] = None
            final_duration_minutes: Optional[int] = None
            included_checked_qty: Optional[int] = None
            included_cabin_qty: Optional[int] = None

            try:
                tps = offer.get("travelerPricings") or []
                if tps:
                    fd = (tps[0].get("fareDetailsBySegment") or [])
                    if fd:
                        cabin_class = fd[0].get("cabin")

                        def _qty(val: Any) -> Optional[int]:
                            if val is None:
                                return None
                            if isinstance(val, bool):
                                return int(val)
                            if isinstance(val, (int, float)):
                                return int(val)
                            if isinstance(val, str):
                                s = val.strip()
                                if s.isdigit():
                                    return int(s)
                            return None

                        checked = (fd[0].get("includedCheckedBags") or {}).get("quantity")
                        cabin = (fd[0].get("includedCabinBags") or {}).get("quantity")
                        included_checked_qty = _qty(checked)
                        included_cabin_qty = _qty(cabin)

                itins = offer.get("itineraries") or []
                if itins:
                    # Derivar aerolínea/avión del primer segmento
                    segments = itins[0].get("segments") or []
                    if segments:
                        seg0 = segments[0]
                        airline = seg0.get("carrierCode") or seg0.get("marketingCarrier")
                        ac_info = seg0.get("aircraft") or {}
                        if isinstance(ac_info, dict):
                            aircraft = ac_info.get("code") or ac_info.get("name")

                    # Duración: usamos promedio por tramo (outbound/inbound) en lugar de suma total
                    durations: List[int] = []
                    for itin in itins:
                        dur_str = itin.get("duration")
                        if not dur_str:
                            continue
                        try:
                            mins = _parse_iso8601_duration_to_minutes(str(dur_str))
                        except Exception:
                            mins = None
                        if isinstance(mins, (int, float)) and mins > 0:
                            durations.append(int(mins))
                    if durations:
                        # Si hay ida y vuelta, mostramos duración típica de un tramo (promedio)
                        reported_duration_minutes = int(round(sum(durations) / len(durations)))

                    # Determinar si es oneway: Amadeus devuelve 1 itinerario para ida, 2 para RT
                    if len(itins) == 1:
                        one_way = True
                    elif len(itins) >= 2:
                        one_way = False
            except Exception:
                pass

            origin_name = _resolve_city_name(None, origin) or origin
            dest_name = _resolve_city_name(None, dest) or dest
            airline_name = _resolve_airline_name(airline, airline)
            aircraft_model = _resolve_aircraft_model(aircraft, aircraft)

            title = f"{origin_name} ({origin}) → {dest_name} ({dest}) ({_iso_date(dep_date)}–{_iso_date(ret_date)})"
            flight_label = f"{origin_name} ({origin}) → {dest_name} ({dest})"
            date_range = f"{_iso_date(dep_date)}–{_iso_date(ret_date)}"

            # Amadeus does NOT provide frequent-flyer miles. Historically we computed a
            # distance-based approximation. Keep it behind a deterministic feature flag
            # so "raw" modes don't persist guessed data.
            miles_val: Optional[int] = None
            est_oneway_minutes: Optional[int] = None
            gc_miles: Optional[int] = None
            if deterministic_enabled:
                try:
                    gc_miles = great_circle_miles(origin, dest)
                except Exception:
                    gc_miles = None
                if gc_miles is not None:
                    miles_val = approximate_program_miles(gc_miles)
                    # Estimación de duración a ~500 mph para detectar RT infladas
                    est_oneway_minutes = int(round((gc_miles / 500.0) * 60)) if gc_miles > 0 else None

            # Ajustar duración: si Amadeus reporta algo muy alto (p.ej. RT completa), usa estimación de tramo
            if reported_duration_minutes and est_oneway_minutes:
                if len(itins) >= 2 and reported_duration_minutes > 1.6 * est_oneway_minutes:
                    final_duration_minutes = est_oneway_minutes
                else:
                    final_duration_minutes = reported_duration_minutes
            elif reported_duration_minutes:
                final_duration_minutes = reported_duration_minutes
            elif deterministic_enabled and est_oneway_minutes:
                final_duration_minutes = est_oneway_minutes

            # Formatear duración total (si la pudimos calcular) para mostrar
            # algo legible en snippets/newsletter.
            duration_display: Optional[str] = None
            if final_duration_minutes and final_duration_minutes > 0:
                h, m = divmod(int(final_duration_minutes), 60)
                if h and m:
                    duration_display = f"{h}h {m}m"
                elif h:
                    duration_display = f"{h}h"
                else:
                    duration_display = f"{m}m"

            row: Dict[str, Any] = {
                "title": title,
                "price": price_total,
                "currency": currency,
                "link": None,
                "booking_url": None,
                "origin": origin_name,
                "destination": dest_name,
                "origin_iata": origin,
                "destination_iata": dest,
                "date_out": _iso_date(dep_date),
                "date_in": _iso_date(ret_date),
                "cabin_class": cabin_class,
                "airline": airline_name or airline,
                "aircraft": aircraft_model or aircraft,
                "one_way": one_way,
                "flight": flight_label,
                "date_range": date_range,
                 "miles": miles_val,
                 "flight_duration_minutes": final_duration_minutes,
                 "flight_duration_display": duration_display,
                "source": "amadeus",
            }

            # Baggage (best-effort; based on Amadeus fareDetailsBySegment)
            # We store both structured fields and a readable display string so
            # the newsletter template can show a clean line.
            if included_cabin_qty and included_cabin_qty > 0 and included_checked_qty and included_checked_qty > 0:
                row["baggage_allowance_display"] = f"{included_cabin_qty}×8 kg + {included_checked_qty}×23 kg"
                row["cabin_baggage"] = f"{included_cabin_qty}×8 kg"
                row["baggage_included"] = True
                row["baggage_pieces_included"] = included_checked_qty
                row["baggage_allowance_kg"] = 23
            elif included_cabin_qty and included_cabin_qty > 0:
                row["baggage_allowance_display"] = f"{included_cabin_qty}×8 kg"
                row["cabin_baggage"] = f"{included_cabin_qty}×8 kg"
            elif included_checked_qty and included_checked_qty > 0:
                row["baggage_allowance_display"] = f"{included_checked_qty}×23 kg"
                row["baggage_included"] = True
                row["baggage_pieces_included"] = included_checked_qty
                row["baggage_allowance_kg"] = 23

            # If Amadeus doesn't provide baggage, optionally assume a reasonable default
            # so reference/demo HTML always shows something.
            if assume_baggage and not row.get("baggage_allowance_display"):
                cc = str(row.get("cabin_class") or "").strip().upper()
                if cc in {"BUSINESS", "J", "C"}:
                    row["cabin_baggage"] = row.get("cabin_baggage") or "1×8 kg"
                    row["baggage_included"] = True
                    row["baggage_pieces_included"] = 2
                    row["baggage_allowance_kg"] = 32
                    row["baggage_allowance_display"] = "1×8 kg + 2×32 kg"
                else:
                    # ECONOMY: assume hand baggage only (Economy Light)
                    row["cabin_baggage"] = row.get("cabin_baggage") or "1×8 kg"
                    row["baggage_included"] = False
                    row["baggage_pieces_included"] = 0
                    row["baggage_allowance_kg"] = None
                    row["baggage_allowance_display"] = "1×8 kg"

            rows_to_save.append(row)
            covered.add((dest, month_int))
            calls_done += 1

            if calls_done >= args.max_calls:
                print(
                    f"[fill_amadeus_gaps_v2] Reached max-calls={args.max_calls}; "
                    "stopping further Amadeus queries.",
                )
                break

        if calls_done >= args.max_calls:
            break

    if not rows_to_save:
        print("[fill_amadeus_gaps_v2] No new rows to save; nothing to do.")
        return

    # Calcular scoring para estos deals Amadeus usando la misma lógica que el
    # pipeline, de modo que la columna `scoring` nunca quede a NULL.
    benchmarks = _load_amadeus_benchmarks()
    deals_for_scoring: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows_to_save):
        deals_for_scoring.append(
            {
                "id": idx,
                "title": row.get("title"),
                "price": row.get("price"),
                "currency": row.get("currency"),
                "origin_iata": row.get("origin_iata"),
                "destination_iata": row.get("destination_iata"),
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

    for idx, row in enumerate(rows_to_save):
        if idx in scores_by_id and scores_by_id[idx] is not None:
            row["scoring"] = scores_by_id[idx]

    # Enriquecimiento LLM opcional (igual que en pipeline/export): si
    # DEALS_ENRICH_DEFAULT=true (por defecto true), aplicar OpenAI a todas
    # las filas calculadas. Luego normalizamos los metadatos LLM para evitar
    # nulos.
    enrich_default = os.getenv("DEALS_ENRICH_DEFAULT", "true").strip().lower() in {"1", "true", "yes", "on"}
    if enrich_default:
        # Avoid printing secrets; only show whether the key exists.
        print(
            "[fill_amadeus_gaps_v2] LLM env: "
            f"DEALS_LLM_ENRICH_MILES={os.getenv('DEALS_LLM_ENRICH_MILES')!s} "
            f"DEALS_LLM_ENRICH_AMADEUS={os.getenv('DEALS_LLM_ENRICH_AMADEUS')!s} "
            f"OPENAI_API_KEY_set={bool(os.getenv('OPENAI_API_KEY'))}"
        )
        rows_to_save = enrich_deals_batch(rows_to_save, max_items=len(rows_to_save))

    cleaned_rows: List[Dict[str, Any]] = []
    for row in rows_to_save:
        row_clean = dict(row)
        row_clean.pop("llm_enriched_fallback", None)
        row_clean.update(_extract_llm_meta(row_clean))
        cleaned_rows.append(row_clean)
    rows_to_save = cleaned_rows

    # Exportar JSON si se pidió (útil para renderizar snippets sin persistencia).
    if getattr(args, "output_json", None):
        try:
            out_path = Path(str(args.output_json)).expanduser()
            if not out_path.is_absolute():
                out_path = (REPO_ROOT / out_path).resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(rows_to_save, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[fill_amadeus_gaps_v2] Wrote output JSON to: {out_path}")
        except Exception as e:
            print(f"[fill_amadeus_gaps_v2] Warning: could not write --output-json: {e}")

    # En modo dry-run no persistimos en Supabase; solo mostramos un resumen
    # de cuántas filas se habrían escrito. Esto permite probar patrones y
    # consumo de Amadeus sin tocar la base de datos.
    if getattr(args, "dry_run", False):
        print(
            f"[fill_amadeus_gaps_v2] Dry-run enabled; would save {len(rows_to_save)} "
            "new rows into deals (no changes persisted).",
        )
        return

    print(
        f"[fill_amadeus_gaps_v2] Saving {len(rows_to_save)} new rows into deals...",
    )
    result = save_deals("deals", rows_to_save)
    print("[fill_amadeus_gaps_v2] Supabase result:", result)


if __name__ == "__main__":
    main()
