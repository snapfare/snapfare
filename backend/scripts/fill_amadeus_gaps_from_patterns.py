#!/usr/bin/env python
"""Rellenar huecos con Amadeus en base a patterns.json.

Uso (desde la raíz del repo):

    python -m backend.scripts.fill_amadeus_gaps_from_patterns \
        --origin MAD --max-calls 30

Comportamiento:
- Lee patterns.json del root del repo.
- Para cada destino y cada cheap_month, construye una fecha de salida
  razonable (primer weekday permitido del mes) y una vuelta con el
  primer trip_length definido.
- Calcula la "cobertura" existente mirando en:
    - deals_traveldealz
    - deals_secretflying
    - best_deals_amadeus
  filtrando por origin_iata == --origin.
- Para cada par (destino, mes) que ya tenga al menos un vuelo en
  cualquiera de esas tablas, NO llama a Amadeus.
- Para los pares (destino, mes) sin cobertura, hace UNA llamada a
  Amadeus (round-trip) y guarda el mejor vuelo en best_deals_amadeus.

Así evitas duplicar rutas/mes ya cubiertos por Travel-Dealz o
SecretFlying, y sólo gastas créditos de Amadeus en huecos reales.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv, find_dotenv

# Asegurar que backend/ está en sys.path cuando se ejecuta como módulo
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

# Imports locales después de ajustar sys.path
from scoring.amadeus_api import get_flight_offers  # type: ignore  # noqa: E402
from scoring.scoring import get_best_amadeus_flights  # type: ignore  # noqa: E402
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


def _first_valid_departure(year: int, month: int, weekday_name: str, today: date) -> Optional[date]:
    """Primer día del mes con ese weekday, no anterior a hoy.

    Si el mes es anterior al mes actual, devuelve None (no tiene sentido
    buscar en el pasado). Si el mes es el actual, elige el primer
    weekday >= hoy; para meses futuros, el primer weekday del mes.
    """

    if month < today.month and year == today.year:
        return None

    weekday = WEEKDAY_MAP.get(weekday_name, 0)
    d = date(year, month, 1)

    # Avanzar hasta el primer weekday de ese tipo
    while d.weekday() != weekday:
        d += timedelta(days=1)

    # Si es el mes actual y está en el pasado, saltar semanas de 7 días
    if d < today and year == today.year and month == today.month:
        while d < today and d.month == month:
            d += timedelta(days=7)
        if d.month != month:
            return None

    return d


def _load_coverage_by_route_month(origin_iata: str) -> Set[Tuple[str, int]]:
    """Cargar pares (dest_iata, mes) ya cubiertos en Supabase.

    Mira en deals_traveldealz, deals_secretflying y deals_amadeus,
    filtrando por origin_iata.
    """

    covered: Set[Tuple[str, int]] = set()

    if not _client:
        return covered

    tables = ["deals_traveldealz", "deals_secretflying", "deals_amadeus"]
    for table in tables:
        try:
            rsp = (
                _client.table(table)
                .select("origin_iata,destination_iata,date_out")
                .eq("origin_iata", origin_iata)
                .execute()
            )
        except Exception as e:  # pragma: no cover - robustez en runtime
            print(f"[fill_amadeus_gaps] Warning: could not query {table}: {e}")
            continue

        rows = getattr(rsp, "data", []) or []
        for row in rows:
            dest = str(row.get("destination_iata") or "").upper()
            date_out = row.get("date_out")
            if not dest or not date_out:
                continue

            # date_out viene como ISO (YYYY-MM-DD) o similar
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
            "Rellenar huecos de rutas/mes en deals_amadeus usando patterns.json "
            "y evitando destinos/mes ya cubiertos por Travel-Dealz/SecretFlying."
        ),
    )
    parser.add_argument(
        "--origin",
        required=True,
        help="Código IATA de origen (por ejemplo MAD, ZRH)",
    )
    parser.add_argument(
        "--max-calls",
        type=int,
        default=50,
        help="Máximo de llamadas a Amadeus en esta ejecución (para limitar créditos)",
    )
    args = parser.parse_args()

    origin = args.origin.strip().upper()
    today = date.today()
    year = today.year

    patterns = _load_patterns()
    covered = _load_coverage_by_route_month(origin)

    print(
        f"[fill_amadeus_gaps] origin={origin}, year={year}, today={today.isoformat()}, "
        f"covered_pairs={len(covered)}"
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

        depart_weekday = str(depart_weekdays[0])  # usamos el primero como referencia
        trip_length = int(trip_lengths[0])  # usamos el primer trip_length

        for month in cheap_months:
            try:
                month_int = int(month)
            except Exception:
                continue

            # Si ya hay cobertura para (dest, mes), pasamos al siguiente
            if (dest, month_int) in covered:
                continue

            dep_date = _first_valid_departure(year, month_int, depart_weekday, today)
            if not dep_date:
                continue

            ret_date = dep_date + timedelta(days=trip_length)

            print(
                f"[fill_amadeus_gaps] GAP detected for {origin}->{dest} month={month_int}; "
                f"querying Amadeus for {dep_date}–{ret_date}"
            )

            # Llamada a Amadeus
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
                print(f"[fill_amadeus_gaps] Amadeus error for {origin}->{dest}: {e!r}")
                continue

            if not isinstance(offers, list) or not offers:
                print(f"[fill_amadeus_gaps] No offers returned for {origin}->{dest}")
                continue

            ranked = get_best_amadeus_flights(offers, top_n=1)
            if not ranked:
                print(f"[fill_amadeus_gaps] Could not rank offers for {origin}->{dest}")
                continue

            best = ranked[0]
            offer = best.get("offer", {})
            price_total = best.get("price")

            price_info = offer.get("price") or {}
            currency = price_info.get("currency") or "EUR"

            cabin_class: Optional[str] = None
            airline: Optional[str] = None
            aircraft: Optional[str] = None
            try:
                # Cabin class from travelerPricings
                tps = offer.get("travelerPricings") or []
                if tps:
                    fd = (tps[0].get("fareDetailsBySegment") or [])
                    if fd:
                        cabin_class = fd[0].get("cabin")

                # Airline / aircraft from first segment of first itinerary
                itins = offer.get("itineraries") or []
                if itins:
                    segments = itins[0].get("segments") or []
                    if segments:
                        seg0 = segments[0]
                        airline = seg0.get("carrierCode") or seg0.get("marketingCarrier")
                        ac_info = seg0.get("aircraft") or {}
                        if isinstance(ac_info, dict):
                            aircraft = ac_info.get("code") or ac_info.get("name")
            except Exception:
                # Si algo falla, nos quedamos con lo que ya tuviéramos
                pass

            # Título y metadatos legibles para la tabla
            title = f"{origin} → {dest} ({_iso_date(dep_date)}–{_iso_date(ret_date)})"
            flight_label = f"{origin} → {dest}"
            date_range = f"{_iso_date(dep_date)}–{_iso_date(ret_date)}"

            row: Dict[str, Any] = {
                "title": title,
                "price": price_total,
                "currency": currency,
                "link": None,
                "booking_url": None,
                "origin": origin,
                "destination": dest,
                "origin_iata": origin,
                "destination_iata": dest,
                "date_out": _iso_date(dep_date),
                "date_in": _iso_date(ret_date),
                "cabin_class": cabin_class,
                "airline": airline,
                "aircraft": aircraft,
                "flight": flight_label,
                "date_range": date_range,
            }

            rows_to_save.append(row)
            covered.add((dest, month_int))
            calls_done += 1

            if calls_done >= args.max_calls:
                print(
                    f"[fill_amadeus_gaps] Reached max-calls={args.max_calls}; "
                    "stopping further Amadeus queries."
                )
                break

        if calls_done >= args.max_calls:
            break

    if not rows_to_save:
        print("[fill_amadeus_gaps] No new rows to save; nothing to do.")
        return

    print(
        f"[fill_amadeus_gaps] Saving {len(rows_to_save)} new rows into deals_amadeus..."
    )
    result = save_deals("deals_amadeus", rows_to_save)
    print("[fill_amadeus_gaps] Supabase result:", result)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""Rellenar huecos con Amadeus en base a patterns.json.

Uso (desde la raíz del repo):

    python -m backend.scripts.fill_amadeus_gaps_from_patterns \
        --origin MAD --max-calls 30

Comportamiento:
- Lee patterns.json del root del repo.
- Para cada destino y cada cheap_month, construye una fecha de salida
  razonable (primer weekday permitido del mes) y una vuelta con el
  primer trip_length definido.
- Calcula la "cobertura" existente mirando en:
    - deals_traveldealz
    - deals_secretflying
    - best_deals_amadeus
  filtrando por origin_iata == --origin.
- Para cada par (destino, mes) que ya tenga al menos un vuelo en
  cualquiera de esas tablas, NO llama a Amadeus.
- Para los pares (destino, mes) sin cobertura, hace UNA llamada a
  Amadeus (round-trip) y guarda el mejor vuelo en best_deals_amadeus.

Así evitas duplicar rutas/mes ya cubiertos por Travel-Dealz o
SecretFlying, y sólo gastas créditos de Amadeus en huecos reales.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv, find_dotenv

# Asegurar que backend/ está en sys.path cuando se ejecuta como módulo
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

load_dotenv(find_dotenv())

# Imports locales después de ajustar sys.path
from scoring.amadeus_api import get_flight_offers  # type: ignore  # noqa: E402
from scoring.scoring import get_best_amadeus_flights  # type: ignore  # noqa: E402
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


def _first_valid_departure(year: int, month: int, weekday_name: str, today: date) -> Optional[date]:
    """Primer día del mes con ese weekday, no anterior a hoy.

    Si el mes es anterior al mes actual, devuelve None (no tiene sentido
    buscar en el pasado). Si el mes es el actual, elige el primer
    weekday >= hoy; para meses futuros, el primer weekday del mes.
    """

    if month < today.month and year == today.year:
        return None

    weekday = WEEKDAY_MAP.get(weekday_name, 0)
    d = date(year, month, 1)

    # Avanzar hasta el primer weekday de ese tipo
    while d.weekday() != weekday:
        d += timedelta(days=1)

    # Si es el mes actual y está en el pasado, saltar semanas de 7 días
    if d < today and year == today.year and month == today.month:
        while d < today and d.month == month:
            d += timedelta(days=7)
        if d.month != month:
            return None

            best = ranked[0]
            offer = best.get("offer", {})
            price_total = best.get("price")

            price_info = offer.get("price") or {}
            currency = price_info.get("currency") or "EUR"

            cabin_class = None
            airline: Optional[str] = None
            aircraft: Optional[str] = None
            try:
                # Cabin class from travelerPricings
                tps = offer.get("travelerPricings") or []
                if tps:
                    fd = (tps[0].get("fareDetailsBySegment") or [])
                    if fd:
                        cabin_class = fd[0].get("cabin")

                # Airline / aircraft from first segment of first itinerary
                itins = offer.get("itineraries") or []
                if itins:
                    segments = itins[0].get("segments") or []
                    if segments:
                        seg0 = segments[0]
                        airline = seg0.get("carrierCode") or seg0.get("marketingCarrier")
                        ac_info = seg0.get("aircraft") or {}
                        if isinstance(ac_info, dict):
                            aircraft = ac_info.get("code") or ac_info.get("name")
            except Exception:
                cabin_class = cabin_class  # keep whatever we already had

            # Título y metadatos legibles para la tabla
            title = f"{origin} → {dest} ({_iso_date(dep_date)}–{_iso_date(ret_date)})"
            flight_label = f"{origin} → {dest}"
            date_range = f"{_iso_date(dep_date)}–{_iso_date(ret_date)}"

            row: Dict[str, Any] = {
                "title": title,
                "price": price_total,
                "currency": currency,
                "link": None,
                "booking_url": None,
                "origin": origin,
                "destination": dest,
                "origin_iata": origin,
                "destination_iata": dest,
                "date_out": _iso_date(dep_date),
                "date_in": _iso_date(ret_date),
                "cabin_class": cabin_class,
                "airline": airline,
                "aircraft": aircraft,
                "flight": flight_label,
                "date_range": date_range,
            }
            

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Rellenar huecos de rutas/mes en best_deals_amadeus usando patterns.json "
            "y evitando destinos/mes ya cubiertos por Travel-Dealz/SecretFlying."
        ),
    )
    parser.add_argument(
        "--origin",
        required=True,
        help="Código IATA de origen (por ejemplo MAD, ZRH)",
    )
    parser.add_argument(
        "--max-calls",
        type=int,
        default=50,
        help="Máximo de llamadas a Amadeus en esta ejecución (para limitar créditos)",
    )
    args = parser.parse_args()

    origin = args.origin.strip().upper()
    today = date.today()
    year = today.year

    patterns = _load_patterns()
    covered = _load_coverage_by_route_month(origin)

    print(
        f"[fill_amadeus_gaps] origin={origin}, year={year}, today={today.isoformat()}, "
        f"covered_pairs={len(covered)}"
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

        depart_weekday = str(depart_weekdays[0])  # usamos el primero como referencia
        trip_length = int(trip_lengths[0])  # usamos el primer trip_length

        for month in cheap_months:
            try:
                month_int = int(month)
            except Exception:
                continue

            # Si ya hay cobertura para (dest, mes), pasamos al siguiente
            if (dest, month_int) in covered:
                continue

            dep_date = _first_valid_departure(year, month_int, depart_weekday, today)
            if not dep_date:
                continue

            ret_date = dep_date + timedelta(days=trip_length)

            print(
                f"[fill_amadeus_gaps] GAP detected for {origin}->{dest} month={month_int}; "
                f"querying Amadeus for {dep_date}–{ret_date}"
            )

            # Llamada a Amadeus
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
                print(f"[fill_amadeus_gaps] Amadeus error for {origin}->{dest}: {e!r}")
                continue

            if not isinstance(offers, list) or not offers:
                print(f"[fill_amadeus_gaps] No offers returned for {origin}->{dest}")
                continue

            ranked = get_best_amadeus_flights(offers, top_n=1)
            if not ranked:
                print(f"[fill_amadeus_gaps] Could not rank offers for {origin}->{dest}")
                continue

            best = ranked[0]
            offer = best.get("offer", {})
            price_total = best.get("price")

            price_info = offer.get("price") or {}
            currency = price_info.get("currency") or "EUR"

            cabin_class = None
            try:
                tps = offer.get("travelerPricings") or []
                if tps:
                    fd = (tps[0].get("fareDetailsBySegment") or [])
                    if fd:
                        cabin_class = fd[0].get("cabin")
            except Exception:
                cabin_class = None

            title = f"{origin}  {dest} ({_iso_date(dep_date)}{_iso_date(ret_date)})"

            row: Dict[str, Any] = {
                "title": title,
                "price": price_total,
                "currency": currency,
                "link": None,
                "booking_url": None,
                "origin": origin,
                "destination": dest,
                "origin_iata": origin,
                "destination_iata": dest,
                "date_out": _iso_date(dep_date),
                "date_in": _iso_date(ret_date),
                "cabin_class": cabin_class,
            }

            rows_to_save.append(row)
            covered.add((dest, month_int))
            calls_done += 1

            if calls_done >= args.max_calls:
                print(
                    f"[fill_amadeus_gaps] Reached max-calls={args.max_calls}; "
                    "stopping further Amadeus queries."
                )
                break

        if calls_done >= args.max_calls:
            break

    if not rows_to_save:
        print("[fill_amadeus_gaps] No new rows to save; nothing to do.")
        return

    print(
        f"[fill_amadeus_gaps] Saving {len(rows_to_save)} new rows into best_deals_amadeus..."
    )
    result = save_deals("best_deals_amadeus", rows_to_save)
    print("[fill_amadeus_gaps] Supabase result:", result)


if __name__ == "__main__":
    main()
