"""Utilities for estimating flown distance in miles between airports.

We use the `airportsdata` package (IATA dataset) and a simple
haversine implementation to compute great-circle distances.
"""

from __future__ import annotations

import math
import re
from typing import Optional

try:  # airportsdata is optional at runtime; degrade gracefully if missing.
    import airportsdata  # type: ignore
    _airports = airportsdata.load("IATA")
except Exception:  # pragma: no cover - defensive fallback
    airportsdata = None  # type: ignore
    _airports = {}


def great_circle_miles(origin_iata: str | None, dest_iata: str | None) -> Optional[int]:
    """Return great-circle distance in miles between two IATA codes.

    This is the *geographic* distance, not what a specific airline
    program will credit. It is mainly used as a base for deriving an
    approximate "program miles" value.

    If any code is missing or unknown in the airports database, returns None.
    """

    if not origin_iata or not dest_iata or not _airports:
        return None

    o = _airports.get(origin_iata.upper())
    d = _airports.get(dest_iata.upper())
    if not o or not d:
        return None

    lat1 = math.radians(o["lat"])
    lon1 = math.radians(o["lon"])
    lat2 = math.radians(d["lat"])
    lon2 = math.radians(d["lon"])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Earth radius ~ 3959 miles
    distance_miles = 3959 * c
    return int(round(distance_miles))


def approximate_program_miles(distance_miles: int | None) -> Optional[int]:
    """Return a smoothed, program-style miles estimate from distance.

    This does *not* try to match any specific airline chart. It just
    gives a ballpark similar to what many programs would credit for
    economy, rounding to the nearest 100 and enforcing a small minimum.
    """

    if distance_miles is None or distance_miles <= 0:
        return None

    # Simple heuristic: floor to minimum 500 and round to nearest 100.
    base = max(distance_miles, 500)
    return int(round(base / 100.0) * 100)


# ------------------------------
# Miles program selection
# ------------------------------


def _norm_airline_name(airline_name: str | None) -> str:
    return (airline_name or "").strip().lower()


def guess_alliance(airline_name: str | None) -> Optional[str]:
    """Best-effort alliance guess from airline name.

    Returns one of: 'star', 'skyteam', 'oneworld', or None.

    This is intentionally heuristic (no external live data), used only to
    constrain which programs we display.
    """

    raw = (airline_name or "").strip()
    a = _norm_airline_name(raw)
    if not a:
        return None

    # If we only have an airline code (IATA 2-letter or ICAO 3-letter),
    # use a small built-in mapping for the airlines we commonly see.
    # This keeps the system deterministic (no live lookups) while fixing
    # cases like airline="LH" where we still want Miles&More.
    code = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    if code and code.isalnum() and len(code) in {2, 3}:
        code_map: dict[str, str] = {
            # Star Alliance / Miles&More-heavy carriers
            "LH": "star",  # Lufthansa
            "DLH": "star",
            "LX": "star",  # SWISS
            "SWR": "star",
            "OS": "star",  # Austrian
            "AUA": "star",
            "SN": "star",  # Brussels
            "BEL": "star",
            "EW": "star",  # Eurowings
            "EWG": "star",
            "4Y": "star",  # Discover
            "OCN": "star",
            "WK": "star",  # Edelweiss
            "EDW": "star",

            # SkyTeam / Flying Blue
            "KL": "skyteam",  # KLM
            "KLM": "skyteam",
            "AF": "skyteam",  # Air France
            "AFR": "skyteam",
            "HV": "skyteam",  # Transavia
            "TRA": "skyteam",
        }
        mapped = code_map.get(code)
        if mapped:
            return mapped

    star = [
        "lufthansa",
        "swiss",
        "austrian",
        "brussels",
        "discover",
        "eurowings",
        "edelweiss",
        "air dolomiti",
        "lot",
        "tap",
        "turkish",
        "air china",
        "air india",
        "singapore airlines",
        "thai",
        "united",
        "air canada",
        "ana",
    ]
    if any(k in a for k in star):
        return "star"

    skyteam = [
        "klm",
        "air france",
        "transavia",
        "delta",
        "aeromexico",
        "korean air",
        "china eastern",
        "china airlines",
        "garuda",
        "saudia",
    ]
    if any(k in a for k in skyteam):
        return "skyteam"

    oneworld = [
        "british airways",
        "iberia",
        "finnair",
        "american airlines",
        "qatar",
        "cathay",
        "japan airlines",
        "qantas",
        "alaska airlines",
        "royal jordanian",
    ]
    if any(k in a for k in oneworld):
        return "oneworld"

    return None


def guess_priority_programs(airline_name: str | None) -> list[str]:
    """Programs that must win when applicable.

    Business rule: if a flight can be attributed to Miles&More or Flying Blue,
    it should always be attributed to exactly one of those two.
    """

    alliance = guess_alliance(airline_name)
    if alliance == "star":
        return ["Miles&More"]
    if alliance == "skyteam":
        return ["Flying Blue"]
    return []


def eligible_programs_for_airline(airline_name: str | None) -> list[str]:
    """Return a list of programs we consider *valid options* for this airline."""

    priority = guess_priority_programs(airline_name)
    if priority:
        return priority

    alliance = guess_alliance(airline_name)
    if alliance == "star":
        return ["Miles&More", "MileagePlus", "Aeroplan"]
    if alliance == "skyteam":
        return ["Flying Blue", "SkyMiles"]
    if alliance == "oneworld":
        return ["Avios", "AAdvantage", "Mileage Plan"]

    # Unknown airline: we avoid guessing a program.
    return []


def estimate_miles_for_program(distance_miles: int, program: str) -> Optional[int]:
    """Conservative, deterministic estimate for credited miles.

    This is a heuristic; it does not implement real earning charts.
    It exists to pick a single program consistently.
    """

    if not distance_miles or distance_miles <= 0:
        return None

    base = max(int(round(distance_miles)), 500)
    p = (program or "").strip().lower()

    # Factors are intentionally conservative.
    factors: dict[str, float] = {
        "miles&more": 1.0,
        "flying blue": 0.75,
        "mileageplus": 1.0,
        "aeroplan": 1.0,
        "avios": 0.9,
        "aadvantage": 1.0,
        "mileage plan": 1.0,
        "skymiles": 0.75,
    }

    # Normalize a few aliases.
    if "miles & more" in p:
        p = "miles&more"
    if "flyingblue" in p:
        p = "flying blue"

    factor = factors.get(p)
    if factor is None:
        return None

    est = int(round(base * factor / 100.0) * 100)
    return max(est, 500)


def _is_business_cabin(cabin_class: str | None) -> bool:
    c = (cabin_class or "").strip().upper()
    return c in {"BUSINESS", "C", "J", "D", "Z", "R"}


def _is_first_cabin(cabin_class: str | None) -> bool:
    c = (cabin_class or "").strip().upper()
    return c in {"FIRST", "F"}


def estimate_credited_miles_for_program(
    distance_miles: int,
    program: str,
    cabin_class: str | None = None,
    roundtrip: bool | None = None,
) -> Optional[int]:
    """Heuristic credited-miles estimate including trip + cabin multipliers.

    Purpose: produce a user-facing number that is less misleading than
    a pure one-way distance for business class roundtrips.
    """

    base = estimate_miles_for_program(distance_miles, program)
    if base is None:
        return None

    trip_mult = 2.0 if roundtrip is True else 1.0
    if _is_first_cabin(cabin_class):
        cabin_mult = 2.0
    elif _is_business_cabin(cabin_class):
        cabin_mult = 1.5
    else:
        cabin_mult = 1.0

    est = int(round((base * trip_mult * cabin_mult) / 100.0) * 100)
    return max(est, 500)


def choose_best_program_for_deal(
    distance_miles: int,
    airline_name: str | None,
    cabin_class: str | None = None,
    roundtrip: bool | None = None,
) -> tuple[Optional[str], Optional[int]]:
    """Pick a single eligible program and compute miles with deal context."""

    programs = eligible_programs_for_airline(airline_name)
    if not programs:
        return None, None

    candidates: list[tuple[int, str]] = []
    for prog in programs:
        est = estimate_credited_miles_for_program(distance_miles, prog, cabin_class=cabin_class, roundtrip=roundtrip)
        if isinstance(est, int) and est > 0:
            candidates.append((est, prog))

    if not candidates:
        return None, None

    preference = {"Miles&More": 3, "Flying Blue": 2}
    best_est, best_prog = max(candidates, key=lambda x: (x[0], preference.get(x[1], 0), x[1]))
    return best_prog, best_est


def _fmt_miles_apostrophe(value: int) -> str:
    return f"{int(value):,}".replace(",", "'")


def choose_best_program(distance_miles: int, airline_name: str | None) -> tuple[Optional[str], Optional[int]]:
    programs = eligible_programs_for_airline(airline_name)
    if not programs:
        return None, None

    candidates: list[tuple[int, str]] = []
    for prog in programs:
        est = estimate_miles_for_program(distance_miles, prog)
        if isinstance(est, int) and est > 0:
            candidates.append((est, prog))

    if not candidates:
        return None, None

    # Max miles; tie-break by stable preference order.
    preference = {"Miles&More": 3, "Flying Blue": 2}
    best_est, best_prog = max(
        candidates,
        key=lambda x: (x[0], preference.get(x[1], 0), x[1]),
    )
    return best_prog, best_est


_MILES_NUM_RE = re.compile(r"(?P<num>\d[\d'.,\s]{0,12}\d|\d)")


def _parse_miles_int(text: str) -> Optional[int]:
    if not text:
        return None
    m = _MILES_NUM_RE.search(text)
    if not m:
        return None
    raw = m.group("num")
    cleaned = re.sub(r"[^0-9]", "", raw)
    try:
        val = int(cleaned)
        return val if val > 0 else None
    except Exception:
        return None


def filter_miles_programs_display(miles_text: str | None, airline_name: str | None) -> Optional[str]:
    """Reduce a multi-program display string to one *valid* program.

    If the airline maps to Miles&More or Flying Blue, we keep exactly that one.
    Otherwise, we keep the segment with the highest miles among eligible programs.
    If we cannot infer eligibility, returns the original string.
    """

    text = (miles_text or "").strip()
    if not text:
        return None

    eligible = eligible_programs_for_airline(airline_name)
    if not eligible:
        return text

    def _norm_token(s: str) -> str:
        # Normalize for fuzzy matching: remove non-alphanumerics and lowercase.
        return re.sub(r"[^a-z0-9]", "", (s or "").lower())

    # Split on typical separators used in multi-program outputs.
    # Avoid commas because they often appear inside numbers.
    parts = [
        p.strip()
        for p in re.split(r"(?:\s*/\s*|\s*\|\s*|\s*;\s*|\n+)", text)
        if p.strip()
    ]
    if len(parts) <= 1:
        return text

    eligible_l = {_norm_token(p): p for p in eligible}
    matched: list[tuple[int, str]] = []
    for part in parts:
        pl = _norm_token(part)
        prog = None
        for key_l, proper in eligible_l.items():
            if key_l and key_l in pl:
                prog = proper
                break
        if not prog:
            continue
        miles_i = _parse_miles_int(part)
        if miles_i is None:
            # If we can't parse miles, keep as low-priority.
            miles_i = 0
        matched.append((miles_i, part))

    if not matched:
        # If nothing matches, do not drop information.
        return text

    # Keep highest miles segment.
    best = max(matched, key=lambda x: x[0])[1]
    return best.strip() or text

