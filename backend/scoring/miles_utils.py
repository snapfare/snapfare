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

            # Star Alliance (additional codes beyond the name-list below)
            "TK": "star",   # Turkish Airlines
            "THY": "star",
            "SQ": "star",   # Singapore Airlines
            "SIA": "star",
            "NH": "star",   # ANA
            "TG": "star",   # Thai Airways
            "AC": "star",   # Air Canada
            "UA": "star",   # United
            "CA": "star",   # Air China
            "AI": "star",   # Air India
            "LO": "star",   # LOT Polish

            # oneworld
            "BA": "oneworld",  # British Airways
            "BAW": "oneworld",
            "IB": "oneworld",   # Iberia
            "IBE": "oneworld",
            "QR": "oneworld",   # Qatar Airways
            "QTR": "oneworld",
            "CX": "oneworld",   # Cathay Pacific
            "CPA": "oneworld",
            "AA": "oneworld",   # American Airlines
            "AAL": "oneworld",
            "JL": "oneworld",   # Japan Airlines
            "JAL": "oneworld",
            "QF": "oneworld",   # Qantas
            "QFA": "oneworld",
            "AS": "oneworld",   # Alaska Airlines
            "ASA": "oneworld",
            "AY": "oneworld",   # Finnair
            "FIN": "oneworld",
            "EI": "oneworld",   # Aer Lingus
            "EIN": "oneworld",
            "RJ": "oneworld",   # Royal Jordanian
            "RJA": "oneworld",
            "MH": "oneworld",   # Malaysia Airlines
            "MAS": "oneworld",
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


# Carriers whose own FFP is the best earning program for their own flights,
# taking priority over the generic alliance program (e.g. TK → Miles&Smiles, not M&M).
_CARRIER_PRIORITY_PROGRAMS: dict[str, list[str]] = {
    # Turkish Airlines → Miles&Smiles earns 100% economy on own TK flights
    # (vs ~25-50% as Star Alliance partner in M&M)
    "TK": ["Miles&Smiles"],
    "THY": ["Miles&Smiles"],
    # Singapore Airlines → KrisFlyer earns 50%+ economy on own SQ flights
    "SQ": ["KrisFlyer"],
    "SIA": ["KrisFlyer"],
    # Etihad Airways → Etihad Guest (not in any alliance; no major alliance earns well on EY)
    "EY": ["Etihad Guest"],
    "ETD": ["Etihad Guest"],
}


def guess_priority_programs(airline_name: str | None) -> list[str]:
    """Programs that must win when applicable.

    For carriers with strong own FFPs (Turkish, Singapore, Etihad) we prefer
    their own program over the generic alliance program.
    For LH group / AF-KLM group, Miles&More / Flying Blue win.
    """
    raw = (airline_name or "").strip()
    code = re.sub(r"[^A-Za-z0-9]", "", raw).upper()

    # Check carrier-specific priority FIRST (before alliance check)
    if code and len(code) in {2, 3}:
        carrier_prog = _CARRIER_PRIORITY_PROGRAMS.get(code)
        if carrier_prog:
            return carrier_prog

    # Also check by name for Turkish / Singapore / Etihad
    a = _norm_airline_name(raw)
    if "turkish" in a or "türk" in a:
        return ["Miles&Smiles"]
    if "singapore airlines" in a:
        return ["KrisFlyer"]
    if "etihad" in a:
        return ["Etihad Guest"]

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
        # Return airline-specific Avios program name when we can identify the carrier
        avios_name = _avios_display_name(airline_name)
        return [avios_name, "AAdvantage", "Mileage Plan"]

    # Check for Emirates Skywards (non-alliance)
    a = _norm_airline_name(airline_name)
    code = re.sub(r"[^A-Za-z0-9]", "", (airline_name or "")).upper()
    if code in {"EK", "UAE"} or "emirates" in a:
        return ["Emirates Skywards"]

    # Unknown airline: we avoid guessing a program.
    return []


def _avios_display_name(airline_name: str | None) -> str:
    """Return the airline-specific Avios program display name."""
    a = _norm_airline_name(airline_name)
    code = re.sub(r"[^A-Za-z0-9]", "", (airline_name or "")).upper()
    if code in {"BA", "BAW"} or "british airways" in a:
        return "British Airways Avios"
    if code in {"IB", "IBE"} or "iberia" in a:
        return "Iberia Avios"
    if code in {"QR", "QTR"} or "qatar" in a:
        return "Qatar Avios"
    if code in {"EI", "EIN"} or "aer lingus" in a:
        return "Aer Lingus Avios"
    # Default: British Airways Avios (most widely-used Avios collection for oneworld flights)
    return "British Airways Avios"


def _normalize_program_key(program: str) -> str:
    p = (program or "").strip().lower()
    p = p.replace("miles & more", "miles&more").replace("flyingblue", "flying blue")
    # Normalize all Avios variants (British Airways Avios, Qatar Avios, etc.) → "avios"
    if "avios" in p:
        return "avios"
    # Normalize Emirates Skywards
    if "skywards" in p or "emirates" in p:
        return "emirates skywards"
    # Normalize Turkish Miles&Smiles
    if "miles&smiles" in p or "miles & smiles" in p or "milessmiles" in p:
        return "miles&smiles"
    # Normalize Singapore KrisFlyer
    if "krisflyer" in p or "kris flyer" in p:
        return "krisflyer"
    # Normalize Etihad Guest
    if "etihad guest" in p or ("etihad" in p and "guest" in p):
        return "etihad guest"
    return p


# Earning rates by (program, cabin_class) as a fraction of flown distance.
# Based on published earning charts (2024/2025) for cheapest available fare class.
# "cheapest" = Light/Basic/discounted fares — the fares we surface as deals.
_EARNING_RATES: dict[str, dict[str, float]] = {
    # Miles&More (Lufthansa Group: LH, LX, OS, SN, EW).
    # Own LH/LX flights: Light fare = 25%, Classic = 50%, Flex = 100%.
    # Deals are typically Light/cheapest fares → 25%.
    # Business C/D/J = 200%; First A/F = 300%.
    "miles&more": {
        "economy": 0.25,
        "premium economy": 0.75,
        "business": 2.00,
        "first": 3.00,
    },
    # Flying Blue (Air France / KLM).
    # Economy Light = 25%; Economy Standard = 75%; Business = 150%; La Première = 200%.
    "flying blue": {
        "economy": 0.25,
        "premium economy": 0.75,
        "business": 1.50,
        "first": 2.00,
    },
    # Avios (British Airways Executive Club; also Iberia, Qatar, Aer Lingus).
    # BA Economy Basic (G/K/L/M/N): 25%; Economy Classic: 50%;
    # Club World Business: 150%; First: 225%.
    "avios": {
        "economy": 0.25,
        "premium economy": 0.50,
        "business": 1.50,
        "first": 2.25,
    },
    # Turkish Miles&Smiles (own TK flights).
    # Uniquely generous: ALL economy fare classes earn 100% flown distance.
    # Business: 175%; (no First on TK).
    "miles&smiles": {
        "economy": 1.00,
        "premium economy": 1.00,
        "business": 1.75,
        "first": 2.00,
    },
    # Singapore KrisFlyer (own SQ flights).
    # Cheapest discount fares (W/T/S/Q/N classes): 50%; mid economy: 100%;
    # Business (C/D/J/Z): 125%; Suites (F/R): 150%.
    "krisflyer": {
        "economy": 0.50,
        "premium economy": 1.00,
        "business": 1.25,
        "first": 1.50,
    },
    # Etihad Guest (own EY flights).
    # Cheapest economy (V/W/S/L/E): 25%; standard economy: 50–100%;
    # Business Studio (C/D/J/Z): 100%; The Residence/First: 150%.
    "etihad guest": {
        "economy": 0.25,
        "premium economy": 0.75,
        "business": 1.00,
        "first": 1.50,
    },
    # United MileagePlus (Star Alliance partner flights — not own UA metal).
    # Partner economy = 50%; Business = 100%; First = 125%.
    "mileageplus": {
        "economy": 0.50,
        "premium economy": 0.75,
        "business": 1.00,
        "first": 1.25,
    },
    # Air Canada Aeroplan (Star Alliance partners).
    # Partner economy = 25%; Business = 100%.
    "aeroplan": {
        "economy": 0.25,
        "premium economy": 0.50,
        "business": 1.00,
        "first": 1.25,
    },
    # American AAdvantage (oneworld partner flights).
    # Partner economy = 50%; Business = 100%; First = 150%.
    "aadvantage": {
        "economy": 0.50,
        "premium economy": 0.75,
        "business": 1.00,
        "first": 1.50,
    },
    # Alaska Mileage Plan (oneworld partners).
    # One of the most generous partner programs — BA/IB/QR economy = 100%.
    "mileage plan": {
        "economy": 1.00,
        "premium economy": 1.00,
        "business": 1.50,
        "first": 2.00,
    },
    # Delta SkyMiles (revenue-based for own DL flights; partner economy = 25%).
    # Least generous major program for partner earning.
    "skymiles": {
        "economy": 0.25,
        "premium economy": 0.50,
        "business": 0.75,
        "first": 1.00,
    },
    # Emirates Skywards (own EK flights; not in any alliance).
    # Economy discount fares: 50%; Business: 100%; First: 150%.
    "emirates skywards": {
        "economy": 0.50,
        "premium economy": 0.75,
        "business": 1.00,
        "first": 1.50,
    },
}


def estimate_miles_for_program(
    distance_miles: int,
    program: str,
    cabin_class: str | None = None,
) -> Optional[int]:
    """Deterministic credited-miles estimate using per-program earning rates.

    Rates vary by cabin class: Business/First earn significantly more than Economy.
    """

    if not distance_miles or distance_miles <= 0:
        return None

    base = max(int(round(distance_miles)), 500)
    p = _normalize_program_key(program)

    rates = _EARNING_RATES.get(p)
    if rates is None:
        return None

    cabin_key = (cabin_class or "economy").strip().lower()
    if cabin_key in {"c", "j", "d", "z", "r"}:
        cabin_key = "business"
    elif cabin_key in {"f"}:
        cabin_key = "first"
    elif cabin_key in {"w", "p"}:
        cabin_key = "premium economy"
    elif cabin_key not in rates:
        cabin_key = "economy"

    factor = rates.get(cabin_key, rates.get("economy", 1.0))
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
    """Credited-miles estimate including cabin earning rate and round-trip doubling.

    Cabin earning rates come from per-program lookup tables in estimate_miles_for_program.
    Round-trip doubles the one-way estimate.
    """

    base = estimate_miles_for_program(distance_miles, program, cabin_class=cabin_class)
    if base is None:
        return None

    trip_mult = 2.0 if roundtrip is True else 1.0
    est = int(round((base * trip_mult) / 100.0) * 100)
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

    # Tie-break by stable preference: own-program first, then major alliance programs.
    preference = {"Miles&More": 5, "Flying Blue": 4, "Miles&Smiles": 3, "KrisFlyer": 3, "Etihad Guest": 3, "Emirates Skywards": 3}
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

