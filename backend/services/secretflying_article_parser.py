import os
import re
import csv
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv
from scrapingbee import ScrapingBeeClient

from scrapers.secretflying import _extract_price, _extract_route_from_title
from scoring.miles_utils import great_circle_miles, approximate_program_miles


_SKYSCANNER_PSEUDO_TO_IATA: Dict[str, str] = {
    # Internal Skyscanner code for "Singapore (all cities)".
    # We normalize it to the main IATA code SIN so the pipeline can
    # treat it the same as other destinations.
    "SINS": "SIN",
    # Internal Skyscanner code used for Mumbai in some aggregated links.
    # We normalize it to BOM.
    "IBOM": "BOM",
    # Internal Skyscanner code for "Beijing (all cities)".
    # We normalize it to the city code BJS to represent the city.
    "BJSA": "BJS",
    # Typical "any airport" codes in US links
    "NYCA": "NYC",  # New York (covers JFK/LGA/EWR)
    "FLLA": "FLL",  # Fort Lauderdale (area)
}

# City code (3 letters) to primary airport for distance calculations.
_CITY_CODE_TO_PRIMARY: Dict[str, str] = {
    "NYC": "JFK",
    "LON": "LHR",
    "PAR": "CDG",
    "CHI": "ORD",
    "TYO": "HND",
    "WAS": "DCA",
    "MIL": "MXP",
    "ROM": "FCO",
    "OSA": "ITM",
    "RIO": "GIG",
    "SAO": "GRU",
    "BER": "BER",
}

# City names for common IATA codes when the HTML does not provide clear text.
_IATA_TO_CITY: Dict[str, str] = {
    "NYC": "New York",
    "FLL": "Fort Lauderdale",
    "MIA": "Miami",
    "LAX": "Los Angeles",
    "SFO": "San Francisco",
    "SIN": "Singapore",
    "BOM": "Mumbai",
    "COK": "Kochi",
}

_AIRPORT_NAMES_BY_IATA: Dict[str, str] | None = None


_TRAILING_PRICE_RE = re.compile(
    r"\s+(?:f\u00fcr|ab|from|from only|for|for only)\s*\d[\d\s.,']*(?:\s*(?:\u20ac|eur|usd|chf|gbp|\$|\u00a3))?.*$",
    flags=re.IGNORECASE,
)


def _load_airport_names_map() -> Dict[str, str]:
    global _AIRPORT_NAMES_BY_IATA
    if _AIRPORT_NAMES_BY_IATA is not None:
        return _AIRPORT_NAMES_BY_IATA

    csv_path = os.path.join(os.path.dirname(__file__), "..", "scoring", "data", "airport_names_german.csv")
    names: Dict[str, str] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code") or "").strip().upper()
                name_raw = str(row.get("deutscher_Name") or row.get("name") or "").strip()
                name = _TRAILING_PRICE_RE.sub("", name_raw).strip()
                if code and name:
                    names[code] = name
    except Exception:
        names = {}

    _AIRPORT_NAMES_BY_IATA = names
    return names


def _normalize_iata_code(code: str | None) -> Optional[str]:
    if not code:
        return None
    code_up = code.strip().upper()
    mapped = _SKYSCANNER_PSEUDO_TO_IATA.get(code_up, code_up)
    return mapped


def _iata_to_city(code: str | None) -> Optional[str]:
    if not code:
        return None
    code_up = code.strip().upper()
    return _IATA_TO_CITY.get(code_up) or _load_airport_names_map().get(code_up)


def _iata_for_distance(code: str | None) -> Optional[str]:
    """Convert city codes to a representative airport for distance calc."""

    if not code:
        return None
    code_up = code.strip().upper()
    # First normalize pseudo-codes like NYCA -> NYC.
    code_norm = _normalize_iata_code(code_up) or code_up
    # If it is a known city code, use its primary airport.
    mapped = _CITY_CODE_TO_PRIMARY.get(code_norm)
    if mapped:
        return mapped
    return code_norm


def _approx_duration_from_miles(miles: int | None) -> tuple[Optional[int], Optional[str]]:
    """Heuristic duration from distance: cruise ~480 mph + 20 min buffer."""

    if miles is None or miles <= 0:
        return None, None
    hours = miles / 480.0 + (20.0 / 60.0)
    minutes = int(round(hours * 60))
    h = minutes // 60
    m = minutes % 60
    display = f"{h}h {m}m" if h else f"{m}m"
    return minutes, display


_MONTHS_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _parse_date_range_fragment(text: str, default_year: int) -> list[tuple[date, date]]:
    """Parse simple day-month ranges like '24th-28th Jan' or '30th Jan – 5th Feb'."""

    ranges: list[tuple[date, date]] = []
    # Normalize separators
    cleaned = text.replace("\u2013", "-").replace("\u2014", "-")
    pattern = re.compile(
        r"(\d{1,2})(?:st|nd|rd|th)?"       # day 1
        r"(?:\s*([A-Za-z]{3,9}))?"          # optional month 1
        r"\s*-\s*"
        r"(\d{1,2})(?:st|nd|rd|th)?"       # day 2
        r"(?:\s*([A-Za-z]{3,9}))?",        # optional month 2
        re.IGNORECASE,
    )
    for m in pattern.finditer(cleaned):
        d1 = int(m.group(1))
        d2 = int(m.group(3))
        m1 = m.group(2)
        m2 = m.group(4)
        month1 = _MONTHS_MAP.get(m1.lower()) if m1 else None
        month2 = _MONTHS_MAP.get(m2.lower()) if m2 else month1
        # If month1 is missing but month2 exists, assume month1=month2
        if not month1 and month2:
            month1 = month2
        if not month1:
            continue
        try:
            start = date(default_year, month1, d1)
            end = date(default_year, month2 or month1, d2)
            if end < start and month2:
                end = date(default_year + 1, end.month, end.day)
            ranges.append((start, end))
        except Exception:
            continue
    return ranges


def _extract_airline_from_body(soup: BeautifulSoup) -> Optional[str]:
    """Simple heuristic to extract the airline name.

    Looks for patterns like "Etihad Airways", "Qatar Airways", "Lufthansa",
    etc. in the first paragraphs of the article.
    """

    body = soup.find("article") or soup.find("main") or soup.body
    if not body:
        return None

    paras = body.find_all("p", limit=6) or []
    text = " ".join(p.get_text(" ", strip=True) for p in paras) or body.get_text(" ", strip=True)
    if not text:
        return None

    # Main pattern: proper noun followed by Airlines / Airways / Air / Line(s)
    m = re.search(
        r"([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*){0,3})\s+"
        r"(Airlines?|Airways|Air|Line|Lines)",
        text,
    )
    if m:
        return f"{m.group(1).strip()} {m.group(2).strip()}".strip()

    # Fallback: capture something like "Etihad Airways is offering" / "Lufthansa offers"
    m = re.search(
        r"([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*){0,3})\s+"
        r"(is offering|offers|are offering|has launched|has once again launched|is selling|sells)",
        text,
    )
    if m:
        return m.group(1).strip()

    return None


def _parse_secretflying_html(html: str, url: str) -> Dict[str, Any]:
    """Parse already-fetched SecretFlying HTML into the normalized deal dict.

    This allows reusing the same logic both from `parse_secretflying_post`
    (which makes the actual HTTP request) and from debugging scripts that
    load static HTML from disk (e.g. html_dumps).
    """

    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # Price + currency from the full text
    full_text = soup.get_text(" ", strip=True)
    price, currency = _extract_price(full_text)

    # Heuristic origin / destination from the title (free text)
    origin, destination = _extract_route_from_title(title)

    # Main airline (if mentioned in the article body)
    airline = _extract_airline_from_body(soup)

    # Try to locate a "Travel dates" / "Travel period" block (or similar)
    # to have a human-readable description of the dates even if we cannot extract
    # exact days.
    travel_dates_text: Optional[str] = None
    try:
        labels = ["travel dates", "travel period", "travel date"]

        def _match_label(s: object) -> bool:
            if not isinstance(s, str):
                return False
            t = s.lower()
            return any(lbl in t for lbl in labels)

        node = soup.find(string=_match_label)
        if node is not None:
            parent = node.parent
            if parent is not None:
                line = parent.get_text(" ", strip=True)
                # Typical case: "Travel dates: January to March 2025"
                if ":" in line:
                    travel_dates_text = line.split(":", 1)[1].strip() or line.strip()
                else:
                    sib = parent.find_next_sibling(["p", "div", "ul", "ol"])
                    if sib is not None:
                        travel_dates_text = sib.get_text(" ", strip=True)
            # Very conservative fallback
            if not travel_dates_text:
                travel_dates_text = str(node).strip()

        # Additional fallback specific to the current SecretFlying pattern,
        # where they use a block like:
        #   "DATES: Availability from February to April 2026"
        # instead of the "Travel dates" label.
        if not travel_dates_text:
            for p in soup.find_all("p"):
                text_p = p.get_text(" ", strip=True).strip()
                lt = text_p.lower()
                if "availability from" in lt:
                    # Extract only the part starting from "availability from".
                    # Example: "DATES: Availability from February to April 2026"
                    # -> "February to April 2026".
                    idx = lt.find("availability from")
                    if idx >= 0:
                        tail = text_p[idx + len("availability from"):].strip(" :")
                        travel_dates_text = tail or text_p
                        break
    except Exception:
        travel_dates_text = None

    # Optional aggregated routes block ("Routes:") with prices per city
    # pair, e.g. "Barcelona – Kochi: €219-€221".
    routes: List[Dict[str, Any]] = []
    try:
        article_root = soup.find("article") or soup
        routes_p = None
        for p in article_root.find_all("p"):
            ems = p.find_all("em")
            if not ems:
                continue
            label = ems[0].get_text(" ", strip=True).lower()
            if "routes" in label:
                routes_p = p
                break

        if routes_p is not None:
            ems = routes_p.find_all("em")[1:]
            pattern = re.compile(
                r"(.+?)\s+–\s+(.+?):\s*€?\s*([\d]+(?:[.,]\d+)?)(?:\s*-\s*€?\s*([\d]+(?:[.,]\d+)?))?",
            )
            for em in ems:
                txt = em.get_text(" ", strip=True)
                m = pattern.match(txt)
                if not m:
                    continue
                origin_label = m.group(1).strip()
                dest_label = m.group(2).strip()
                price_min_raw = m.group(3)
                price_max_raw = m.group(4)

                try:
                    price_min_val = float(price_min_raw.replace(",", ".")) if price_min_raw else None
                except Exception:
                    price_min_val = None
                try:
                    price_max_val = float(price_max_raw.replace(",", ".")) if price_max_raw else None
                except Exception:
                    price_max_val = None

                routes.append(
                    {
                        "origin": origin_label,
                        "destination": dest_label,
                        "price_min": price_min_val,
                        "price_max": price_max_val,
                    }
                )
    except Exception:
        routes = []

    # For each route date example block (for instance, the one starting with
    # "Barcelona – Kochi: €219-€221" followed by several Skyscanner links),
    # we try to associate an origin_iata/destination_iata pair and a
    # reference booking_url to the corresponding route.
    route_details: Dict[tuple, Dict[str, Any]] = {}
    try:
        article_root = soup.find("article") or soup
        for p in article_root.find_all("p"):
            strong = p.find("strong")
            if not strong:
                continue
            text = strong.get_text(" ", strip=True)
            m = re.match(r"(.+?)\s+–\s+(.+?):", text)
            if not m:
                continue
            r_origin = m.group(1).strip()
            r_dest = m.group(2).strip()

            # First Skyscanner link within this block
            a = p.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            parsed = urlparse(href)
            qs = parse_qs(parsed.query)

            def _first_param(qs_dict: Dict[str, List[str]], *cands: str) -> Optional[str]:
                for k in cands:
                    vals = qs_dict.get(k)
                    if vals:
                        return vals[0]
                return None

            def _find_param_like(qs_dict: Dict[str, List[str]], subs: List[str]) -> Optional[str]:
                for k, vals in qs_dict.items():
                    if not vals:
                        continue
                    lk = k.lower()
                    if any(s in lk for s in subs):
                        return vals[0]
                return None

            o_code = (
                _first_param(qs, "origin", "from")
                or _find_param_like(qs, ["origin", "from"])
                or ""
            ).strip().upper()
            d_code = (
                _first_param(qs, "destination", "to")
                or _find_param_like(qs, ["dest", "to"])
                or ""
            ).strip().upper()

            o_code = _normalize_iata_code(o_code) or ""
            d_code = _normalize_iata_code(d_code) or ""

            details: Dict[str, Any] = {"booking_url": href}
            if len(o_code) == 3 and o_code.isalpha():
                details["origin_iata"] = o_code
            if len(d_code) == 3 and d_code.isalpha():
                details["destination_iata"] = d_code

            route_details[(r_origin, r_dest)] = details
    except Exception:
        route_details = {}

    # Enrich each route with IATA/booking_url details if we were able
    # to associate them.
    if routes and route_details:
        for r in routes:
            key = (r.get("origin"), r.get("destination"))
            det = route_details.get(key)
            if not det:
                continue
            if det.get("origin_iata"):
                r["origin_iata"] = det["origin_iata"]
            if det.get("destination_iata"):
                r["destination_iata"] = det["destination_iata"]
            if det.get("booking_url"):
                r["booking_url"] = det["booking_url"]

    # Optional explicit airlines block ("AIRLINES:") as seen in many posts:
    # "AIRLINES:" followed by the name in bold.
    try:
        if not airline:
            article_root = soup.find("article") or soup
            for p in article_root.find_all("p"):
                strongs = p.find_all("strong")
                if not strongs:
                    continue
                label = strongs[0].get_text(" ", strip=True).lower()
                if "airlines:" in label:
                    # Take the remaining <strong> elements within the same <p>
                    names = [s.get_text(" ", strip=True) for s in strongs[1:]]
                    names = [n for n in names if n]
                    if names:
                        airline = "/".join(names)
                    break
    except Exception:
        pass

    # If we were able to extract a structured routes block, use the first
    # route as the "official" origin/destination of the deal, instead of the
    # free text with multiple cities from the title.
    if routes:
        origin = routes[0].get("origin") or origin
        destination = routes[0].get("destination") or destination

    # Main image: og:image or first <img>
    image = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        image = og["content"]
    else:
        img = soup.find("img")
        if img and img.get("src"):
            image = img["src"]
            if image.startswith("/"):
                image = urljoin(url, image)

    # booking_url: first external link (not secretflying) within the article
    booking_url = None
    article = soup.find("article") or soup
    external_links: List[str] = []
    for a in article.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = urljoin(url, href)
        netloc = urlparse(href).netloc.lower()
        if not netloc:
            continue
        if "secretflying.com" in netloc:
            continue

        # Use the first external link we find as the main booking_url
        if booking_url is None:
            booking_url = href
        # Save all external links to potentially build multiple itineraries
        external_links.append(href)

    # Try to extract structured data from all Skyscanner-type external links
    # (origin/destination, dates, cabin...). This allows us to model multiple
    # itineraries per post when there are multiple links.
    origin_iata = None
    destination_iata = None
    departure_date = None
    return_date = None
    cabin_class = None
    roundtrip = None

    itineraries: List[Dict[str, Any]] = []
    seen_keys: set[tuple] = set()

    def _first_matching_param(qs: Dict[str, List[str]], *candidates: str) -> Optional[str]:
        for key in candidates:
            vals = qs.get(key)
            if vals:
                return vals[0]
        return None

    def _find_param_by_substring(qs: Dict[str, List[str]], substrings: List[str]) -> Optional[str]:
        for k, vals in qs.items():
            if not vals:
                continue
            lk = k.lower()
            if any(sub in lk for sub in substrings):
                return vals[0]
        return None

    for ext_url in external_links:
        try:
            parsed = urlparse(ext_url)
            qs = parse_qs(parsed.query)

            # 1) Try to get IATA codes and dates from the query string
            origin_code = (
                _first_matching_param(qs, "origin", "from")
                or _find_param_by_substring(qs, ["origin", "from"])
                or ""
            ).strip().upper()
            dest_code = (
                _first_matching_param(qs, "destination", "to")
                or _find_param_by_substring(qs, ["dest", "to"])
                or ""
            ).strip().upper()

            dep = _first_matching_param(qs, "outboundDate", "departureDate", "departDate")
            if not dep:
                dep = _find_param_by_substring(qs, ["outbounddate", "departure", "depart", "date_from", "datefrom"])

            ret = _first_matching_param(qs, "inboundDate", "returnDate")
            if not ret:
                ret = _find_param_by_substring(qs, ["inbounddate", "return", "date_to", "dateto"])

            cabin_raw = _first_matching_param(qs, "cabinclass", "cabin_Class", "cabin")
            if not cabin_raw:
                cabin_raw = _find_param_by_substring(qs, ["cabin", "class"])
            cabin_raw = (cabin_raw or "").strip()

            rtn = _first_matching_param(qs, "rtn", "roundtrip")
            rtn = (rtn or "").strip()

            # Normalize possible internal codes (e.g. SINS -> SIN in Skyscanner)
            origin_code = _normalize_iata_code(origin_code) or ""
            dest_code = _normalize_iata_code(dest_code) or ""

            # 2) If we could not deduce codes from the query, try to extract
            # them from the path (e.g. /flights/BCN/COK/2025-02-01/2025-02-10).
            if not origin_code or not dest_code:
                segments = [seg for seg in parsed.path.split("/") if seg]
                iata_candidates = [seg.upper() for seg in segments if len(seg) == 3 and seg.isalpha()]
                if len(iata_candidates) >= 2:
                    origin_code = origin_code or iata_candidates[0]
                    dest_code = dest_code or iata_candidates[1]

                # Simple dates in the path (YYYYMMDD or YYYY-MM-DD / DDMMYY, etc.)
                if not dep or not ret:
                    date_like = [seg for seg in segments if any(ch.isdigit() for ch in seg) and len(seg) in {6, 8, 10}]
                    if date_like:
                        if not dep:
                            dep = date_like[0]
                        if len(date_like) > 1 and not ret:
                            ret = date_like[1]

            # Without origin/destination we do not consider this a well-defined flight
            if not origin_code or not dest_code:
                continue

            key = (origin_code, dest_code, dep, ret, cabin_raw.upper(), rtn)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            itin: Dict[str, Any] = {
                "origin_iata": origin_code if len(origin_code) == 3 and origin_code.isalpha() else None,
                "destination_iata": dest_code if len(dest_code) == 3 and dest_code.isalpha() else None,
                "departure_date": dep,
                "return_date": ret,
                "cabin_class": cabin_raw.upper() if cabin_raw else None,
                "roundtrip": (rtn == "1" or rtn.lower() == "true") if rtn else None,
                "booking_url": ext_url,
            }

            if airline:
                itin["airline"] = airline

            # Fill in global price/currency as an approximation
            if price is not None:
                itin["price"] = price
                itin["currency"] = (currency or "EUR") if price is not None else None

            itineraries.append(itin)
        except Exception:
            continue

    # Consolidate itineraries: one row per origin/destination pair.
    if len(itineraries) > 1:
        merged: Dict[frozenset, Dict[str, Any]] = {}

        def _safe_min_date(a: str | None, b: str | None) -> str | None:
            if not a:
                return b
            if not b:
                return a
            return min(a, b)

        def _safe_max_date(a: str | None, b: str | None) -> str | None:
            if not a:
                return b
            if not b:
                return a
            return max(a, b)

        # Prefer the direction detected in the title to choose the canonical order
        preferred_pair = None
        if origin_iata and destination_iata:
            preferred_pair = (origin_iata, destination_iata)

        for it in itineraries:
            if not isinstance(it, dict):
                continue
            o_iata = it.get("origin_iata")
            d_iata = it.get("destination_iata")
            if not o_iata or not d_iata:
                continue

            pair_key = frozenset({o_iata, d_iata})

            def _canonical_pair() -> tuple[str, str]:
                if preferred_pair and set(preferred_pair) == set(pair_key):
                    return preferred_pair
                # fallback: alphabetical order for determinism
                return tuple(sorted([o_iata, d_iata]))

            existing = merged.get(pair_key)
            if not existing:
                can_o, can_d = _canonical_pair()
                new_it = dict(it)
                new_it["origin_iata"] = can_o
                new_it["destination_iata"] = can_d
                merged[pair_key] = new_it
                continue

            # Merge: keep existing booking_url, accumulate date range and normalize order
            existing["departure_date"] = _safe_min_date(existing.get("departure_date"), it.get("departure_date"))
            existing["return_date"] = _safe_max_date(existing.get("return_date"), it.get("return_date"))
            can_o, can_d = _canonical_pair()
            existing["origin_iata"] = can_o
            existing["destination_iata"] = can_d

            # roundtrip/one_way
            if it.get("roundtrip") is True:
                existing["roundtrip"] = True
                existing["one_way"] = False
            if it.get("roundtrip") is False and existing.get("roundtrip") is None:
                existing["roundtrip"] = False
                existing["one_way"] = True

            # Prefer airline if missing
            if not existing.get("airline") and it.get("airline"):
                existing["airline"] = it.get("airline")
            # Prefer cabin class if missing
            if not existing.get("cabin_class") and it.get("cabin_class"):
                existing["cabin_class"] = it.get("cabin_class")
            # Prefer duration if missing
            if not existing.get("flight_duration_minutes") and it.get("flight_duration_minutes"):
                existing["flight_duration_minutes"] = it.get("flight_duration_minutes")
            if not existing.get("flight_duration_display") and it.get("flight_duration_display"):
                existing["flight_duration_display"] = it.get("flight_duration_display")
            # Prefer miles if missing
            if not existing.get("miles") and it.get("miles"):
                existing["miles"] = it.get("miles")

        itineraries = list(merged.values())

    # Extract date ranges from the post text (e.g. "24th-28th Jan").
    try:
        year_guess = None
        for src in (travel_dates_text, title, full_text):
            if not src:
                continue
            m = re.search(r"(20\d{2})", src)
            if m:
                year_guess = int(m.group(1))
                break
        if year_guess is None:
            year_guess = datetime.now().year

        ranges = _parse_date_range_fragment(full_text, year_guess)
        if ranges:
            starts = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            min_start = min(starts)
            max_end = max(ends)

            dep_str = min_start.isoformat()
            ret_str = max_end.isoformat()

            # Refresh deal-level dates
            if not departure_date or dep_str < str(departure_date):
                departure_date = dep_str
            if not return_date or ret_str > str(return_date):
                return_date = ret_str

            # Apply to consolidated itineraries if they exist
            if itineraries:
                for it in itineraries:
                    if not isinstance(it, dict):
                        continue
                    if not it.get("departure_date") or dep_str < str(it.get("departure_date")):
                        it["departure_date"] = dep_str
                    if not it.get("return_date") or ret_str > str(it.get("return_date")):
                        it["return_date"] = ret_str

            # Fill in travel_dates_text if it did not exist
            if not travel_dates_text:
                travel_dates_text = f"{dep_str} – {ret_str}"
    except Exception:
        pass

    # For compatibility with the current pipeline, copy the first itinerary
    # to the root level of the deal (origin_iata/destination_iata/dates/cabin_class...)
    if itineraries:
        first = itineraries[0]
        origin_iata = first.get("origin_iata")
        destination_iata = first.get("destination_iata")
        departure_date = first.get("departure_date")
        return_date = first.get("return_date")
        cabin_class = first.get("cabin_class")
        roundtrip = first.get("roundtrip")
        one_way = None
        if roundtrip is True:
            one_way = False
        elif roundtrip is False:
            one_way = True

        # If we know a textual origin/destination at the deal level,
        # propagate it to itineraries that do not have that field filled in.
        for it in itineraries:
            if isinstance(it, dict):
                if origin and not it.get("origin"):
                    it["origin"] = origin
                if destination and not it.get("destination"):
                    it["destination"] = destination

    # Fill in origin/destination labels if they are empty or broken
    # (e.g., titles "Non-stop from ..." that end up as "Non").
    def _fill_place(current: Optional[str], iata: Optional[str]) -> Optional[str]:
        def _looks_broken(text: str) -> bool:
            low = text.strip().lower()
            cleaned = re.sub(r"[^a-z\s-]", "", low).strip()
            if not cleaned:
                return True
            if cleaned.startswith("non"):
                return True
            if "stop from" in cleaned:
                return True
            if cleaned in {"non", "non stop", "non-stop", "nonstop"}:
                return True
            return False

        if current and current.strip():
            if not _looks_broken(current):
                return current.strip()
        city = _iata_to_city(iata)
        if city:
            return city
        return iata

    origin = _fill_place(origin, origin_iata)
    destination = _fill_place(destination, destination_iata)

    # If we don't have travel_dates_text but do have exact dates, generate a simple range
    if not travel_dates_text and departure_date and return_date:
        travel_dates_text = f"{departure_date} – {return_date}"

    # Cleanup per itinerary: fill in human-readable origin/destination if missing or broken
    for it in itineraries or []:
        if not isinstance(it, dict):
            continue
        it_origin = it.get("origin")
        it_dest = it.get("destination")
        it_origin_iata = it.get("origin_iata")
        it_dest_iata = it.get("destination_iata")
        it["origin"] = _fill_place(it_origin, it_origin_iata)
        it["destination"] = _fill_place(it_dest, it_dest_iata)

        # Add approximate miles per itinerary if we know both IATA codes
        if (it_origin_iata or origin_iata) and (it_dest_iata or destination_iata) and not it.get("miles"):
            try:
                o_code = _iata_for_distance(it_origin_iata or origin_iata)
                d_code = _iata_for_distance(it_dest_iata or destination_iata)
                gc = great_circle_miles(o_code, d_code)
                approx_m = approximate_program_miles(gc) if gc is not None else None
                if approx_m is not None:
                    it["miles"] = approx_m
                    mins, disp = _approx_duration_from_miles(approx_m)
                    if mins and not it.get("flight_duration_minutes"):
                        it["flight_duration_minutes"] = mins
                    if disp and not it.get("flight_duration_display"):
                        it["flight_duration_display"] = disp
            except Exception:
                pass

        # Derivar one_way a nivel de itinerario si roundtrip es conocido
        if it.get("roundtrip") is True and it.get("one_way") is None:
            it["one_way"] = False
        elif it.get("roundtrip") is False and it.get("one_way") is None:
            it["one_way"] = True

    deal: Dict[str, Any] = {
        "title": title,
        "price": price,
        "currency": (currency or "EUR") if price is not None else None,
        "link": url,
        "booking_url": booking_url,
        "origin": origin,
        "destination": destination,
        "image": image,
        "travel_dates_text": travel_dates_text,
        "routes": routes or None,
        "itineraries": itineraries or None,
        "origin_iata": origin_iata,
        "destination_iata": destination_iata,
        "departure_date": departure_date,
        "return_date": return_date,
        "cabin_class": cabin_class,
        "roundtrip": roundtrip,
        "one_way": one_way if "one_way" in locals() else None,
        "airline": airline,
        "source": "SecretFlying",
    }

    # Approximate miles and duration at the deal level if we have both IATA codes
    if (deal.get("origin_iata") or origin_iata) and (deal.get("destination_iata") or destination_iata):
        try:
            o_code = _iata_for_distance(deal.get("origin_iata") or origin_iata)
            d_code = _iata_for_distance(deal.get("destination_iata") or destination_iata)
            if not deal.get("miles"):
                gc = great_circle_miles(o_code, d_code)
                approx_m = approximate_program_miles(gc) if gc is not None else None
                if approx_m is not None:
                    deal["miles"] = approx_m
            mins, disp = _approx_duration_from_miles(deal.get("miles"))
            if mins and not deal.get("flight_duration_minutes"):
                deal["flight_duration_minutes"] = mins
            if disp and not deal.get("flight_duration_display"):
                deal["flight_duration_display"] = disp
        except Exception:
            pass

    return deal


def parse_secretflying_post(url: str) -> Dict[str, Any]:
    """Parse a SecretFlying deal post page and return normalized fields.

    - link: URL of the SecretFlying post page
    - booking_url: first external link (Skyscanner, airline, OTA...)
    - price / currency: extracted from the full text
    - origin / destination: heuristic extraction from the title
    - image: og:image or first <img>
    """
    load_dotenv(find_dotenv())

    # 1) Try first with a direct requests call (without ScrapingBee), same as
    # the listing scraper. This avoids always depending on the API and works
    # well when the target page is not heavily protected.
    html: Optional[str] = None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
        }
        resp_req = requests.get(url, headers=headers, timeout=10)
        if resp_req.status_code == 200 and resp_req.text:
            html = resp_req.text
    except Exception:
        html = None

    # 2) If that fails or returns empty content, use ScrapingBee as fallback
    if html is None:
        api_key = os.getenv("SCRAPINGBEE_API_KEY")
        if not api_key:
            raise RuntimeError("SCRAPINGBEE_API_KEY is not set in .env and the direct request failed")

        client = ScrapingBeeClient(api_key=api_key)

        js_scenario = {
            "instructions": [
                {"wait": int(os.getenv("SCRAPINGBEE_JS_WAIT_MS", "2000"))},
            ]
        }

        params = {
            "js_scenario": js_scenario,
            "stealth_proxy": "True",
            "country_code": os.getenv("SCRAPINGBEE_COUNTRY_CODE", "us"),
        }

        resp = client.get(url, params=params)
        html = resp.text

    return _parse_secretflying_html(html, url)
