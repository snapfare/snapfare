import os
import re
import csv
import logging
import tempfile
import time
from contextlib import contextmanager
from html import escape
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlparse, parse_qs

from scrapers.travel_dealz import get_deals as get_travel_dealz, get_deals_de as get_travel_dealz_de
from scrapers.secretflying import get_deals as get_secretflying
from database.supabase_db import save_deals, _client
from services.travel_dealz_article_parser import parse_travel_dealz_article
from services.secretflying_article_parser import parse_secretflying_post
from services.baggage_format import format_baggage_short_de
from scoring.miles_utils import (
    great_circle_miles,
    approximate_program_miles,
    choose_best_program_for_deal,
)

logger = logging.getLogger("snapcore.pipeline")


_TRAILING_PRICE_RE = re.compile(
    r"\s+(?:f\u00fcr|ab|from|from only|for|for only)\s*\d[\d\s.,']*(?:\s*(?:\u20ac|eur|usd|chf|gbp|\$|\u00a3))?.*$",
    flags=re.IGNORECASE,
)


def _sanitize_place_label(val: Any) -> Optional[str]:
    """Normalize a scraped place/city label.

    Defensive against common scraped artifacts like trailing price fragments
    ("Frankfurt f\u00fcr 198\u20ac") and embedded newlines.
    """

    if not isinstance(val, str):
        return None
    s = val.strip()
    if not s:
        return None
    s = s.replace("\r", " ").replace("\n", " ")
    s = _TRAILING_PRICE_RE.sub("", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    # Avoid persisting obviously bad labels.
    if len(s) < 2 or len(s) > 80:
        return None
    if not re.search(r"[A-Za-z\u00c0-\u024f]", s):
        return None
    # If there are digits left, it's likely still polluted.
    if any(ch.isdigit() for ch in s):
        return None
    return s


@contextmanager
def _file_lock(lock_path: str, timeout_seconds: float = 10.0) -> Any:
    """Best-effort cross-platform file lock.

    Used to protect small local CSV updates from concurrent runs.
    Never raises to callers: if lock cannot be acquired quickly,
    we just yield without a lock.
    """

    lock_file = None
    acquired = False
    start = time.time()
    try:
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        lock_file = open(lock_path, "a+", encoding="utf-8")

        while True:
            try:
                if os.name == "nt":
                    import msvcrt  # type: ignore

                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl  # type: ignore

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except Exception:
                if (time.time() - start) >= float(timeout_seconds):
                    break
                time.sleep(0.05)

        yield
    finally:
        try:
            if lock_file and acquired:
                try:
                    if os.name == "nt":
                        import msvcrt  # type: ignore

                        lock_file.seek(0)
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl  # type: ignore

                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
        finally:
            try:
                if lock_file:
                    lock_file.close()
            except Exception:
                pass

# Optional: airportsdata provides IATA -> {city,name,...}. Useful as a best-effort
# fallback to auto-fill airport_names_german.csv when we only have codes.
try:  # pragma: no cover - optional dependency
    import airportsdata  # type: ignore

    _AIRPORTS_IATA_DATA: Dict[str, Dict[str, Any]] = airportsdata.load("IATA")
except Exception:  # pragma: no cover - defensive
    _AIRPORTS_IATA_DATA = {}

# LLM enrichment removed — all fields are now deterministic.


def _parse_scraping_sources() -> set[str]:
    """Decide which scrapers are enabled based on per-source limits.

    - travel-dealz is considered active if SCRAPING_LIMIT_TRAVEL_DEALZ > 0.
    - secretflying is considered active if SCRAPING_LIMIT_SECRETFLYING > 0.

    If neither variable is defined, the legacy behaviour is preserved:
    we try to read SCRAPING_URL and, as a last resort, both sources
    are enabled.
    """

    td_env = os.getenv("SCRAPING_LIMIT_TRAVEL_DEALZ")
    sf_env = os.getenv("SCRAPING_LIMIT_SECRETFLYING")

    enabled: set[str] = set()

    parsed_any_limit = False
    try:
        if td_env is not None:
            parsed_any_limit = True
            if int(td_env) > 0:
                enabled.add("travel-dealz")
    except Exception:
        pass

    try:
        if sf_env is not None:
            parsed_any_limit = True
            if int(sf_env) > 0:
                enabled.add("secretflying")
    except Exception:
        pass

    if parsed_any_limit:
        return enabled

    # Legacy fallback: keep respecting SCRAPING_URL if no
    # per-source limits are configured.
    raw = os.getenv("SCRAPING_URL", "").strip()
    if not raw:
        return {"travel-dealz", "secretflying"}
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    for p in parts:
        if "travel-dealz" in p:
            enabled.add("travel-dealz")
        if "secretflying" in p:
            enabled.add("secretflying")
    return enabled or {"travel-dealz", "secretflying"}


def _load_done_article_urls(limit: int | None = None) -> Dict[str, set[str]]:
    """Load article URLs already marked as done in source_articles.

    Returns a mapping {"travel-dealz": {urls}, "secretflying": {urls}}.
    If Supabase is not configured, returns empty sets.
    """

    if not _client:
        return {"travel-dealz": set(), "secretflying": set()}

    max_rows = limit or int(os.getenv("SOURCE_ARTICLES_DONE_LIMIT", "10000"))
    done_by_source: Dict[str, set[str]] = {"travel-dealz": set(), "secretflying": set()}

    try:
        rsp = (
            _client.table("source_articles")
            .select("article_url, source, status")
            .eq("status", "done")
            .limit(max_rows)
            .execute()
        )
    except Exception:
        # If this query fails, we simply don't filter by done.
        return done_by_source

    rows = getattr(rsp, "data", []) or []
    for row in rows:
        url = str(row.get("article_url") or "").strip()
        if not url:
            continue
        src = str(row.get("source") or "").lower()
        if "travel-dealz" in src:
            done_by_source["travel-dealz"].add(url)
        elif "secretflying" in src:
            done_by_source["secretflying"].add(url)

    return done_by_source


def _upsert_source_articles_done(scored_deals: List[Dict[str, Any]]) -> None:
    """Upsert article URLs from scored deals into source_articles as status='done'.

    This keeps source_articles in sync whenever the pipeline persists deals.
    """

    if not _client:
        return

    rows: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for d in scored_deals:
        link = str(d.get("link") or "").strip()
        if not link or link in seen_urls:
            continue

        source_label = str(d.get("source") or "").lower()
        source: str | None = None
        if "travel-dealz" in source_label:
            source = "travel-dealz"
        elif "secretflying" in source_label:
            source = "secretflying"

        if not source:
            continue

        seen_urls.add(link)
        from datetime import datetime, timezone  # noqa: PLC0415
        rows.append({
            "article_url": link,
            "source": source,
            "status": "done",
            "last_scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    if not rows:
        return

    try:
        logger.info("[deals_pipeline] source_articles upsert rows=%s", len(rows))
        _client.table("source_articles").upsert(rows, on_conflict="article_url").execute()
    except Exception:
        # Non-critical error: don't interrupt the deals pipeline.
        logger.warning("[deals_pipeline] source_articles upsert failed", exc_info=True)
        return


def _mark_source_article_done(url: str, source: str, note: str | None = None) -> None:
    """Best-effort: mark an article URL as done in source_articles.

    Used for items we deliberately ignore (non-flight promos) so we don't
    keep re-scraping them on every run.
    """

    if not _client:
        return

    u = str(url or "").strip()
    if not u:
        return
    src = str(source or "").strip().lower()
    if src not in {"travel-dealz", "secretflying"}:
        return

    payload: Dict[str, Any] = {"article_url": u, "source": src, "status": "done"}
    if note:
        payload["last_error"] = str(note)[:200]

    try:
        _client.table("source_articles").upsert([payload], on_conflict="article_url").execute()
    except Exception:
        return


_TD_NON_FLIGHT_SLUG_MARKERS = {
    "geschenkgutschein",  # gift card
}


def _is_travel_dealz_flight_article(article: Dict[str, Any], url: str) -> bool:
    """Heuristic: decide if a Travel-Dealz article is a flight deal.

    We only want to persist items that describe an itinerary / route.
    """

    u = str(url or "").lower()
    if any(m in u for m in _TD_NON_FLIGHT_SLUG_MARKERS):
        return False

    itins = article.get("itineraries")
    if isinstance(itins, list):
        for it in itins:
            if not isinstance(it, dict):
                continue
            if it.get("booking_url"):
                return True
            oi = it.get("origin_iata")
            di = it.get("destination_iata")
            if isinstance(oi, str) and isinstance(di, str) and len(oi.strip()) == 3 and len(di.strip()) == 3:
                return True
            if it.get("origin") and it.get("destination"):
                return True

    # Fallback: sometimes Travel-Dealz has no explicit booking links but still
    # has a clear flight context (airline + destinations). We keep those.
    destinations = article.get("destinations")
    if isinstance(destinations, list) and destinations:
        if article.get("airline") or article.get("travel_dates_text") or article.get("miles"):
            return True

    return False


_CURRENCY_TO_EUR: Dict[str, float] = {
    "EUR": 1.0,
    "USD": 0.9,   # approximate
    "CHF": 1.02,  # approximate
    "GBP": 1.15,  # approximate
}

# Lightweight cache of airport names (German) indexed by IATA
_AIRPORT_NAMES_BY_IATA: Dict[str, str] | None = None
_AIRLINES_BY_CODE: Dict[str, str] | None = None
_AIRCRAFT_BY_IATA: Dict[str, str] | None = None

_AIRPORT_CSV_FIELDS: list[str] = ["code", "deutscher_Name", "photo_url"]


def _prefer_german_airport_names_enabled() -> bool:
    """Whether we should try to prefer/upgrade airport names to German.

    Default: enabled. Operators can disable via env.
    """

    raw = os.getenv("DEALS_PREFER_GERMAN_AIRPORT_NAMES", "true").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _looks_more_german(new_name: str, old_name: str) -> bool:
    """Heuristic: decide if new_name is a better German label than old_name."""

    n = (new_name or "").strip()
    o = (old_name or "").strip()
    if not n or not o:
        return False

    # German-specific characters
    n_has_umlaut = any(ch in n for ch in "äöüÄÖÜß")
    o_has_umlaut = any(ch in o for ch in "äöüÄÖÜß")
    if n_has_umlaut and not o_has_umlaut:
        return True

    # Prefer non-ASCII (often indicates proper diacritics) if old is plain ASCII
    n_non_ascii = any(ord(ch) > 127 for ch in n)
    o_non_ascii = any(ord(ch) > 127 for ch in o)
    if n_non_ascii and not o_non_ascii:
        return True

    # A few German exonym markers
    german_markers = [
        "Neu-",
        "Süd",
        "Nord",
        "Flughafen",
        "Köln",
        "München",
        "Warschau",
        "Peking",
        "Mexiko-Stadt",
        "Kapstadt",
    ]
    n_low = n.lower()
    o_low = o.lower()
    for m in german_markers:
        if m.lower() in n_low and m.lower() not in o_low:
            return True

    return False


def _load_airport_names_map() -> Dict[str, str]:
    global _AIRPORT_NAMES_BY_IATA
    if _AIRPORT_NAMES_BY_IATA is not None:
        return _AIRPORT_NAMES_BY_IATA

    csv_path = os.path.join(os.path.dirname(__file__), "..", "scoring", "data", "airport_names_german.csv")
    # IMPORTANT: include codes with empty names.
    # We use this map both for lookups and for preventing duplicate appends.
    names: Dict[str, str] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code") or "").strip().upper()
                name_raw = str(row.get("deutscher_Name") or "").strip()
                name = _sanitize_place_label(name_raw) or _TRAILING_PRICE_RE.sub("", name_raw).strip()
                # Skip embedded header rows like "code,deutscher_Name"
                if code.lower() == "code":
                    continue
                if code and len(code) == 3:
                    names[code] = name
    except Exception:
        names = {}

    _AIRPORT_NAMES_BY_IATA = names
    return names


def _read_airport_csv_rows(csv_path: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not os.path.exists(csv_path):
        return rows
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code") or "").strip().upper()
                if not code or code.lower() == "code" or len(code) != 3:
                    continue
                name_val = str(row.get("deutscher_Name") or "").strip()
                photo_val = str(row.get("photo_url") or row.get("image_url") or "").strip()
                rows.append({"code": code, "deutscher_Name": name_val, "photo_url": photo_val})
    except Exception:
        return []
    return rows


def _write_airport_csv_atomic(csv_path: str, rows: list[dict[str, str]]) -> None:
    folder = os.path.dirname(csv_path)
    os.makedirs(folder, exist_ok=True)
    tmp_file = None
    try:
        tmp_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            delete=False,
            dir=folder,
            prefix=os.path.basename(csv_path) + ".",
            suffix=".tmp",
        )
        with tmp_file:
            writer = csv.DictWriter(tmp_file, fieldnames=_AIRPORT_CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_file.name, csv_path)
    finally:
        try:
            if tmp_file and os.path.exists(tmp_file.name):
                os.remove(tmp_file.name)
        except Exception:
            pass


def _update_airport_name(code: str, name: str) -> None:
    """Update airport_names_german.csv with a name for a given IATA code.

    Only writes when the code is missing or has an empty name.
    Best-effort and never raises.
    """
    try:
        code = code.strip().upper()
        name = (name or "").strip()
        name_clean = _sanitize_place_label(name)
        if name_clean:
            name = name_clean
        if len(code) != 3 or not name:
            return

        csv_path = os.path.join(os.path.dirname(__file__), "..", "scoring", "data", "airport_names_german.csv")

        lock_path = csv_path + ".lock"
        with _file_lock(lock_path):
            existing = _load_airport_names_map()
            current_name = existing.get(code)

        # If already present with a non-empty name, don't overwrite
        # unless we can clearly upgrade to a more German label.
        if current_name and current_name.strip():
            if not _prefer_german_airport_names_enabled():
                return
            if not _looks_more_german(name, current_name):
                return

            rows = _read_airport_csv_rows(csv_path)

            # Remove duplicates while keeping last occurrence.
            out_rows: list[dict[str, str]] = []
            last_index_by_code: dict[str, int] = {}
            for r in rows:
                c = str(r.get("code") or "").strip().upper()
                if not c or len(c) != 3:
                    continue
                if c in last_index_by_code:
                    out_rows[last_index_by_code[c]] = None  # type: ignore[assignment]
                last_index_by_code[c] = len(out_rows)
                out_rows.append(r)
            out_rows = [r for r in out_rows if r]  # type: ignore[arg-type]

            updated = False
            for r in out_rows:
                if r.get("code") == code:
                    r["deutscher_Name"] = name
                    if "photo_url" not in r:
                        r["photo_url"] = ""
                    updated = True
                    break

            if not updated:
                out_rows.append({"code": code, "deutscher_Name": name, "photo_url": ""})

            _write_airport_csv_atomic(csv_path, out_rows)

            existing[code] = name
    except Exception:
        return


def _append_missing_airport_code(code: str) -> None:
    """Append missing IATA codes to airport_names_german.csv for later curation.

    Writes a row with empty name. Best-effort; never raises.
    """
    try:
        csv_path = os.path.join(os.path.dirname(__file__), "..", "scoring", "data", "airport_names_german.csv")
        code = code.strip().upper()
        if not code or len(code) != 3:
            return

        lock_path = csv_path + ".lock"
        with _file_lock(lock_path):
            current = _load_airport_names_map()
            # current includes empty names now; if present at all, don't add.
            if code in current:
                return

            rows = _read_airport_csv_rows(csv_path)
            # Double-check in raw rows too (paranoia against cache staleness).
            if any(str(r.get("code") or "").strip().upper() == code for r in rows):
                current[code] = ""
                return

            rows.append({"code": code, "deutscher_Name": "", "photo_url": ""})
            _write_airport_csv_atomic(csv_path, rows)
            current[code] = ""
    except Exception:
        return


def _lookup_airport_name(iata: Any) -> Optional[str]:
    if not isinstance(iata, str) or len(iata.strip()) != 3:
        return None
    return _load_airport_names_map().get(iata.strip().upper())


def _guess_airport_name_from_airportsdata(code: str) -> Optional[str]:
    if not code or len(code) != 3:
        return None
    rec = _AIRPORTS_IATA_DATA.get(code.upper())
    if not rec:
        return None
    city = str(rec.get("city") or "").strip()
    name = str(rec.get("name") or "").strip()
    return city or name or None


def _resolve_city_name(city_val: Any, iata_val: Any) -> Optional[str]:
    """Return a city/airport name, preferring provided city, else CSV map.

    - If city_val equals the IATA code (e.g. "ZRH"), we treat it as missing.
    - If missing, we try airport_names_german.csv.
    - If city_val conflicts with IATA but the map has a name, prefer the mapped name
      to avoid labels like "Tallinn (RIX)".
    """

    city_str_raw = str(city_val).strip() if isinstance(city_val, str) and city_val.strip() else None
    city_str = _sanitize_place_label(city_str_raw) or city_str_raw
    iata_str = str(iata_val).strip().upper() if isinstance(iata_val, str) and len(iata_val.strip()) == 3 else None

    if city_str and iata_str and city_str.upper() == iata_str:
        city_str = None
    if iata_str:
        _maybe_log_missing_iata(iata_str)

    # If we have a readable city and IATA, update CSV if missing
    if city_str and iata_str:
        # Only auto-write when the IATA is a real airport (airportsdata knows it)
        # or when we already have a curated mapping for this code.
        if iata_str in _AIRPORTS_IATA_DATA or _lookup_airport_name(iata_str):
            _update_airport_name(iata_str, city_str)

    # If we already have a name (from scraping), don't overwrite it with the CSV
    if city_str:
        return city_str

    if iata_str:
        mapped = _lookup_airport_name(iata_str)
        if mapped:
            return mapped
        # Robust fallback: if the CSV is missing/empty for a real IATA airport,
        # return the airportsdata city/name directly (do not depend on writing
        # the CSV successfully).
        guessed = _guess_airport_name_from_airportsdata(iata_str)
        if guessed:
            # Best-effort: try to write it for future runs, but don't rely on it.
            _update_airport_name(iata_str, guessed)
            return guessed
        # No mapping found; return the IATA code itself to avoid leaving it empty
        return iata_str

    return city_str or iata_str


_MISSING_IATA_SEEN: set[str] = set()


def _maybe_log_missing_iata(iata: Any) -> None:
    # Best-effort: track unknown IATAs to improve airport map later.
    if not isinstance(iata, str) or len(iata.strip()) != 3:
        return
    code = iata.strip().upper()
    names_map = _load_airport_names_map()

    # If we already have a non-empty curated value, nothing to do.
    if code in names_map and str(names_map.get(code) or "").strip():
        return
    if code in _MISSING_IATA_SEEN:
        return

    # Best-effort: if airportsdata knows this code, auto-fill and avoid noisy logs.
    guessed = _guess_airport_name_from_airportsdata(code)
    if guessed:
        _update_airport_name(code, guessed)
        return

    _MISSING_IATA_SEEN.add(code)
    logger.info("[deals_pipeline] Missing airport name for IATA=%s; consider adding to airport_names_german.csv", code)
    _append_missing_airport_code(code)


def _load_airline_map() -> Dict[str, str]:
    global _AIRLINES_BY_CODE
    if _AIRLINES_BY_CODE is not None:
        return _AIRLINES_BY_CODE

    csv_path = os.path.join(os.path.dirname(__file__), "..", "scoring", "data", "airlines.csv")
    data: Dict[str, str] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code") or "").strip().upper()
                name = str(row.get("name") or "").strip()
                if code and name:
                    data[code] = name
    except Exception:
        data = {}
    _AIRLINES_BY_CODE = data
    return data


def _resolve_airline_name(airline_val: Any = None, airline_code: Any = None) -> Optional[str]:
    # If we already have a descriptive name (contains space or length > 3), keep it.
    if isinstance(airline_val, str) and airline_val.strip():
        val = airline_val.strip()
        if len(val) > 3 or " " in val or not val.isalnum():
            return val
        # Short alnum strings could be codes; fall through

    code = None
    if isinstance(airline_code, str) and airline_code.strip():
        c = airline_code.strip()
        if len(c) <= 3:
            code = c.upper()
    if code is None and isinstance(airline_val, str) and airline_val.strip():
        v = airline_val.strip()
        if len(v) <= 3 and v.replace(" ", "").isalnum():
            code = v.upper()

    if code:
        return _load_airline_map().get(code) or code
    return None


def _load_aircraft_map() -> Dict[str, str]:
    global _AIRCRAFT_BY_IATA
    if _AIRCRAFT_BY_IATA is not None:
        return _AIRCRAFT_BY_IATA

    csv_path = os.path.join(os.path.dirname(__file__), "..", "scoring", "data", "aircraft_types.csv")
    data: Dict[str, str] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("IATA") or "").strip().upper()
                model = str(row.get("Aircraft_Model") or "").strip()
                if code and model:
                    data[code] = model
    except Exception:
        data = {}
    _AIRCRAFT_BY_IATA = data
    return data


def _resolve_aircraft_model(aircraft_val: Any = None, aircraft_code: Any = None) -> Optional[str]:
    # Keep readable strings (with spaces or length > 4) as-is.
    if isinstance(aircraft_val, str) and aircraft_val.strip():
        val = aircraft_val.strip()
        if len(val) > 4 or " " in val or not val.isalnum():
            return val

    code = None
    if isinstance(aircraft_code, str) and aircraft_code.strip():
        c = aircraft_code.strip()
        if len(c) <= 4:
            code = c.upper()
    if code is None and isinstance(aircraft_val, str) and aircraft_val.strip():
        v = aircraft_val.strip()
        if len(v) <= 4 and v.replace(" ", "").isalnum():
            code = v.upper()

    if code:
        return _load_aircraft_map().get(code) or code
    return None


def _parse_miles_value(raw: Any) -> Optional[int]:
    """Parse textual miles like "1'592 – 3'184" or numeric strings.

    Returns an integer (max of the numbers found) or None if nothing usable.
    """

    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            return int(raw)
        except Exception:
            return None

    if not isinstance(raw, str):
        return None

    # Remove common thousand separators and unify dashes
    text = raw.replace("'", "").replace(",", " ").replace("–", "-")
    nums = re.findall(r"\d+", text)
    if not nums:
        return None
    try:
        values = [int(n) for n in nums]
    except Exception:
        return None
    if not values:
        return None
    # Use the maximum value (we want the highest miles figure)
    return max(values)


def _parse_duration_display_to_minutes(text: Any) -> Optional[int]:
    if not isinstance(text, str) or not text.strip():
        return None
    t = text.strip().lower()
    pattern = re.compile(r"(?:(?P<h>\d+)\s*h)?\s*(?:(?P<m>\d+)\s*m)?")
    m = pattern.match(t)
    if not m:
        return None
    hours = m.group("h")
    mins = m.group("m")
    total = 0
    if hours:
        total += int(hours) * 60
    if mins:
        total += int(mins)
    return total or None


def _format_duration_minutes(minutes: Any) -> Optional[str]:
    if not isinstance(minutes, (int, float)) or minutes <= 0:
        return None
    mins_int = int(minutes)
    h, m = divmod(mins_int, 60)
    if h and m:
        return f"{h}h {m}m"
    if h:
        return f"{h}h"
    if m:
        return f"{m}m"
    return None


def _estimate_duration_by_iata(origin_iata: Any, destination_iata: Any, direct: Optional[bool] = None) -> Optional[int]:
    """Estimate flight duration (minutes) from IATA pair.

    Uses a slower average speed for non-direct itineraries and adds a 90m
    buffer for connections. Returns None if IATAs are missing or distance
    cannot be computed.
    """

    if not origin_iata or not destination_iata:
        return None

    try:
        dist = great_circle_miles(str(origin_iata).strip().upper(), str(destination_iata).strip().upper())
    except Exception:
        dist = None
    if not dist:
        return None

    speed_mph = 450.0 if direct else 400.0
    minutes_val = int(round((dist / speed_mph) * 60))
    if direct is False:
        minutes_val += 90  # connection buffer

    return max(minutes_val, 40)


def _infer_baggage_from_text(text: Any) -> Tuple[Optional[bool], Optional[int], Optional[float], Optional[str]]:
    """Infer baggage inclusion and allowance from free text.

    Returns (included, pieces, kg, display_text).
    """

    if not isinstance(text, str) or not text.strip():
        return None, None, None, None

    lower = text.lower()
    # Normalize common curly apostrophes for robust matching
    lower = lower.replace("’", "'").replace("`", "'").replace("´", "'")

    included: Optional[bool] = None

    neg_tokens = [
        "no checked",
        "no baggage",
        "no luggage",
        "doesn't include",
        "does not include",
        "doesnt include",
        "without checked",
        "not include",
        "doesn't include checked luggage",
        "does not include checked luggage",
    ]

    pos_tokens = [
        "baggage included",
        "luggage included",
        "incluido equipaje",
        "incluye equipaje",
        "includes baggage",
        "includes luggage",
        "checked luggage",
        "checked baggage",
    ]

    has_neg = any(tok in lower for tok in neg_tokens)
    has_pos = any(tok in lower for tok in pos_tokens)

    if has_neg:
        included = False
    elif has_pos:
        included = True

    kg_val: Optional[float] = None
    pieces_val: Optional[int] = None

    # Detect patterns like "2x23 kg" or "2×23kg"
    combo_match = re.search(r"(\d+)\s*[x×]\s*(\d+(?:\.\d+)?)\s*kg", lower)
    if combo_match:
        try:
            pieces_val = int(combo_match.group(1))
            kg_val = float(combo_match.group(2))
            if included is None:
                included = True
        except Exception:
            pieces_val = pieces_val or None
            kg_val = kg_val or None
    else:
        kg_match = re.search(r"(\d+(?:\.\d+)?)\s*kg", lower)
        if kg_match:
            try:
                kg_val = float(kg_match.group(1))
            except Exception:
                kg_val = None
            if kg_val is not None:
                pieces_val = 1
                if included is None:
                    included = True

    return included, pieces_val, kg_val, text.strip()


_CABIN_CLASS_CANONICAL: Dict[str, str] = {
    # Single-letter booking class codes → canonical display names
    "y": "Economy", "m": "Economy", "h": "Economy", "k": "Economy",
    "l": "Economy", "q": "Economy", "t": "Economy", "v": "Economy",
    "x": "Economy", "b": "Economy", "e": "Economy", "n": "Economy",
    "o": "Economy", "s": "Economy", "economy": "Economy",
    "w": "Premium Economy", "p": "Premium Economy",
    "premium economy": "Premium Economy", "premium_economy": "Premium Economy",
    "c": "Business", "j": "Business", "d": "Business", "z": "Business",
    "r": "Business", "business": "Business",
    "f": "First", "first": "First",
}


def _normalize_cabin_class(val: Any) -> Optional[str]:
    """Map single-letter booking codes and lowercase strings to canonical cabin names."""
    if val is None:
        return None
    s = str(val).strip()
    return _CABIN_CLASS_CANONICAL.get(s.lower(), s) if s else None


def _normalize_miles_display(miles_text: Any) -> Optional[str]:
    """Ensure miles display is always in 'Program · Amount' order.

    TravelDealz sometimes returns 'Amount · Program' (e.g. "6'784 · Flying Blue").
    This function reverses such strings to 'Program · Amount'.
    """
    if miles_text is None:
        return None
    s = str(miles_text).strip()
    if not s or s == "—":
        return s or None
    if " · " not in s:
        return s
    parts = s.split(" · ", 1)
    left, right = parts[0].strip(), parts[1].strip()
    # If the left part looks like a number (all digits, apostrophes, commas), reverse
    import re as _re
    if _re.match(r"^[\d\s',.']+$", left):
        return f"{right} · {left}"
    return s


def _coerce_numeric_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure numeric fields are stored as ints/floats, not strings.

    This prevents DB errors like "invalid input syntax for type integer".
    Also normalizes cabin_class to canonical display names.
    """

    coerced = dict(row)

    int_fields = [
        "baggage_pieces_included",
        "baggage_allowance_kg",
        "flight_duration_minutes",
    ]

    for key in int_fields:
        val = coerced.get(key)
        if val is None:
            continue
        try:
            if isinstance(val, str) and val.strip() == "":
                coerced[key] = None
            else:
                coerced[key] = int(float(val))
        except Exception:
            coerced[key] = None

    # price can be float; if string, try to parse
    if "price" in coerced:
        val = coerced.get("price")
        if isinstance(val, str):
            try:
                coerced["price"] = float(val)
            except Exception:
                coerced["price"] = None

    # Normalize cabin_class codes to display names (e.g. "Y" → "Economy")
    if "cabin_class" in coerced:
        coerced["cabin_class"] = _normalize_cabin_class(coerced.get("cabin_class"))

    return coerced


def _normalize_deal_fields(deal: Dict[str, Any]) -> Dict[str, Any]:
    """Coalesce and infer commonly-missing fields before scoring/persist."""

    normalized = dict(deal)

    # If itineraries are present, estimate duration per itinerary without copying to deal level
    if isinstance(normalized.get("itineraries"), list):
        for it in normalized["itineraries"]:
            if not isinstance(it, dict):
                continue
            if it.get("flight_duration_minutes") and it.get("flight_duration_display"):
                continue

            oi = it.get("origin_iata")
            di = it.get("destination_iata")
            direct_flag = it.get("direct") if isinstance(it.get("direct"), bool) else None
            mins_est = _estimate_duration_by_iata(oi, di, direct_flag)
            if mins_est:
                it["flight_duration_minutes"] = mins_est
                disp = _format_duration_minutes(mins_est)
                if disp:
                    it["flight_duration_display"] = disp

    # Miles: keep as-is (text). If an LLM provides a numeric miles_estimate,
    # keep it in miles_estimate; we don't overwrite the user-facing miles string.

    # Duration: derive minutes from display or display from minutes
    if not normalized.get("flight_duration_minutes") and normalized.get("flight_duration_display"):
        mins = _parse_duration_display_to_minutes(normalized.get("flight_duration_display"))
        if mins:
            normalized["flight_duration_minutes"] = mins
    if not normalized.get("flight_duration_display") and normalized.get("flight_duration_minutes"):
        disp = _format_duration_minutes(normalized.get("flight_duration_minutes"))
        if disp:
            normalized["flight_duration_display"] = disp

    # Airline/aircraft: map codes (UA, 77W) to readable names/models
    readable_airline = _resolve_airline_name(normalized.get("airline"), normalized.get("airline_code"))
    if readable_airline:
        normalized["airline"] = readable_airline

    readable_ac = _resolve_aircraft_model(normalized.get("aircraft"), normalized.get("aircraft"))
    if readable_ac:
        normalized["aircraft"] = readable_ac

    # Baggage inference from any available text snippets.
    # cabin_baggage is an in-memory field from the article parser (not a Supabase column).
    bag_text_sources = [
        normalized.get("baggage_allowance_display"),
        normalized.get("baggage_summary"),
        normalized.get("cabin_baggage"),
    ]
    for txt in bag_text_sources:
        included, pieces, kg_val, display = _infer_baggage_from_text(txt)
        if normalized.get("baggage_included") is None and included is not None:
            normalized["baggage_included"] = included
        if normalized.get("baggage_pieces_included") is None and pieces is not None:
            normalized["baggage_pieces_included"] = pieces
        if normalized.get("baggage_allowance_kg") is None and kg_val is not None:
            normalized["baggage_allowance_kg"] = int(kg_val) if kg_val == int(kg_val) else kg_val
        if normalized.get("baggage_allowance_display") is None and display:
            normalized["baggage_allowance_display"] = display

    # Duration: if no minutes/display but we have IATAs, estimate at 500 mph
    if not normalized.get("flight_duration_minutes") and normalized.get("origin_iata") and normalized.get("destination_iata"):
        try:
            gc = great_circle_miles(normalized.get("origin_iata"), normalized.get("destination_iata"))
        except Exception:
            gc = None
        if gc and gc > 0:
            mins_est = int(round((gc / 500.0) * 60))
            if mins_est > 0:
                normalized["flight_duration_minutes"] = mins_est
                disp = _format_duration_minutes(mins_est)
                if disp:
                    normalized["flight_duration_display"] = disp

    origin_filled = _resolve_city_name(normalized.get("origin") or normalized.get("origin_city"), normalized.get("origin_iata"))
    if origin_filled:
        normalized["origin"] = origin_filled
    destination_filled = _resolve_city_name(normalized.get("destination") or normalized.get("destination_city"), normalized.get("destination_iata"))
    if destination_filled:
        normalized["destination"] = destination_filled

    # Fetch Unsplash image for destination (always overrides source-provided images).
    dest_city_for_img = normalized.get("destination") or normalized.get("destination_iata")
    if dest_city_for_img:
        try:
            from services.unsplash_service import fetch_destination_image  # type: ignore
            img_url = fetch_destination_image(str(dest_city_for_img))
            if img_url:
                normalized["image"] = img_url
        except Exception:
            pass

    # Build a German-language travel period display string.
    if not normalized.get("travel_period_display"):
        normalized["travel_period_display"] = _build_travel_period_display(normalized)

    # Add Skyscanner affiliate link when route is known (always refresh so adultsv2=2 is used).
    try:
        from services.skyscanner_links import build_skyscanner_link  # type: ignore
        url = build_skyscanner_link(
            origin_iata=normalized.get("origin_iata") or "",
            dest_iata=normalized.get("destination_iata") or "",
            depart_date=normalized.get("departure_date") or normalized.get("date_out"),
            return_date=normalized.get("return_date") or normalized.get("date_in"),
            cabin_class=normalized.get("cabin_class"),
        )
        if url:
            normalized["skyscanner_url"] = url
    except Exception:
        pass

    # Compute stops from itineraries (segments - 1 per outbound slice).
    # Only set stops if itineraries contain actual segment data; otherwise leave as NULL
    # to avoid incorrectly showing 0 stops for flights where we don't know the routing.
    if normalized.get("stops") is None:
        itins = normalized.get("itineraries")
        if isinstance(itins, list) and itins:
            max_segs = max(
                (len(it.get("segments", [])) for it in itins if isinstance(it, dict)),
                default=0,
            )
            if max_segs > 0:
                normalized["stops"] = max(0, max_segs - 1)

    return normalized


_DE_MONTHS = [
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
]


def _build_travel_period_display(deal: Dict[str, Any]) -> Optional[str]:
    """Build a German travel period string like '15. Jan – 22. Jan 2026'
    or 'Januar bis März 2026' from available date/period fields."""

    dep = deal.get("departure_date") or deal.get("date_out")
    ret = deal.get("return_date") or deal.get("date_in")

    if dep and ret:
        try:
            d1 = datetime.fromisoformat(str(dep))
            d2 = datetime.fromisoformat(str(ret))
            m1 = _DE_MONTHS[d1.month - 1][:3]
            m2 = _DE_MONTHS[d2.month - 1][:3]
            if d1.year == d2.year:
                return f"{d1.day}. {m1} – {d2.day}. {m2} {d1.year}"
            return f"{d1.day}. {m1} {d1.year} – {d2.day}. {m2} {d2.year}"
        except Exception:
            pass

    # Try to use cheap_months as a range (e.g. [1, 2, 3])
    cheap_months = deal.get("cheap_months")
    if isinstance(cheap_months, list) and cheap_months:
        try:
            months = sorted({int(m) for m in cheap_months if 1 <= int(m) <= 12})
            if len(months) == 1:
                return _DE_MONTHS[months[0] - 1]
            year = datetime.now().year
            if dep:
                try:
                    year = datetime.fromisoformat(str(dep)).year
                except Exception:
                    pass
            return f"{_DE_MONTHS[months[0] - 1]} bis {_DE_MONTHS[months[-1] - 1]} {year}"
        except Exception:
            pass

    # Fall back to date_range string if present
    date_range = str(deal.get("date_range") or "").strip()
    return date_range or None


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


def _extract_secretflying_from_booking_url(booking_url: str) -> Dict[str, Any]:
    """Best-effort extraction of flight data from a SecretFlying booking_url.

    Designed primarily for Skyscanner-type links, where the URL usually
    includes parameters like origin/destination/outboundDate/inboundDate, but
    also tries to leverage patterns in the path (IATA segments, dates).
    """

    try:
        parsed = urlparse(booking_url)
        qs = parse_qs(parsed.query)

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

        # Normalize possible internal aggregator codes (e.g. SINS -> SIN in Skyscanner)
        if dest_code == "SINS":
            dest_code = "SIN"

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

        rtn = _first_matching_param(qs, "rtn", "roundtrip") or ""
        rtn = rtn.strip()

        # If there are no clear codes in the query, try to deduce them from the path
        if not origin_code or not dest_code:
            segments = [seg for seg in parsed.path.split("/") if seg]
            iata_candidates = [seg.upper() for seg in segments if len(seg) == 3 and seg.isalpha()]
            if len(iata_candidates) >= 2:
                if not origin_code:
                    origin_code = iata_candidates[0]
                if not dest_code:
                    dest_code = iata_candidates[1]

            if not dep or not ret:
                date_like = [seg for seg in segments if any(ch.isdigit() for ch in seg) and len(seg) in {6, 8, 10}]
                if date_like:
                    if not dep:
                        dep = date_like[0]
                    if len(date_like) > 1 and not ret:
                        ret = date_like[1]

        result: Dict[str, Any] = {}

        if origin_code and len(origin_code) == 3 and origin_code.isalpha():
            result["origin_iata"] = origin_code
        if dest_code and len(dest_code) == 3 and dest_code.isalpha():
            result["destination_iata"] = dest_code
        if dep:
            result["departure_date"] = dep
        if ret:
            result["return_date"] = ret
        if cabin_raw:
            result["cabin_class"] = cabin_raw.upper()
        if rtn:
            result["roundtrip"] = (rtn == "1" or rtn.lower() == "true")

        return result
    except Exception:
        return {}


def _normalize_price(price: float | None, currency: str | None) -> float | None:
    if price is None:
        return None
    if not currency:
        currency = "EUR"
    factor = _CURRENCY_TO_EUR.get(currency.upper(), 1.0)
    try:
        return float(price) * factor
    except Exception:
        return None


_SWISS_AIRPORTS = {"ZRH", "GVA", "BSL", "BRN", "LUG", "SIR"}


def _get_origin_iata_filter() -> set[str]:
    """Return a set of origin IATA codes to keep, from env ORIGIN_IATA_FILTER.

    Example: ORIGIN_IATA_FILTER="ZRH,BSL" -> {"ZRH", "BSL"}.
    Defaults to Swiss airports (ZRH, GVA, BSL, BRN, LUG, SIR) when not set.
    Set ORIGIN_IATA_FILTER="" explicitly to disable filtering.
    """

    raw = os.getenv("ORIGIN_IATA_FILTER")
    if raw is None:
        return _SWISS_AIRPORTS
    raw = raw.strip()
    if not raw:
        return set()
    return {part.strip().upper() for part in raw.split(",") if part.strip()}


def _load_duffel_benchmarks(limit: int | None = None) -> Dict[Tuple[str, str, int], float]:
    """Load Duffel benchmarks from the unified deals table.

    Returns a mapping ``(origin_iata, destination_iata, month) -> best_price_eur``.
    Queries rows with ``source='duffel'`` (or legacy ``source='amadeus'``).
    """

    if not _client:
        return {}

    max_rows = limit or int(os.getenv("DEALS_DUFFEL_BENCHMARK_LIMIT", "5000"))
    origin_filter = _get_origin_iata_filter()

    try:
        rsp = (
            _client.table("deals")
            .select("origin_iata,destination_iata,date_out,price,currency,source")
            .in_("source", ["duffel", "amadeus"])
            .limit(max_rows)
            .execute()
        )
    except Exception:
        return {}

    rows = getattr(rsp, "data", []) or []
    benchmarks: Dict[Tuple[str, str, int], float] = {}

    for row in rows:
        origin = str(row.get("origin_iata") or "").strip().upper()
        dest = str(row.get("destination_iata") or "").strip().upper()
        date_out = row.get("date_out")
        if not origin or not dest or not date_out:
            continue

        # We only care about benchmarks for origins within the filter
        # (e.g. Swiss airports like ZRH/BSL) when said filter
        # is configured.
        if origin_filter and origin not in origin_filter:
            continue

        month: Optional[int] = None
        try:
            month = datetime.fromisoformat(str(date_out)).month
        except Exception:
            try:
                parts = str(date_out).split("-")
                if len(parts) >= 2:
                    month = int(parts[1])
            except Exception:
                month = None

        if month is None:
            continue

        price_eur = _normalize_price(row.get("price"), row.get("currency"))
        if price_eur is None:
            continue

        key = (origin, dest, month)
        best = benchmarks.get(key)
        if best is None or price_eur < best:
            benchmarks[key] = price_eur

    return benchmarks


def _score_raw(price_eur: float | None) -> float:
    """Simple raw score: cheaper deals get higher score."""
    if price_eur is None or price_eur <= 0:
        return 5.0
    return 1000.0 / price_eur


def _assign_tier(deal: Dict[str, Any]) -> str:
    """Return 'premium' for Business/First/Premium Economy, else 'free'."""
    cabin = str(deal.get("cabin_class") or "").strip().upper()
    title = str(deal.get("title") or "").lower()
    if cabin in {"BUSINESS", "J", "C", "D", "Z", "FIRST", "F", "PREMIUM ECONOMY", "PREMIUM_ECONOMY", "W", "P"}:
        return "premium"
    if "business" in title or "premium economy" in title or "first class" in title:
        return "premium"
    return "free"


def _extract_route_month(deal: Dict[str, Any]) -> Optional[Tuple[str, str, int]]:
    """Best-effort extraction of (origin_iata, destination_iata, month).

    Used so the scorer can compare a deal price vs. route/month benchmarks
    stored in best_deals_amadeus.
    """

    origin = str(deal.get("origin_iata") or "").strip().upper()
    dest = str(deal.get("destination_iata") or "").strip().upper()

    if not origin or not dest:
        return None

    dep_raw = deal.get("departure_date") or deal.get("date_out")
    if not dep_raw:
        return None

    month: Optional[int] = None
    try:
        month = datetime.fromisoformat(str(dep_raw)).month
    except Exception:
        try:
            parts = str(dep_raw).split("-")
            if len(parts) >= 2:
                month = int(parts[1])
        except Exception:
            month = None

    if month is None:
        return None

    return origin, dest, month


def _clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def _score_single_deal(
    d: Dict[str, Any],
    benchmarks: Optional[Dict[Tuple[str, str, int], float]],
) -> float:
    """Compute a weighted raw score for a single deal (higher = better deal).

    Weights:
      40% price competitiveness vs benchmark
      40% duration efficiency vs great-circle estimate
      20% number of stops (0 stops = best)
    """

    # ── 1. Price competitiveness (35%) ───────────────────────────────────────
    price_eur = _normalize_price(d.get("price"), d.get("currency"))
    if price_eur and price_eur > 0 and benchmarks:
        route = _extract_route_month(d)
        bench = benchmarks.get(route) if route else None
        if bench and bench > 0:
            ratio = price_eur / bench
            # ratio 0.5 or better → full marks; ratio 2.0+ → zero marks
            price_score = _clamp01(1.0 - (ratio - 0.5) / 1.5)
        else:
            # No benchmark: use inverse-price heuristic scaled to ~500 EUR being "average"
            price_score = _clamp01(500.0 / (price_eur + 1))
    elif price_eur and price_eur > 0:
        price_score = _clamp01(500.0 / (price_eur + 1))
    else:
        price_score = 0.3  # unknown price: moderate score

    # ── 2. Duration efficiency (25%) ─────────────────────────────────────────
    actual_mins = d.get("flight_duration_minutes")
    origin_iata = d.get("origin_iata")
    dest_iata = d.get("destination_iata")
    if actual_mins and actual_mins > 0 and origin_iata and dest_iata:
        try:
            gc_miles = great_circle_miles(origin_iata, dest_iata)
            # Assume ~500 mph cruising → expected mins
            expected_mins = int((gc_miles / 500.0) * 60) if gc_miles else actual_mins
            # Efficiency: expected / actual (1.0 = perfect, <1 = slower than direct)
            duration_score = _clamp01(expected_mins / max(actual_mins, 1))
        except Exception:
            duration_score = 0.5
    else:
        duration_score = 0.5  # unknown duration: neutral

    # ── 3. Stops (15%) ───────────────────────────────────────────────────────
    stops = d.get("stops")
    if stops is None:
        stops_str = str(d.get("flight_stops") or d.get("itinerary_stops") or "").strip()
        try:
            stops = int(stops_str) if stops_str.isdigit() else None
        except Exception:
            stops = None
    if stops is None:
        # Infer from duration: >8h with no data → likely 1 stop
        stops = 1 if (actual_mins and actual_mins > 480) else 0
    stops_score = _clamp01(1.0 - stops / 2.0)  # 0 stops=1.0, 1=0.5, 2+=0.0

    # ── Error-fare bonus ──────────────────────────────────────────────────────
    title = (d.get("title") or "").lower()
    error_bonus = 0.15 if ("error fare" in title or "mistake fare" in title) else 0.0

    # Weights: 40% price, 40% duration, 20% stops
    return (
        0.40 * price_score
        + 0.40 * duration_score
        + 0.20 * stops_score
        + error_bonus
    )


def score_deals(
    deals: List[Dict[str, Any]],
    benchmarks: Optional[Dict[Tuple[str, str, int], float]] = None,
) -> List[Dict[str, Any]]:
    """Add a `score` field (0–100) to each deal and sort highest first.

    Formula (weights): price 40%, duration 40%, stops 20%.
    """
    if not deals:
        return []

    raw_scores = [_score_single_deal(d, benchmarks) for d in deals]
    min_s = min(raw_scores)
    max_s = max(raw_scores)
    span = max_s - min_s if max_s > min_s else 1.0

    normalized: List[Dict[str, Any]] = []
    for d, s in zip(deals, raw_scores):
        deal_copy = dict(d)
        deal_copy["score"] = round((s - min_s) / span * 100.0, 2)
        normalized.append(deal_copy)

    normalized.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return normalized


def render_html_snippet(deals: List[Dict[str, Any]], max_items: int | None = None) -> str:
    """Render a self-contained HTML snippet for the given deals.

    The snippet is intentionally fragment-only (<div>...) so it can be
    embedded into any page or CMS.
    """
    if not deals:
        return "<!-- No deals available -->"

    if max_items is not None and max_items > 0:
        deals = deals[:max_items]

    parts: List[str] = []
    parts.append(
        "<div class=\"flight-deals-grid\" "
        "style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));"
        "gap:1rem;font-family:system-ui,sans-serif;\">"
    )

    def _env_display_currency() -> str | None:
        cur = (os.getenv("DISPLAY_CURRENCY") or "").strip().upper()
        return cur or None

    def _convert_price(amount: float, from_cur: str, to_cur: str) -> float | None:
        f = (from_cur or "").strip().upper()
        t = (to_cur or "").strip().upper()
        if not f or not t or f == t:
            return amount
        if f == "EUR" and t == "CHF":
            raw = (os.getenv("FX_EUR_TO_CHF") or "").strip()
            if not raw:
                return None
            try:
                rate = float(raw)
                if rate <= 0:
                    return None
                return amount * rate
            except Exception:
                return None
        return None

    def _format_price_display(price: Any, currency: str) -> str:
        cur = (currency or "").strip().upper() or "EUR"
        if not isinstance(price, (int, float)):
            return f"Preis N/A {cur}".strip()

        target = _env_display_currency()
        if target and target != cur:
            converted = _convert_price(float(price), cur, target)
            if converted is not None:
                return f"ab {converted:.0f} {target}".strip()
        return f"ab {float(price):.0f} {cur}".strip()

    for d in deals:
        title = d.get("title") or "Untitled deal"
        link = d.get("link") or "#"
        price = d.get("price")
        currency = d.get("currency") or ""
        source = d.get("source") or ""
        score = d.get("score")

        cabin = str(d.get("cabin_class") or "").strip().upper()
        is_business = cabin in {"BUSINESS", "J", "C"} or "business" in str(title).lower()
        flight_line = d.get("flight") or ""

        # Optional extra info for newsletter-style snippets
        duration_display = d.get("flight_duration_display")
        if not duration_display and isinstance(d.get("flight_duration_minutes"), (int, float)):
            mins = int(d["flight_duration_minutes"])
            if mins > 0:
                h, m = divmod(mins, 60)
                if h and m:
                    duration_display = f"{h}h {m}m"
                elif h:
                    duration_display = f"{h}h"
                elif m:
                    duration_display = f"{m}m"

        baggage_short = format_baggage_short_de(d)

        miles_val = d.get("miles") or d.get("miles_estimate")
        miles_display = str(miles_val) if miles_val is not None else None

        # Escape user/LLM-provided strings for safe HTML rendering.
        title_e = escape(str(title))
        link_e = escape(str(link), quote=True)
        source_e = escape(str(source))
        flight_line_e = escape(str(flight_line)) if flight_line else ""
        duration_e = escape(str(duration_display)) if duration_display else ""
        baggage_e = escape(str(baggage_short)) if baggage_short else ""
        miles_e = escape(str(miles_display)) if miles_display else ""

        price_str = _format_price_display(price, currency)

        score_str = f"{score:.1f}" if isinstance(score, (int, float)) else "-"

        card_html = (
            "<article class=\"flight-deal-card\" "
            "style=\"border:1px solid #e0e0e0;border-radius:0.75rem;padding:0.9rem;"
            "background:#fff;box-shadow:0 4px 8px rgba(15,23,42,0.06);"
            "display:flex;flex-direction:column;justify-content:space-between;min-height:140px;\" "
            f"data-score=\"{score_str}\">"
            f"<h3 style=\"margin:0 0 .35rem;font-size:0.95rem;line-height:1.3;\">"
            f"<a href=\"{link_e}\" target=\"_blank\" rel=\"noopener noreferrer\" "
            "style=\"color:#0f172a;text-decoration:none;\">"
            f"{title_e}</a>"
            + ("" if not is_business else " <span style=\"font-size:0.72rem;background:#fff7ed;color:#9a3412;border-radius:999px;padding:0.05rem 0.45rem;vertical-align:middle;\">BUSINESS</span>")
            + "</h3>"
            + ("" if not flight_line_e else f"<div style=\"margin:-0.1rem 0 .35rem;font-size:0.82rem;color:#334155;\">{flight_line_e}</div>")
            + "<div style=\"display:flex;align-items:center;justify-content:space-between;"
            "margin-top:.35rem;font-size:0.86rem;color:#475569;\">"
            f"<span style=\"font-weight:600;color:#047857;\">{price_str}</span>"
            f"<span style=\"font-size:0.78rem;background:#eff6ff;color:#1d4ed8;"
            f"border-radius:999px;padding:0.1rem 0.55rem;\">{source_e}</span>"
            "</div>"
            "<div style=\"margin-top:.45rem;display:flex;justify-content:space-between;"
            "align-items:center;font-size:0.78rem;color:#64748b;\">"
            f"<span>Score: <strong>{score_str}</strong>/100</span>"
            "<span style=\"opacity:0.8;\">via snapcore</span>"
            "</div>"
            # Optional flight duration + baggage line when available
            "<div style=\"margin-top:.2rem;font-size:0.76rem;color:#6b7280;display:flex;flex-wrap:wrap;gap:0.35rem;\">"
            f"<span>{'⏱ ' + duration_e if duration_e else ''}</span>"
            f"<span>{'🧳 Gepäck: ' + baggage_e if baggage_e else ''}</span>"
            f"<span>{'🛫 ' + miles_e if miles_e else ''}</span>"
            "</div>"
            "</article>"
        )
        parts.append(card_html)

    parts.append("</div>")
    return "".join(parts)


def run_deals_pipeline(
    limit: int = 50,
    persist: bool = True,
    max_items_html: int | None = None,
    sources: set[str] | None = None,
) -> Dict[str, Any]:
    """End-to-end pipeline: scrape, optional persist, score and HTML.

    Returns a structured dict that can be consumed by API endpoints or
    automation scripts.

    If ``sources`` is provided, it must be a subset of
    {"travel-dealz", "secretflying"} and takes priority over the
    SCRAPING_URL environment configuration. If None, the current
    behaviour based on SCRAPING_URL is respected.
    """
    sources_enabled = sources or _parse_scraping_sources()

    # Allow per-source scraping limits configurable via environment
    # (e.g. from run_config: scraping_limit_travel_dealz, scraping_limit_secretflying).
    try:
        td_limit_env = os.getenv("SCRAPING_LIMIT_TRAVEL_DEALZ")
        td_limit_total = int(td_limit_env) if td_limit_env else limit
    except Exception:
        td_limit_total = limit

    # Ensure the global `limit` remains the hard ceiling for the run.
    # This prevents confusing cases like: mode has scrape.traveldealz=10 but user runs `--limit 5`.
    if limit <= 0:
        td_limit_total = 0
    else:
        try:
            td_limit_total = max(0, min(int(td_limit_total), int(limit)))
        except Exception:
            td_limit_total = int(limit)

    try:
        sf_limit_env = os.getenv("SCRAPING_LIMIT_SECRETFLYING")
        sf_limit = int(sf_limit_env) if sf_limit_env else limit
    except Exception:
        sf_limit = limit

    if limit <= 0:
        sf_limit = 0
    else:
        try:
            sf_limit = max(0, min(int(sf_limit), int(limit)))
        except Exception:
            sf_limit = int(limit)

    # Load URLs already marked as 'done' in source_articles to avoid
    # reprocessing articles that have already been scraped and persisted.
    # We only apply this filter when persisting to Supabase;
    # for runs with persist=False we want to see previously
    # processed articles as well.
    if persist:
        done_by_source = _load_done_article_urls()
    else:
        done_by_source = {"travel-dealz": set(), "secretflying": set()}

    raw_deals: List[Dict[str, Any]] = []
    if "travel-dealz" in sources_enabled:
        desired_td_new = td_limit_total
        done_td = done_by_source.get("travel-dealz", set())

        logger.info(
            "[deals_pipeline] travel-dealz plan desired_new=%s done_known=%s persist=%s",
            desired_td_new,
            len(done_td),
            persist,
        )

        # We over-scrape only when there are already done articles and it's likely
        # we'll discard them. If there are no done articles, we don't add a buffer.
        #
        # Important: SCRAPING_LIMIT_TRAVEL_DEALZ represents how many deals
        # we want to PRODUCE (desired_td_new), not how many links we want to read.
        # That's why we can request more candidates (overfetch) and then trim.
        if persist and done_td:
            try:
                min_fetch_floor = int(os.getenv("SCRAPING_OVERFETCH_TRAVEL_DEALZ_MIN", "10"))
            except Exception:
                min_fetch_floor = 10

            try:
                max_fetch_cap = int(os.getenv("SCRAPING_OVERFETCH_TRAVEL_DEALZ_MAX", "50"))
            except Exception:
                max_fetch_cap = 50

            # Never cap below the target: if we want 500 new deals,
            # a cap of 50 would make it impossible to fulfil the contract.
            # The cap is interpreted as "maximum candidates", not as
            # "maximum produced deals".
            try:
                max_fetch_cap = max(int(max_fetch_cap), int(desired_td_new))
            except Exception:
                max_fetch_cap = max_fetch_cap

            overlap_hint = min(len(done_td), desired_td_new)
            buffer_td = max(desired_td_new // 2, overlap_hint)

            # If desired is very small (e.g. 2), an overfetch of x2 often
            # falls short and shows the "requested_total=4" effect.
            # In that case we apply a minimum floor (min_fetch_floor).
            fetch_td_total = max(desired_td_new + buffer_td, desired_td_new * 2, min_fetch_floor)
            fetch_td_total = min(fetch_td_total, max_fetch_cap)
        else:
            fetch_td_total = desired_td_new

        def _split_td_budget(total: int) -> tuple[int, int]:
            """Split candidate budget between .de and .com.

            We keep a preference for .de (typically richer fields), but we
            also want .com represented when requesting multiple deals.
            """

            if total <= 0:
                return 0, 0

            # 70/30 split, with floor guarantees.
            de_budget = int(round(total * 0.7))
            de_budget = max(0, min(de_budget, total))
            com_budget = total - de_budget

            # Always fetch at least 1 from .de.
            if de_budget == 0:
                de_budget = 1
                com_budget = max(0, total - de_budget)

            # When asking for multiple deals, reserve at least 1 for .com.
            if total >= 2 and com_budget == 0:
                com_budget = 1
                de_budget = max(0, total - com_budget)

            return de_budget, com_budget

        fetch_de_budget, fetch_com_budget = _split_td_budget(fetch_td_total)

        # Allow forcing a specific domain via env (set by run_config).
        # Values: de | com | both (default).
        td_domain = str(os.getenv("SCRAPING_TRAVEL_DEALZ_DOMAIN", "both") or "both").strip().lower()
        if td_domain in {".de", "de", "germany", "de-only"}:
            fetch_de_budget, fetch_com_budget = fetch_td_total, 0
        elif td_domain in {".com", "com", "intl", "us", "com-only"}:
            fetch_de_budget, fetch_com_budget = 0, fetch_td_total
        else:
            td_domain = "both"

        # Fetch from both domains to cover travel-dealz.de and travel-dealz.com.
        # We interleave candidates later to avoid starving .com when .de has many.
        td_de = get_travel_dealz_de(limit=fetch_de_budget) if fetch_de_budget > 0 else []
        td_com = get_travel_dealz(limit=fetch_com_budget) if fetch_com_budget > 0 else []

        logger.info(
            "[deals_pipeline] travel-dealz fetched de=%s com=%s requested_total=%s",
            len(td_de),
            len(td_com),
            fetch_td_total,
        )

        def _interleave(a: list[Dict[str, Any]], b: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
            out: list[Dict[str, Any]] = []
            for i in range(max(len(a), len(b))):
                if i < len(a):
                    out.append(a[i])
                if i < len(b):
                    out.append(b[i])
            return out

        td_candidates: List[Dict[str, Any]] = []
        for d in _interleave(td_de, td_com):
            link = str(d.get("link") or "").strip()
            if link and link in done_td:
                continue
            td_candidates.append(d)

        # Maintain the contract: produce at most desired_td_new deals
        # (even though we read more candidates to skip the done ones).
        if desired_td_new > 0 and len(td_candidates) > desired_td_new:
            td_candidates = td_candidates[:desired_td_new]

        if td_candidates:
            try:
                selected_preview = [
                    {
                        "source": str(x.get("source") or ""),
                        "title": str(x.get("title") or "")[:80],
                        "link": str(x.get("link") or ""),
                    }
                    for x in td_candidates
                ]
            except Exception:
                selected_preview = []
            logger.info("[deals_pipeline] travel-dealz selected=%s", selected_preview)

        raw_deals.extend(td_candidates)

        if persist and not raw_deals and (td_com or td_de):
            logger.warning(
                "[deals_pipeline] travel-dealz produced 0 new deals after filtering; "
                "all scraped URLs may already be marked done in source_articles"
            )

    if "secretflying" in sources_enabled:
        desired_sf_new = sf_limit
        done_sf = done_by_source.get("secretflying", set())

        if persist and done_sf:
            overlap_hint = min(len(done_sf), desired_sf_new)
            buffer_sf = max(desired_sf_new // 2, overlap_hint)
            buffer_sf = min(buffer_sf, desired_sf_new)
            fetch_sf = min(desired_sf_new + buffer_sf, desired_sf_new * 2)
        else:
            fetch_sf = desired_sf_new

        logger.info(
            "[deals_pipeline] secretflying fetch desired_new=%s done_known=%s fetch_with_buffer=%s persist=%s",
            desired_sf_new,
            len(done_sf),
            fetch_sf,
            persist,
        )

        try:
            sf = get_secretflying(limit=fetch_sf)
            logger.info("[deals_pipeline] secretflying fetched=%s", len(sf))
        except Exception as e:  # pragma: no cover - network/provider failures
            logger.warning(
                "[deals_pipeline] secretflying fetch failed; skipping source. error_type=%s error=%r",
                type(e).__name__,
                e,
            )
            sf = []

        for d in sf:
            link = str(d.get("link") or "").strip()
            if link and link in done_sf:
                continue
            raw_deals.append(d)

    logger.info(
        "[deals_pipeline] counts raw_deals=%s cap_limit=%s sources_enabled=%s persist=%s",
        len(raw_deals),
        limit,
        sorted(sources_enabled),
        persist,
    )

    if len(raw_deals) > limit:
        logger.info(
            "[deals_pipeline] capping raw_deals before=%s after=%s",
            len(raw_deals),
            limit,
        )
        raw_deals = raw_deals[:limit]

    # Cheap enrichment specific to Travel-Dealz: itineraries and metadata per article.
    # This does NOT use OpenAI; it relies only on article scraping.
    # Additionally: we filter out promotions without itineraries (e.g. gift cards) to avoid
    # persisting them as deals.
    filtered_raw_deals: List[Dict[str, Any]] = []
    for d in raw_deals:
        link = (d.get("link") or "").lower()
        if not link or ("travel-dealz.com" not in link and "travel-dealz.de" not in link):
            filtered_raw_deals.append(d)
            continue
        try:
            article = parse_travel_dealz_article(d["link"])
        except Exception:
            filtered_raw_deals.append(d)
            continue
        if not isinstance(article, dict) or article.get("status") != "ok":
            filtered_raw_deals.append(d)
            continue

        # Skip non-flight promos (e.g. vouchers). Mark them done so we don't re-scrape.
        if not _is_travel_dealz_flight_article(article, d.get("link") or ""):
            if persist:
                _mark_source_article_done(d.get("link") or "", source="travel-dealz", note="ignored_non_flight")
            continue

        itins = article.get("itineraries")
        if isinstance(itins, list) and itins:
            d["itineraries"] = itins

            # If the base deal has no IATAs/dates, inherit them from the first itinerary
            first_itin = itins[0] if isinstance(itins[0], dict) else None
            if first_itin:
                if not d.get("origin_iata") and first_itin.get("origin_iata"):
                    d["origin_iata"] = first_itin.get("origin_iata")
                if not d.get("destination_iata") and first_itin.get("destination_iata"):
                    d["destination_iata"] = first_itin.get("destination_iata")
                if not d.get("return_date") and first_itin.get("return_date"):
                    d["return_date"] = first_itin.get("return_date")
                if not d.get("departure_date") and first_itin.get("departure_date"):
                    d["departure_date"] = first_itin.get("departure_date")

        # If this article has no concrete price anywhere, treat it as an
        # unpriced overview and do not persist it ("price N/A" isn't a deal).
        has_any_price = False
        if d.get("price") is not None:
            has_any_price = True
        if not has_any_price and article.get("price") is not None:
            has_any_price = True
        if not has_any_price and isinstance(itins, list):
            for it in itins:
                if isinstance(it, dict) and it.get("price") is not None:
                    has_any_price = True
                    break

        if persist and not has_any_price:
            _mark_source_article_done(d.get("link") or "", source="travel-dealz", note="missing_price")
            continue

        # Fields common to the article: copied to the base deal and then
        # inherited in each itinerary row in expanded_rows.
        for key in ("miles", "travel_dates_text", "expires_in", "airline", "aircraft", "cabin_baggage", "stops"):
            if article.get(key) is not None:
                d[key] = article.get(key)

        filtered_raw_deals.append(d)

    raw_deals = filtered_raw_deals

    # SecretFlying-specific enrichment: obtain booking_url and, if missing,
    # fill in price/origin/destination and IATA/dates from the individual post.
    # Additionally, if the post exposes multiple booking links (Skyscanner, etc.),
    # we build a list of itineraries to generate multiple flights
    # per post, same as in Travel-Dealz.
    for d in raw_deals:
        link = (d.get("link") or "").lower()
        if not link or "secretflying.com" not in link or "/posts/" not in link:
            continue
        try:
            article = parse_secretflying_post(d["link"])
        except Exception:
            continue

        if not isinstance(article, dict):
            continue

        # Direct booking_url to the offer (Skyscanner, airline, etc.)
        if article.get("booking_url"):
            d["booking_url"] = article["booking_url"]

        # If we couldn't extract price/currency from the listing, use the one from the post
        if d.get("price") is None and article.get("price") is not None:
            d["price"] = article.get("price")
            if article.get("currency"):
                d["currency"] = article.get("currency")

        # Fill in origin/destination and image if missing
        if not d.get("origin") and article.get("origin"):
            d["origin"] = article.get("origin")
        if not d.get("destination") and article.get("destination"):
            d["destination"] = article.get("destination")
        if not d.get("image") and not d.get("image_url") and article.get("image"):
            d["image_url"] = article.get("image")

        # Extract structured fields from the booking_url (via post parser)
        if article.get("origin_iata"):
            d["origin_iata"] = article.get("origin_iata")
        if article.get("destination_iata"):
            d["destination_iata"] = article.get("destination_iata")
        if article.get("departure_date"):
            d["departure_date"] = article.get("departure_date")
        if article.get("return_date"):
            d["return_date"] = article.get("return_date")
        if article.get("cabin_class"):
            d["cabin_class"] = article.get("cabin_class")
        if article.get("roundtrip") is not None:
            d["roundtrip"] = article.get("roundtrip")

        # List of specific itineraries (one per external booking link)
        itins = article.get("itineraries")
        if isinstance(itins, list) and itins:
            d["itineraries"] = itins

        # Grouped routes (if the post has a "Routes:" block with prices
        # per origin/destination).
        routes = article.get("routes")
        if isinstance(routes, list) and routes:
            d["routes"] = routes

        # Main airline of the deal (if we were able to infer it from the article)
        if article.get("airline") and not d.get("airline"):
            d["airline"] = article.get("airline")

        # Additional fallback: if we still lack structured fields but already
        # have a booking_url (e.g. extracted from the listing),
        # try to parse the URL directly without depending on the post's
        # HTML (which is sometimes protected by anti-bots).
        if d.get("booking_url"):
            bf = _extract_secretflying_from_booking_url(str(d["booking_url"]))
            if bf:
                if not d.get("origin_iata") and bf.get("origin_iata"):
                    d["origin_iata"] = bf["origin_iata"]
                if not d.get("destination_iata") and bf.get("destination_iata"):
                    d["destination_iata"] = bf["destination_iata"]
                if not d.get("departure_date") and bf.get("departure_date"):
                    d["departure_date"] = bf["departure_date"]
                if not d.get("return_date") and bf.get("return_date"):
                    d["return_date"] = bf["return_date"]
                if not d.get("cabin_class") and bf.get("cabin_class"):
                    d["cabin_class"] = bf["cabin_class"]
                if d.get("roundtrip") is None and bf.get("roundtrip") is not None:
                    d["roundtrip"] = bf["roundtrip"]

    # All fields are deterministic — no LLM enrichment step.
    enriched_deals = raw_deals

    # Coalesce fields that can be derived locally before filtering/scoring
    enriched_deals = [_normalize_deal_fields(d) for d in enriched_deals]

    # Fetch destination images from Unsplash for deals that don't have one.
    try:
        from services.unsplash_service import fetch_destination_image  # type: ignore
        for d in enriched_deals:
            if not d.get("image_url"):
                city = d.get("destination") or d.get("destination_iata") or ""
                if city:
                    img = fetch_destination_image(str(city))
                    if img:
                        d["image_url"] = img
    except Exception:
        pass  # Unsplash is optional; never break the pipeline

    logger.info(
        "[deals_pipeline] normalized enriched_deals=%s",
        len(enriched_deals),
    )

    # Exclude one-way flights — deals without a return date are skipped.
    enriched_deals = [d for d in enriched_deals if d.get("return_date")]

    # Optional: keep only deals whose origin_iata matches a configured filter
    # (defaults to Swiss airports: ZRH, GVA, BSL, BRN, LUG, SIR).
    origin_filter = _get_origin_iata_filter()
    if origin_filter:
        filtered: List[Dict[str, Any]] = []
        for d in enriched_deals:
            origin_iata = str(d.get("origin_iata") or "").strip().upper()
            if origin_iata and origin_iata in origin_filter:
                filtered.append(d)
        enriched_deals = filtered

    logger.info(
        "[deals_pipeline] origin_filter filter=%s after_filter=%s",
        sorted(origin_filter) if origin_filter else "none",
        len(enriched_deals),
    )

    benchmarks = _load_duffel_benchmarks()
    scored_deals = score_deals(enriched_deals, benchmarks=benchmarks)

    logger.info("[deals_pipeline] scored scored_deals=%s", len(scored_deals))

    # Deduplicate deals at the article level using booking_url (and, if missing,
    # the article link itself) to avoid repeating the same deal when
    # it comes from multiple sources/listings.
    unique_scored: List[Dict[str, Any]] = []
    seen_article_urls: set[str] = set()
    for d in scored_deals:
        key = d.get("booking_url") or d.get("link")
        if not isinstance(key, str) or not key:
            unique_scored.append(d)
            continue
        if key in seen_article_urls:
            continue
        seen_article_urls.add(key)
        unique_scored.append(d)

    scored_deals = unique_scored

    deterministic_enabled = os.getenv("DEALS_DETERMINISTIC_ENRICH", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    # Ensure that deals returned by the pipeline already carry a reasonable
    # miles value, so that HTML/newsletter don't depend solely on the
    # persistence layer.
    if deterministic_enabled:
        for d in scored_deals:
            if d.get("miles") or d.get("miles_estimate"):
                continue
            origin_iata = d.get("origin_iata")
            dest_iata = d.get("destination_iata")
            if not origin_iata or not dest_iata:
                continue
            try:
                gc_miles = great_circle_miles(origin_iata, dest_iata)
            except Exception:
                gc_miles = None
            if gc_miles is None:
                continue

            # Prefer a single *valid* miles program when we can infer one.
            airline_name = d.get("airline") or d.get("airline_name")
            cabin_class = d.get("cabin_class")
            is_roundtrip = bool(d.get("roundtrip") is True or d.get("oneway") is False or d.get("one_way") is False)
            best_prog, best_miles = choose_best_program_for_deal(
                int(gc_miles),
                str(airline_name) if airline_name else None,
                cabin_class=str(cabin_class) if cabin_class not in (None, "") else None,
                roundtrip=is_roundtrip,
            )
            if best_prog and isinstance(best_miles, int) and best_miles > 0:
                d["miles"] = f"{best_prog} · " + f"{best_miles:,}".replace(",", "'")
                continue

            # Fallback: raw approximation without program.
            approx = approximate_program_miles(gc_miles)
            if approx is not None:
                d["miles"] = approx

    html_snippet = render_html_snippet(scored_deals, max_items=max_items_html or limit)

    # Tier-split HTML: Economy deals → deals_free.html, Business/PE/First → deals_premium.html
    try:
        from scoring.html_output import build_deals_html  # type: ignore
        tier_limit = max_items_html or limit
        free_deals = [d for d in scored_deals if _assign_tier(d) == "free"]
        premium_deals = [d for d in scored_deals if _assign_tier(d) == "premium"]
        html_free = build_deals_html(free_deals, max_items=tier_limit)
        html_premium = build_deals_html(premium_deals, max_items=tier_limit)

        snippets_dir = os.path.join(os.path.dirname(__file__), "..", "snippets")
        os.makedirs(snippets_dir, exist_ok=True)
        with open(os.path.join(snippets_dir, "deals_free.html"), "w", encoding="utf-8") as fh:
            fh.write(html_free)
        with open(os.path.join(snippets_dir, "deals_premium.html"), "w", encoding="utf-8") as fh:
            fh.write(html_premium)
        logger.info(
            "[deals_pipeline] tier HTML written: free=%s premium=%s",
            len(free_deals), len(premium_deals),
        )
    except Exception as _tier_err:
        logger.warning("[deals_pipeline] tier HTML generation failed: %r", _tier_err)

    result: Dict[str, Any] = {
        "count": len(scored_deals),
        "sources_enabled": list(sources_enabled),
        "deals": scored_deals,
        "html_snippet": html_snippet,
        "free_count": len([d for d in scored_deals if _assign_tier(d) == "free"]),
        "premium_count": len([d for d in scored_deals if _assign_tier(d) == "premium"]),
    }

    if persist and scored_deals:
        # Expand each enriched+scored article into one row per itinerary / flight option
        expanded_rows: List[Dict[str, Any]] = []
        for d in scored_deals:
            itins = d.get("itineraries")
            if isinstance(itins, list) and itins:
                for itin in itins:
                    if not isinstance(itin, dict):
                        continue
                    row = dict(d)
                    # Override / specialise fields per flight
                    if itin.get("origin"):
                        row["origin"] = itin.get("origin")
                    if itin.get("destination"):
                        row["destination"] = itin.get("destination")
                    if itin.get("origin_iata"):
                        row["origin_iata"] = itin.get("origin_iata")
                    if itin.get("destination_iata"):
                        row["destination_iata"] = itin.get("destination_iata")
                    if itin.get("price") is not None:
                        row["price"] = itin.get("price")
                    if itin.get("currency"):
                        row["currency"] = itin.get("currency")
                    if itin.get("booking_url"):
                        row["booking_url"] = itin.get("booking_url")
                    if itin.get("roundtrip") is not None:
                        row["roundtrip"] = itin.get("roundtrip")
                    if "oneway" in itin:
                        row["oneway"] = itin.get("oneway")
                    if itin.get("departure_date"):
                        row["departure_date"] = itin.get("departure_date")
                    if itin.get("return_date"):
                        row["return_date"] = itin.get("return_date")
                    if itin.get("travel_dates_text"):
                        row["travel_dates_text"] = itin.get("travel_dates_text")
                    if itin.get("airline"):
                        row["airline"] = itin.get("airline")
                    if itin.get("airline_code"):
                        row["airline_code"] = itin.get("airline_code")
                    if itin.get("aircraft"):
                        row["aircraft"] = itin.get("aircraft")
                    if itin.get("cabin_class"):
                        row["cabin_class"] = itin.get("cabin_class")
                    if itin.get("miles"):
                        row["miles"] = itin.get("miles")
                    if itin.get("expires_in"):
                        row["expires_in"] = itin.get("expires_in")
                    if itin.get("flight_duration_minutes") is not None:
                        row["flight_duration_minutes"] = itin.get("flight_duration_minutes")
                    if itin.get("flight_duration_display"):
                        row["flight_duration_display"] = itin.get("flight_duration_display")

                    # Fallback miles estimation if still missing but IATAs available
                    if deterministic_enabled and (not row.get("miles")) and row.get("origin_iata") and row.get("destination_iata"):
                        try:
                            gc_m = great_circle_miles(row["origin_iata"], row["destination_iata"])
                            approx_m = approximate_program_miles(gc_m) if gc_m is not None else None
                            if approx_m is not None:
                                row["miles"] = approx_m
                        except Exception:
                            pass
                    expanded_rows.append(row)
            else:
                expanded_rows.append(d)

        # Save clean version for direct consumption in the unified table
        # public.deals. We no longer persist copies in per-source tables
        # (deals_traveldealz/deals_secretflying).
        deals_payload: List[Dict[str, Any]] = []

        for d in expanded_rows:
            # Build short flight name: "Zürich (ZRH) → Los Angeles (LAX)"
            origin_iata = d.get("origin_iata")
            dest_iata = d.get("destination_iata")
            origin_city = _resolve_city_name(d.get("origin") or d.get("origin_city"), origin_iata)
            dest_city = _resolve_city_name(d.get("destination") or d.get("destination_city"), dest_iata)

            def _fmt_place(city: Any, code: Any) -> Optional[str]:
                code_str = str(code).strip().upper() if isinstance(code, str) else None
                city_str = _resolve_city_name(city, code_str)

                # Only add parentheses if we have a clear IATA code (3 letters)
                # that is different from the city name.
                if city_str and code_str and len(code_str) == 3 and code_str.isalpha() and code_str != city_str.upper():
                    return f"{city_str} ({code_str})"
                if city_str:
                    return city_str
                if code_str:
                    return code_str
                return None

            origin_label = _fmt_place(origin_city, origin_iata)
            dest_label = _fmt_place(dest_city, dest_iata)

            flight_name = None
            if origin_label and dest_label:
                # Same format as in Travel-Dealz: "City (IATA) → City (IATA)"
                flight_name = f"{origin_label} → {dest_label}"
            elif origin_label:
                flight_name = origin_label
            elif dest_label:
                flight_name = dest_label

            def _fmt_miles_text(v: Any) -> Any:
                if v is None:
                    return None
                if isinstance(v, (int, float)):
                    try:
                        n = int(v)
                        return f"{n:,.0f}".replace(",", "'")
                    except Exception:
                        return str(v)
                if isinstance(v, str):
                    s = v.strip()
                    return s or None
                return str(v)

            # Ensure we always have a reasonable miles value.
            # Always stored as text (a single "miles" column).
            miles_raw = d.get("miles") or d.get("miles_estimate")
            if deterministic_enabled and (not miles_raw) and origin_iata and dest_iata:
                try:
                    gc_miles = great_circle_miles(origin_iata, dest_iata)
                except Exception:
                    gc_miles = None
                if gc_miles is not None:
                    airline_name = d.get("airline") or d.get("airline_name")
                    cabin_class = d.get("cabin_class")
                    is_roundtrip = bool(d.get("roundtrip") is True or d.get("oneway") is False or d.get("one_way") is False)
                    best_prog, best_miles = choose_best_program_for_deal(
                        int(gc_miles),
                        str(airline_name) if airline_name else None,
                        cabin_class=str(cabin_class) if cabin_class not in (None, "") else None,
                        roundtrip=is_roundtrip,
                    )
                    if best_prog and isinstance(best_miles, int) and best_miles > 0:
                        miles_raw = f"{best_prog} · " + f"{best_miles:,}".replace(",", "'")
                    else:
                        approx = approximate_program_miles(gc_miles)
                        if approx is not None:
                            miles_raw = approx

            miles_value = _normalize_miles_display(_fmt_miles_text(miles_raw))

            row_payload: Dict[str, Any] = {
                "title": d.get("title"),
                "price": d.get("price"),
                # link = article URL; booking_url = concrete flight booking URL (may be null)
                "link": d.get("link") or d.get("booking_url"),
                "booking_url": (
                    None  # TravelDealz: go2.travel-dealz.de links are not booking URLs
                    if "travel-dealz" in str(d.get("source") or "").lower()
                    else (
                        d.get("booking_url")
                        if d.get("booking_url") and d.get("booking_url") != d.get("link")
                        else None
                    )
                ),
                "currency": d.get("currency"),
                "image": d.get("image"),  # always Unsplash (set by _normalize_deal_fields)
                "aircraft": d.get("aircraft"),
                "airline": d.get("airline") or d.get("airline_name"),
                "origin": origin_city,
                "destination": dest_city,
                "miles": miles_value,
                "expires_in": d.get("expires_in"),
                "date_out": d.get("departure_date"),
                "date_in": d.get("return_date"),
                "cabin_class": _normalize_cabin_class(d.get("cabin_class")),
                "origin_iata": d.get("origin_iata"),
                "destination_iata": d.get("destination_iata"),
                "source": d.get("source"),
                "scoring": d.get("score"),
                "flight_duration_minutes": d.get("flight_duration_minutes"),
                "flight_duration_display": d.get("flight_duration_display"),
                "baggage_included": d.get("baggage_included"),
                "baggage_pieces_included": d.get("baggage_pieces_included"),
                "baggage_allowance_kg": d.get("baggage_allowance_kg"),
                "stops": d.get("stops"),
                "skyscanner_url": d.get("skyscanner_url"),
                "travel_period_display": d.get("travel_period_display"),
                "tier": _assign_tier(d),
            }

            deals_payload.append(row_payload)

        # For SecretFlying we want one record per aggregated route (when the
        # post has a "Routes:" block), instead of just one per post.
        for base in scored_deals:
            source_label = str(base.get("source") or "").lower()
            if "secretflying" not in source_label:
                continue

            base_origin_city = base.get("origin") or base.get("origin_city")
            base_dest_city = base.get("destination") or base.get("destination_city")
            base_origin_iata = base.get("origin_iata")
            base_dest_iata = base.get("destination_iata")

            def _fmt_place_sf(city: Any, code: Any) -> Optional[str]:
                code_str = str(code).strip().upper() if isinstance(code, str) else None
                city_str = _resolve_city_name(city, code_str)
                if city_str and code_str and len(code_str) == 3 and code_str.isalpha() and code_str != city_str.upper():
                    return f"{city_str} ({code_str})"
                if city_str:
                    return city_str
                if code_str:
                    return code_str
                return None

            base_routes = base.get("routes")
            if isinstance(base_routes, list) and base_routes:
                # One record per route declared in the "Routes:" block.
                for r in base_routes:
                    if not isinstance(r, dict):
                        continue
                    origin_city = r.get("origin") or base_origin_city
                    dest_city = r.get("destination") or base_dest_city

                    # Allow route-specific IATAs if the parser provides them;
                    # otherwise use those from the base deal.
                    route_origin_iata = r.get("origin_iata") or base_origin_iata
                    route_dest_iata = r.get("destination_iata") or base_dest_iata

                    origin_label = _fmt_place_sf(origin_city, route_origin_iata)
                    dest_label = _fmt_place_sf(dest_city, route_dest_iata)

                    flight_name = None
                    if origin_label and dest_label:
                        flight_name = f"{origin_label} → {dest_label}"
                    elif origin_label:
                        flight_name = origin_label
                    elif dest_label:
                        flight_name = dest_label

                    price_route = r.get("price_min")
                    if price_route is None:
                        price_route = base.get("price")

                    # Route-specific booking_url if it exists and differs
                    # from the main link. If there is no clear one, we leave NULL to
                    # avoid conflicts with the unique constraint in Supabase.
                    route_booking_url = r.get("booking_url") or base.get("booking_url")
                    if not route_booking_url or route_booking_url == base.get("link"):
                        route_booking_url = None

                    sf_row: Dict[str, Any] = {
                        "title": base.get("title") or flight_name,
                        "price": price_route,
                        "link": base.get("link") or base.get("booking_url"),
                        "booking_url": route_booking_url,
                        "currency": base.get("currency"),
                        "image": base.get("image"),
                        "aircraft": base.get("aircraft"),
                        "airline": base.get("airline") or base.get("airline_name"),
                        "origin": origin_city,
                        "destination": dest_city,
                        "miles": _normalize_miles_display(base.get("miles") or base.get("miles_estimate")),
                        "expires_in": base.get("expires_in"),
                        "date_out": base.get("departure_date"),
                        "date_in": base.get("return_date"),
                        "cabin_class": _normalize_cabin_class(base.get("cabin_class")),
                        "origin_iata": route_origin_iata,
                        "destination_iata": route_dest_iata,
                        "source": base.get("source"),
                        "scoring": base.get("score"),
                        "flight_duration_minutes": base.get("flight_duration_minutes"),
                        "flight_duration_display": base.get("flight_duration_display"),
                        "baggage_included": base.get("baggage_included"),
                        "baggage_pieces_included": base.get("baggage_pieces_included"),
                        "baggage_allowance_kg": base.get("baggage_allowance_kg"),
                        "stops": base.get("stops"),
                        "skyscanner_url": base.get("skyscanner_url"),
                        "travel_period_display": base.get("travel_period_display"),
                        "tier": _assign_tier(base),
                            }

                    if not sf_row.get("miles") and sf_row.get("origin_iata") and sf_row.get("destination_iata"):
                        try:
                            gc_m = great_circle_miles(sf_row["origin_iata"], sf_row["destination_iata"])
                            approx_m = approximate_program_miles(gc_m) if gc_m is not None else None
                            if approx_m is not None:
                                sf_row["miles"] = f"{int(approx_m):,.0f}".replace(",", "'")
                        except Exception:
                            pass

                    # SecretFlying is also reflected in deals (aggregated table)
                    deals_payload.append(sf_row)
            else:
                # Fallback: a single record per post (previous behaviour).
                origin_label = _fmt_place_sf(base_origin_city, base_origin_iata)
                dest_label = _fmt_place_sf(base_dest_city, base_dest_iata)

                flight_name = None
                if origin_label and dest_label:
                    flight_name = f"{origin_label} → {dest_label}"
                elif origin_label:
                    flight_name = origin_label
                elif dest_label:
                    flight_name = dest_label

                sf_row: Dict[str, Any] = {
                    "title": base.get("title") or flight_name,
                    "price": base.get("price"),
                    "link": base.get("link") or base.get("booking_url"),
                    "booking_url": (
                        base.get("booking_url")
                        if base.get("booking_url") and base.get("booking_url") != base.get("link")
                        else None
                    ),
                    "currency": base.get("currency"),
                    "image": base.get("image"),
                    "aircraft": base.get("aircraft"),
                    "airline": base.get("airline") or base.get("airline_name"),
                    "origin": base_origin_city,
                    "destination": base_dest_city,
                    "miles": _normalize_miles_display(base.get("miles") or base.get("miles_estimate")),
                    "expires_in": base.get("expires_in"),
                    "date_out": base.get("departure_date"),
                    "date_in": base.get("return_date"),
                    "cabin_class": _normalize_cabin_class(base.get("cabin_class")),
                    "origin_iata": base_origin_iata,
                    "destination_iata": base_dest_iata,
                    "source": base.get("source"),
                    "scoring": base.get("score"),
                    "flight_duration_minutes": base.get("flight_duration_minutes"),
                    "flight_duration_display": base.get("flight_duration_display"),
                    "baggage_included": base.get("baggage_included"),
                    "baggage_pieces_included": base.get("baggage_pieces_included"),
                    "baggage_allowance_kg": base.get("baggage_allowance_kg"),
                    "stops": base.get("stops"),
                    "skyscanner_url": base.get("skyscanner_url"),
                    "travel_period_display": base.get("travel_period_display"),
                    "tier": _assign_tier(base),
                    }

                if not sf_row.get("miles") and sf_row.get("origin_iata") and sf_row.get("destination_iata"):
                    try:
                        gc_m = great_circle_miles(sf_row["origin_iata"], sf_row["destination_iata"])
                        approx_m = approximate_program_miles(gc_m) if gc_m is not None else None
                        if approx_m is not None:
                            sf_row["miles"] = f"{int(approx_m):,.0f}".replace(",", "'")
                    except Exception:
                        pass

                deals_payload.append(sf_row)

        # Avoid Postgres/Supabase error "ON CONFLICT DO UPDATE command cannot
        # affect row a second time" by deduplicating by booking_url within the
        # same upsert command.
        unique_deals_payload = []
        seen_booking_urls: set[str] = set()
        for row in deals_payload:
            b_url = row.get("booking_url")
            if isinstance(b_url, str) and b_url:
                if b_url in seen_booking_urls:
                    continue
                seen_booking_urls.add(b_url)
            unique_deals_payload.append(row)

        # Coerce numeric fields before persistence to satisfy DB types
        payload_sanitized = [_coerce_numeric_fields(r) for r in unique_deals_payload]

        deals_save_result = save_deals("deals", payload_sanitized)
        persisted_ok = deals_save_result.get("status") == "ok"
        result["persisted"] = persisted_ok
        result["persisted_count"] = len(unique_deals_payload) if persisted_ok else 0
        if not persisted_ok:
            result["persist_info"] = deals_save_result

        # Sync source_articles only if persistence succeeded
        if persisted_ok and unique_deals_payload:
            _upsert_source_articles_done(scored_deals)

        # 3) We no longer store copies in per-source tables

    return result
