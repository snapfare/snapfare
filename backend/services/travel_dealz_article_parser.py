import csv
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from scrapers.travel_dealz import _extract_route_from_title


# Cache liviano de nombres de aeropuerto (alemán) indexado por IATA
_AIRPORT_NAMES_BY_IATA: Dict[str, str] | None = None


def _load_airport_names_map() -> Dict[str, str]:
    global _AIRPORT_NAMES_BY_IATA
    if _AIRPORT_NAMES_BY_IATA is not None:
        return _AIRPORT_NAMES_BY_IATA

    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "scoring", "data", "airport_names_german.csv"
    )
    def _clean_mapped_name(s: str) -> str:
        if not s:
            return s
        s2 = s.strip()
        # Guard against accidental pollution like "Frankfurt für 198€" in the CSV.
        s2 = re.sub(
            r"\s+(?:f\u00fcr|ab)\s*\d[\d\s.,']*(?:\s*(?:\u20ac|eur|usd|chf|gbp))?.*$",
            "",
            s2,
            flags=re.IGNORECASE,
        ).strip()
        return s2

    names: Dict[str, str] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code") or "").strip().upper()
                name = _clean_mapped_name(str(row.get("deutscher_Name") or "").strip())
                if code and name:
                    names[code] = name
    except Exception:
        names = {}

    _AIRPORT_NAMES_BY_IATA = names
    return names


def _resolve_city_from_iata(city_val: Any, iata_val: Any) -> Optional[str]:
    """Return a city/airport name, preferring provided city, else CSV map."""

    def _strip_trailing_price_fragments(s: str) -> str:
        if not s:
            return s
        # Common Travel-Dealz list/table formatting:
        #   "Frankfurt für 198€"
        #   "Basel BSL für 324 M + 225€"
        # We want to keep the place name/station and drop the trailing price part.
        s2 = s.strip()
        s2 = re.sub(
            r"\s+(?:f\u00fcr|ab)\s*\d[\d\s.,']*(?:\s*(?:\u20ac|eur|usd|chf|gbp))?.*$",
            "",
            s2,
            flags=re.IGNORECASE,
        ).strip()
        return s2

    city_str = (
        _strip_trailing_price_fragments(str(city_val)).strip()
        if isinstance(city_val, str) and str(city_val).strip()
        else None
    )
    iata_str = str(iata_val).strip().upper() if isinstance(iata_val, str) and len(iata_val.strip()) == 3 else None

    if city_str and iata_str and city_str.upper() == iata_str:
        city_str = None

    # If the page-provided "city" looks polluted (not a place name), drop it.
    # This prevents persisting tokens like "Alternativen" / "Klappt übrigens ..."
    # as origin/destination.
    if city_str:
        if any(ch.isdigit() for ch in city_str):
            city_str = None
        else:
            bad_re = re.compile(
                r"\b(?:alternativen|klappt|\boneway\b|\broundtrip\b|inkl\.?|gepack|gepäck|flugpreis|voucher|gift\s*card|home\s*»|dealz\s*»|flights\s*»|show\s+deal)\b",
                re.IGNORECASE,
            )
            if bad_re.search(city_str) or len(city_str) > 45:
                city_str = None

    # Si ya tenemos un nombre de la página (limpio), no lo sobreescribimos con el CSV
    if city_str:
        return city_str

    if iata_str:
        mapped = _load_airport_names_map().get(iata_str)
        if mapped:
            return mapped
        # Sin mapeo, devolvemos el propio código IATA para no dejarlo vacío
        return iata_str

    return city_str or iata_str

def _fetch_html(url: str, timeout: int = 10) -> Optional[str]:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            body_snip = (resp.text or "")[:500]
            print(f"[travel_dealz_parser] fetch_failed status={resp.status_code} url={url} body_snip={body_snip!r}")
            return None
        return resp.text
    except Exception:
        return None


def _extract_section_by_heading(soup: BeautifulSoup, heading_text: str) -> Optional[BeautifulSoup]:
    """Find a section by heading text (h2/h3/strong) and return the container.

    We look for headings that contain the given text (case-insensitive) and
    return their parent or the next sibling block, which usually wraps the
    relevant paragraphs and lists.
    """
    heading = soup.find(
        ["h2", "h3", "h4", "strong"],
        string=lambda s: isinstance(s, str) and heading_text.lower() in s.lower(),
    )
    if not heading:
        return None

    # Prefer a direct parent that looks like a section/div
    parent = heading.parent
    if parent and parent.name in {"section", "div", "article"}:
        return parent

    # Fallback: take the next sibling block
    sib = heading.find_next_sibling(["div", "section", "p", "ul", "ol"])
    return sib


def _extract_section_by_heading_any(
    soup: BeautifulSoup,
    heading_texts: List[str],
) -> Optional[BeautifulSoup]:
    """Try multiple heading aliases and return the first matching section."""

    if not soup or not heading_texts:
        return None

    for ht in heading_texts:
        if not ht:
            continue
        sec = _extract_section_by_heading(soup, ht)
        if sec is not None:
            return sec
    return None


_price_re = re.compile(r"(?P<price>\d+[\d.,']*)")


def _normalize_city_name(name: str) -> str:
    """Normalize city names so we can compare origin/destination reliably.

    - lowercase
    - trim
    - replace '-' by space
    - remove basic punctuation
    - collapse multiple spaces
    """
    s = (name or "").strip().lower()
    s = s.replace("-", " ")
    # remove punctuation and non-letter/space chars to make
    # "Washington, DC" vs "Washington D.C." comparable
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _detect_currency(text: str) -> str:
    t = text.lower()
    if "€" in t or " eur" in t:
        return "EUR"
    if "$" in t or " usd" in t:
        return "USD"
    if " chf" in t or " sfr" in t:
        return "CHF"
    if " gbp" in t or "£" in t:
        return "GBP"
    return "EUR"


def _parse_price_from_text(text: str) -> tuple[Optional[float], str]:
    """Extract a numeric price only when there is an explicit currency.

    Nota importante: en títulos tipo "2-in-1: ... ab 577€ ..." el primer número
    ("2") NO es el precio. Por eso priorizamos números adyacentes a símbolos
    o códigos de moneda (577€), antes de caer a heurísticas más generales.

    Esto también evita confundir días de fecha ("2 – 13 March") con precios
    cuando el texto no contiene símbolo de moneda ni código (EUR, USD, ...).
    """
    if not text:
        return None, "EUR"

    # ¿Hay alguna pista de divisa en el texto?
    has_currency = bool(re.search(r"(€|\$|£|\b(?:eur|usd|chf|gbp)\b)", text, re.IGNORECASE))
    if not has_currency:
        return None, "EUR"

    # Prefer numeric tokens that are directly attached to the currency.
    currency_patterns: list[tuple[str, str]] = [
        (r"€\s*(\d[\d.,']*)", "EUR"),
        (r"(\d[\d.,']*)\s*€", "EUR"),
        (r"\$\s*(\d[\d.,']*)", "USD"),
        (r"(\d[\d.,']*)\s*USD\b", "USD"),
        (r"(\d[\d.,']*)\s*EUR\b", "EUR"),
        (r"(\d[\d.,']*)\s*CHF\b", "CHF"),
        (r"(\d[\d.,']*)\s*GBP\b", "GBP"),
        (r"(\d[\d.,']*)\s*£", "GBP"),
    ]

    for pat, cur in currency_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if not m:
            continue
        raw = m.group(1)
        digits = re.sub(r"[^0-9]", "", raw)
        if not digits:
            continue
        try:
            return float(digits), cur
        except ValueError:
            continue

    # Fallback: scan for numbers, but pick a reasonable candidate (max) when currency is present.
    values: list[float] = []
    for m in _price_re.finditer(text):
        raw = m.group("price")
        digits = re.sub(r"[^0-9]", "", raw)
        if not digits:
            continue
        try:
            values.append(float(digits))
        except ValueError:
            continue

    if not values:
        return None, "EUR"

    currency = _detect_currency(text)
    return max(values), currency


def _extract_itineraries_from_booking_links(root: BeautifulSoup, base_url: str, fallback_title: str = "") -> List[Dict[str, Any]]:
    """Parse all booking links (go2.travel-dealz.eu/?from=...&to=...) into itineraries.

    No dependemos de la sección "Search & Book" salvo para fechas; aquí
    buscamos en todo el HTML enlaces con el patrón de booking y usamos su
    texto para inferir la ciudad de origen.
    """
    itineraries: List[Dict[str, Any]] = []
    if not root:
        return itineraries

    seen_urls: set[str] = set()
    # Deduplicamos por combinación lógica de parámetros (from/to/fechas, etc.)
    # para evitar contar dos veces el mismo vuelo con URLs casi idénticas.
    seen_keys: set[tuple] = set()

    # Pequeño helper para extraer origen/destino de una línea de texto
    # como "Honolulu → Hamburg [from €203]" o "Barcelona at €969".
    def _parse_route_line(line: str) -> tuple[Optional[str], Optional[str]]:
        origin_name: Optional[str] = None
        dest_name: Optional[str] = None

        if not line:
            return None, None

        text = line.strip()

        # Patrón principal con flecha
        if "→" in text:
            left, right = text.split("→", 1)
            origin_name = left.strip() or None

            # Destino: parte antes de corchetes o " from ", si existen
            right_clean = right
            if "[" in right_clean:
                right_clean = right_clean.split("[", 1)[0]
            idx = right_clean.lower().find(" from ")
            if idx != -1:
                right_clean = right_clean[:idx]
            dest_name = right_clean.strip() or None
            if origin_name:
                origin_name = re.sub(r"(?i)^from\s+", "", origin_name).strip() or None
            return origin_name, dest_name

        # Patrones de una sola ciudad tipo
        #   "Stockholm: €368"
        #   "Barcelona at €969 *"
        # y frases del estilo
        #   "You can also purchase one way tickets from Oslo at €641".
        # En la práctica, en casi todos los deals modernos de Travel-Dealz
        # esta ciudad es el ORIGEN (la lista enumera aeropuertos de salida).
        # La ciudad de destino real suele aparecer como "Destination" global
        # (p.ej. Bangkok) o en el propio título del artículo.
        city: Optional[str] = None
        base = text
        if ":" in base:
            base = base.split(":", 1)[0].strip()
        else:
            m_city = re.match(r"(.+?)\s+at\s+", base, re.IGNORECASE)
            if m_city:
                base = m_city.group(1).strip()

        if base:
            # 1) Si el texto contiene uno o varios "from X", nos quedamos
            #    con la última ciudad X (p.ej. "from Oslo" dentro de una
            #    frase larga).
            from_matches = list(
                re.finditer(r"\bfrom\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)", base)
            )
            if from_matches:
                city = from_matches[-1].group(1).strip(" ,.")
            else:
                # 2) Como fallback muy conservador, solo aceptamos el
                #    texto completo como ciudad si parece un nombre propio
                #    corto tipo "Oslo" o "Los Angeles" (una o varias
                #    palabras, todas con inicial mayúscula).
                if re.match(r"^[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*$", base.strip()):
                    candidate = base.strip()

                    # Evitar tratar textos de call-to-action como ciudades,
                    # p.ej. "Show Deal", "Show Flights", etc.
                    bad_tokens = {"deal", "deals", "flight", "flights", "fare", "fares", "search", "click", "here"}
                    lower_candidate = candidate.lower()
                    if not any(tok in lower_candidate for tok in bad_tokens):
                        city = candidate

        if city:
            origin_name = re.sub(r"(?i)^from\s+", "", city).strip() or None

        return origin_name, dest_name

    # 1) Listas de precios con booking URLs en <li>
    for li in root.find_all("li"):
        li_text = li.get_text(" ", strip=True)
        if not li_text:
            continue

        # Intentar extraer ORIGEN y DESTINO del propio texto de la línea.
        # Casos cubiertos:
        #   - "Stockholm: €368"  → city único (usado como destino genérico)
        #   - "Madrid at €545"   → city único (usado como destino genérico)
        #   - "Honolulu → Hamburg [from €203]" → origen y destino explícitos
        origin_name, dest_name = _parse_route_line(li_text)

        # booking URL y precio: del primer enlace del <li>
        a = li.find("a", href=True)
        booking_url = None
        price = None
        currency = "EUR"
        if a:
            href = a["href"].strip()
            if href.startswith("//"):
                href = "https:" + href
            if href.startswith("/"):
                href = urljoin(base_url, href)
            # Solo consideramos enlaces go2.travel-dealz.* como booking URLs válidas
            if "go2.travel-dealz." in href:
                booking_url = href

            price_text = a.get_text(" ", strip=True)
            price, currency = _parse_price_from_text(price_text)
        else:
            price, currency = _parse_price_from_text(li_text)

        if not (booking_url or price):
            # Probablemente no es una línea de precios real
            continue

        # Solo creamos itinerarios para booking URLs válidas con parámetros from/to
        if booking_url:
            try:
                parsed = urlparse(booking_url)
                qs = parse_qs(parsed.query)

                def _first(key: str) -> Optional[str]:
                    vals = qs.get(key)
                    return vals[0] if vals else None

                from_code = _first("from")
                to_code = _first("to")
                if not (from_code and to_code):
                    continue

                resolved_from_name = _resolve_city_from_iata(None, from_code)
                resolved_to_name = _resolve_city_from_iata(None, to_code)

                # Si el texto del botón menciona la ciudad de destino pero no hay flecha,
                # evitamos tratarla como origen cuando coincide claramente con el to_code.
                if origin_name and not dest_name and resolved_to_name:
                    if _normalize_city_name(origin_name) == _normalize_city_name(resolved_to_name):
                        # Siempre que no coincida también con el origen
                        if not resolved_from_name or _normalize_city_name(origin_name) != _normalize_city_name(resolved_from_name):
                            dest_name = origin_name
                            origin_name = None

                date_out = _first("date_out") or ""
                date_in = _first("date_in") or ""
                oneway_param = (_first("oneway") or "").lower()
                cabinclass = (_first("cabinclass") or "").upper()
                airlines = (_first("airlines") or "").upper()

                # Para deduplicar, normalizamos también la divisa.
                currency_param = (_first("currency") or (currency or "")).upper()

                key = (
                    from_code.upper(),
                    to_code.upper(),
                    date_out,
                    date_in,
                    oneway_param in {"true", "1"},
                    cabinclass,
                    airlines,
                    currency_param,
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                if booking_url in seen_urls:
                    continue
                seen_urls.add(booking_url)

                itin: Dict[str, Any] = {
                    # Si hemos podido extraer origen/destino por nombre, los
                    # usamos directamente; si no, al menos los códigos IATA
                    # quedarán rellenos y más tarde podremos mostrar
                    # "HNL → HAM", etc.
                    "origin": origin_name,
                    "destination": dest_name,
                    "price": price,
                    "currency": currency_param or currency,
                    "booking_url": booking_url,
                    "origin_iata": from_code.upper(),
                    "destination_iata": to_code.upper(),
                }

                if date_out:
                    itin["departure_date"] = date_out
                if date_in:
                    itin["return_date"] = date_in

                if oneway_param:
                    oneway_bool = oneway_param in {"true", "1"}
                    itin["oneway"] = oneway_bool
                    itin["roundtrip"] = not oneway_bool

                if cabinclass:
                    itin["cabin_class"] = cabinclass

                if airlines:
                    itin["airline_code"] = airlines

                direct = _first("direct")
                if direct is not None:
                    itin["direct"] = direct.lower() in {"true", "1"}

                itineraries.append(itin)
            except Exception:
                # Si el parsing del booking_url falla, simplemente ignoramos la línea
                continue

    # 1b) Botones de precios tipo wp-block-button (caso sin <li>)
    #    <a class="wp-block-button__link ..." href="https://go2.travel-dealz.eu/?from=BRU&to=CGK...">
    #       Brussels at €1,071
    #    </a>
    for a in root.find_all("a", class_=re.compile(r"wp-block-button__link", re.IGNORECASE), href=True):
        href = a["href"].strip()
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith("/"):
            href = urljoin(base_url, href)

        # Solo nos interesan enlaces go2.travel-dealz.*
        if "go2.travel-dealz." not in href:
            continue

        booking_url = href

        text = a.get_text(" ", strip=True)
        if not text:
            continue

        # Intentar usar el texto del propio botón, y si no es suficiente
        # (p.ej. sólo "Show Deal *"), mirar el texto del contenedor padre
        # para detectar patrones "Honolulu → Hamburg [from €203]".
        origin_name: Optional[str] = None
        dest_name: Optional[str] = None

        origin_name, dest_name = _parse_route_line(text)

        if not (origin_name or dest_name):
            parent = a.parent
            if parent is not None:
                context_text = parent.get_text(" ", strip=True)
                o2, d2 = _parse_route_line(context_text)
                if o2 or d2:
                    origin_name, dest_name = o2, d2

        # Compatibilidad hacia atrás: city único tratado como destino genérico
        city: Optional[str] = None
        if not (origin_name or dest_name):
            if ":" in text:
                city = text.split(":", 1)[0].strip()
            else:
                m_city = re.match(r"(.+?)\s+at\s+", text, re.IGNORECASE)
                if m_city:
                    city = m_city.group(1).strip()

        price, currency = _parse_price_from_text(text)

        try:
            parsed = urlparse(booking_url)
            qs = parse_qs(parsed.query)

            def _first_btn(key: str) -> Optional[str]:
                vals = qs.get(key)
                return vals[0] if vals else None

            from_code = _first_btn("from")
            to_code = _first_btn("to")
            if not (from_code and to_code):
                continue

            resolved_from_name = _resolve_city_from_iata(None, from_code)
            resolved_to_name = _resolve_city_from_iata(None, to_code)

            # Ajustar ciudad suelta al destino cuando coincide con to_code.
            if not (origin_name or dest_name) and city:
                norm_city = _normalize_city_name(city)
                if resolved_to_name and norm_city == _normalize_city_name(resolved_to_name):
                    dest_name = city
                elif resolved_from_name and norm_city == _normalize_city_name(resolved_from_name):
                    origin_name = city
                else:
                    origin_name = city

            if origin_name and not dest_name and resolved_to_name:
                if _normalize_city_name(origin_name) == _normalize_city_name(resolved_to_name):
                    if not resolved_from_name or _normalize_city_name(origin_name) != _normalize_city_name(resolved_from_name):
                        dest_name = origin_name
                        origin_name = None

            date_out = _first_btn("date_out") or ""
            date_in = _first_btn("date_in") or ""
            oneway_param = (_first_btn("oneway") or "").lower()
            cabinclass = (_first_btn("cabinclass") or "").upper()
            currency_param = (_first_btn("currency") or currency or "").upper()
            airlines = (_first_btn("airlines") or "").upper()

            key = (
                from_code.upper(),
                to_code.upper(),
                date_out,
                date_in,
                oneway_param in {"true", "1"},
                cabinclass,
                airlines,
                currency_param,
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)

            if booking_url in seen_urls:
                continue
            seen_urls.add(booking_url)

            itin = {
                "origin": origin_name or city or None,
                "destination": dest_name,
                "price": price,
                "currency": currency_param or currency,
                "booking_url": booking_url,
                "origin_iata": from_code.upper(),
                "destination_iata": to_code.upper(),
            }

            if date_out:
                itin["departure_date"] = date_out
            if date_in:
                itin["return_date"] = date_in

            if oneway_param:
                oneway_bool = oneway_param in {"true", "1"}
                itin["oneway"] = oneway_bool
                itin["roundtrip"] = not oneway_bool

            if cabinclass:
                itin["cabin_class"] = cabinclass

            if airlines:
                itin["airline_code"] = airlines

            direct = _first_btn("direct")
            if direct is not None:
                itin["direct"] = direct.lower() in {"true", "1"}

            itineraries.append(itin)
        except Exception:
            continue

    # 1c) Enlaces genéricos a go2.travel-dealz.eu en párrafos, figuras, etc.
    #    Esto cubre casos como:
    #    <p>... <a href="https://go2.travel-dealz.eu/?from=BRU&to=DPS&...">Brussels</a> ...</p>
    #    <figure><a href="https://go2.travel-dealz.eu/?from=BRU&to=CGK&..."><img ...></a></figure>
    for a in root.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        # Normalizar URL relativa / esquema-relative
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith("/"):
            href = urljoin(base_url, href)

        # Solo nos interesan enlaces go2.travel-dealz (cubre .eu, .com, etc.)
        if "go2.travel-dealz." not in href:
            continue

        booking_url = href

        text = a.get_text(" ", strip=True) or ""

        # Igual que en los botones: primero intentamos con el propio texto
        # del enlace y, si no basta, usamos el texto del padre como contexto.
        origin_name: Optional[str] = None
        dest_name: Optional[str] = None

        origin_name, dest_name = _parse_route_line(text)

        if not (origin_name or dest_name):
            parent = a.parent
            if parent is not None:
                context_text = parent.get_text(" ", strip=True)
                o2, d2 = _parse_route_line(context_text)
                if o2 or d2:
                    origin_name, dest_name = o2, d2

        city: Optional[str] = None
        if not (origin_name or dest_name):
            if ":" in text:
                city = text.split(":", 1)[0].strip()
            else:
                m_city = re.match(r"(.+?)\s+at\s+", text, re.IGNORECASE)
                if m_city:
                    city = m_city.group(1).strip()

        price, currency = _parse_price_from_text(text)

        # Many newer Travel-Dealz posts link the booking URL on an <img> inside
        # a <figure>, so the <a> text is empty (no price). In those cases, the
        # price (and often the origin city) is in the immediately preceding
        # heading, e.g. "Milan: €392 with Air China".
        if price is None:
            try:
                heading = a.find_previous(["h2", "h3", "h4"])
            except Exception:
                heading = None
            if heading is not None:
                heading_text = heading.get_text(" ", strip=True)
                if heading_text:
                    p2, c2 = _parse_price_from_text(heading_text)
                    if p2 is not None:
                        price, currency = p2, c2

                    # If we still don't have an origin label, infer it from heading.
                    # This works for patterns like "Milan: €392 ...".
                    o3, d3 = _parse_route_line(heading_text)
                    if o3 and not origin_name and not city:
                        origin_name = o3
                    if d3 and not dest_name:
                        dest_name = d3

        try:
            parsed = urlparse(booking_url)
            qs = parse_qs(parsed.query)

            def _first_generic(key: str) -> Optional[str]:
                vals = qs.get(key)
                return vals[0] if vals else None

            from_code = _first_generic("from")
            to_code = _first_generic("to")
            if not (from_code and to_code):
                continue

            resolved_from_name = _resolve_city_from_iata(None, from_code)
            resolved_to_name = _resolve_city_from_iata(None, to_code)

            # Ajustar ciudad suelta al destino cuando coincide con to_code.
            if not (origin_name or dest_name) and city:
                norm_city = _normalize_city_name(city)
                if resolved_to_name and norm_city == _normalize_city_name(resolved_to_name):
                    dest_name = city
                elif resolved_from_name and norm_city == _normalize_city_name(resolved_from_name):
                    origin_name = city
                else:
                    origin_name = city

            if origin_name and not dest_name and resolved_to_name:
                if _normalize_city_name(origin_name) == _normalize_city_name(resolved_to_name):
                    if not resolved_from_name or _normalize_city_name(origin_name) != _normalize_city_name(resolved_from_name):
                        dest_name = origin_name
                        origin_name = None

            date_out = _first_generic("date_out") or ""
            date_in = _first_generic("date_in") or ""
            oneway_param = (_first_generic("oneway") or "").lower()
            cabinclass = (_first_generic("cabinclass") or "").upper()
            currency_param = (_first_generic("currency") or currency or "").upper()
            airlines = (_first_generic("airlines") or "").upper()

            # Algunos artículos incluyen enlaces auxiliares a agencias online
            # (OTAs) dentro de párrafos explicativos, p.ej. "booking through
            # online travel agencies at the lowest price". Esos enlaces
            # suelen llevar direct=false y no tienen un precio explícito en
            # el texto del anchor. Si ya hemos detectado itinerarios
            # equivalentes (mismo from/to, cabina y aerolínea) directos,
            # ignoramos estos enlaces auxiliares para no crear "viajes"
            # adicionales irrelevantes.
            direct_param = (_first_generic("direct") or "").lower()
            if direct_param in {"false", "0"} and price is None:
                exists_equivalent = any(
                    isinstance(it, dict)
                    and it.get("origin_iata") == from_code.upper()
                    and it.get("destination_iata") == to_code.upper()
                    and (not cabinclass or it.get("cabin_class") == cabinclass)
                    and (not airlines or it.get("airline_code") == airlines)
                    for it in itineraries
                )
                if exists_equivalent:
                    continue

            key = (
                from_code.upper(),
                to_code.upper(),
                date_out,
                date_in,
                oneway_param in {"true", "1"},
                cabinclass,
                airlines,
                currency_param,
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)

            if booking_url in seen_urls:
                continue
            seen_urls.add(booking_url)

            itin: Dict[str, Any] = {
                "origin": origin_name or city or None,
                "destination": dest_name,
                "price": price,
                "currency": currency_param or currency,
                "booking_url": booking_url,
                "origin_iata": from_code.upper(),
                "destination_iata": to_code.upper(),
            }

            if date_out:
                itin["departure_date"] = date_out
            if date_in:
                itin["return_date"] = date_in

            if oneway_param:
                oneway_bool = oneway_param in {"true", "1"}
                itin["oneway"] = oneway_bool
                itin["roundtrip"] = not oneway_bool

            if cabinclass:
                itin["cabin_class"] = cabinclass

            if airlines:
                itin["airline_code"] = airlines

            if direct_param:
                itin["direct"] = direct_param in {"true", "1"}

            itineraries.append(itin)
        except Exception:
            continue

    # Sin fallbacks agresivos: si no hay booking URLs válidas con from/to,
    # devolvemos simplemente la lista vacía y dejamos el deal sin itinerarios.
    return itineraries


def _extract_miles_from_section(section: BeautifulSoup) -> Optional[str]:
    """Extract the *maximum* miles value and the program/provider that gives it.

    Travel-Dealz often lists multiple programs in "Miles & Points" like:
      - "21,016 Miles on TAP Miles&Go"
      - "17,470 Award Miles + 480 Points on Miles&More"

        We return a single text value suitable for the `miles` field:
            - "21'016 · TAP Miles&Go"
            - "17'470 · Miles&More"
        If we can't infer a provider, we return just the miles number.
    """

    if not section:
        return None

    # Some articles explicitly state that no miles are earned.
    # In that case, return a clear marker so downstream fallbacks don't invent miles.
    try:
        txt0 = section.get_text(" ", strip=True)
    except Exception:
        txt0 = ""
    if txt0:
        if re.search(r"\b(?:keine\s+meilen|no\s+miles|does\s+not\s+earn\s+miles)\b", txt0, re.IGNORECASE):
            return "Keine Meilen"

    def _parse_int(raw_num: str) -> Optional[int]:
        digits = re.sub(r"[^0-9]", "", raw_num or "")
        if not digits:
            return None
        try:
            return int(digits)
        except Exception:
            return None

    def _extract_miles_number(text: str) -> Optional[int]:
        if not text:
            return None

        # Prefer Award Miles if present; otherwise Miles (but not Tier/Status miles).
        m_award = re.search(r"(\d[\d.,']*)\s*Award Miles\b", text, re.IGNORECASE)
        if m_award:
            return _parse_int(m_award.group(1))

        # EN: "12345 Miles" | DE: "12.345 Meilen"
        # Also handle DE award wording: "736 Prämienmeilen"
        # Avoid status/tier miles where possible.
        m_miles = re.search(
            r"(\d[\d.,']*)\s*(?<!Tier\s)(?<!Status\s)(?<!Elite\s)(?:Miles|Meilen|Pr(?:\u00e4|ae)mienmeilen)\b",
            text,
            re.IGNORECASE,
        )
        if m_miles:
            return _parse_int(m_miles.group(1))
        return None

    def _extract_provider(text: str) -> Optional[str]:
        if not text:
            return None
        m = re.search(r"\bon\s+(.+)$", text, re.IGNORECASE)
        if not m:
            m = re.search(r"\bat\s+(.+)$", text, re.IGNORECASE)
        if not m:
            m = re.search(r"\bbei\s+(.+)$", text, re.IGNORECASE)
        if not m:
            return None
        provider = m.group(1).strip().strip(". ")
        return provider or None

    def _fmt(v: int) -> str:
        return f"{v:,.0f}".replace(",", "'")

    def _is_preferred_provider(provider: Optional[str]) -> bool:
        if not provider:
            return False
        p = provider.lower()
        # Downstream rules and UI prefer these programs.
        return ("miles&more" in p) or ("miles & more" in p) or ("flying blue" in p)

    # First, try structured list items (best signal for provider).
    candidates: List[tuple[int, Optional[str]]] = []
    try:
        for li in section.find_all("li"):
            item_text = li.get_text(" ", strip=True)
            miles_val = _extract_miles_number(item_text)
            if miles_val is None:
                continue
            provider = _extract_provider(item_text)
            candidates.append((miles_val, provider))
    except Exception:
        candidates = []

    if candidates:
        # Prefer Miles&More / Flying Blue when present; otherwise keep the maximum miles.
        preferred = [c for c in candidates if _is_preferred_provider(c[1])]
        pool = preferred if preferred else candidates
        best_miles, best_provider = max(pool, key=lambda x: x[0])
        if best_provider:
            return f"{_fmt(best_miles)} · {best_provider}"
        return _fmt(best_miles)

    # Fallback: scan full text for miles numbers (provider may be missing).
    text = section.get_text(" ", strip=True)
    if not text:
        return None

    values: List[int] = []
    for m in re.finditer(r"(\d[\d.,']*)\s*Award Miles\b", text, re.IGNORECASE):
        v = _parse_int(m.group(1))
        if v is not None:
            values.append(v)
    for m in re.finditer(
        r"(\d[\d.,']*)\s*(?<!Tier\s)(?<!Status\s)(?<!Elite\s)Miles\b",
        text,
        re.IGNORECASE,
    ):
        v = _parse_int(m.group(1))
        if v is not None:
            values.append(v)

    if not values:
        return None

    best_miles = max(values)
    return _fmt(best_miles)


def _extract_baggage_from_section(section: BeautifulSoup) -> Optional[str]:
    """Try to extract a short baggage summary from Search & Book block."""
    if not section:
        return None
    text = section.get_text(" ", strip=True)
    if not text:
        return None

    kw_re = re.compile(
        r"\b(?:baggage|luggage|gepäck|handgepäck|aufgabegepäck|koffer|carry\s*-?on|personal\s*item)\b",
        re.IGNORECASE,
    )

    # Split into sentences when possible (works for DE+EN).
    try:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s and s.strip()]
    except Exception:
        sentences = [text]

    def _clip(s: str, max_len: int = 220) -> str:
        s = (s or "").strip()
        if len(s) <= max_len:
            return s
        return s[: max_len - 1].rstrip() + "…"

    # Avoid false positives coming from breadcrumbs/menus.
    bad_sentence_re = re.compile(r"Home\s*»|Dealz\s*»|Flights\s*»", re.IGNORECASE)

    # Require some concrete baggage detail; otherwise we prefer returning None
    # over persisting a polluted long sentence.
    detail_re = re.compile(
        r"(\b\d+\s*(?:x\s*)?\d*\s*kg\b|\b\d+\s*(?:pc|pcs|piece|pieces|st\u00fcck|st\u00fccke)\b|\bfreigep\u00e4ck\b|\binclusive\b|\binkl\.?\b)",
        re.IGNORECASE,
    )

    for sent in sentences:
        if bad_sentence_re.search(sent):
            continue
        if not (kw_re.search(sent) or re.search(r"\b\d+\s*kg\b", sent, re.IGNORECASE)):
            continue
        if not detail_re.search(sent):
            continue
        out = sent.strip()
        return _clip(out) if out else None

    # Fallback: return a clipped window around first keyword match.
    m = kw_re.search(text)
    if m:
        start = max(0, m.start() - 80)
        end = min(len(text), m.end() + 160)
        out = text[start:end].strip()
        if out and (not bad_sentence_re.search(out)) and detail_re.search(out):
            return _clip(out)

    return None


def _extract_airline_from_intro(soup: BeautifulSoup) -> Optional[str]:
    """Try to infer airline name from the intro paragraphs."""
    body = soup.find("article") or soup.find("main") or soup.body
    if not body:
        return None

    # Look at first few paragraphs
    paras = body.find_all("p", limit=5)
    text = " ".join(p.get_text(" ", strip=True) for p in paras)
    if not text:
        return None

    # Patrón principal: "SkyTeam member Vietnam Airlines is offering ...",
    # "Oneworld member British Airways has once again launched ...", etc.
    m = re.search(
        r"(SkyTeam|Oneworld|Star Alliance)\s+member\s+([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)*)",
        text,
    )
    if m:
        return m.group(2).strip()

    # Patrón genérico: "Etihad Airways is offering a deal", "X offers ..."
    m = re.search(
        r"([A-Z][A-Za-z]*(?:\s+[A-Z][A-Za-z]*)*)\s+"
        r"(?:is offering|offers|are offering|is currently offering|has launched|has once again launched|launched|is selling|sells)",
        text,
    )
    if m:
        return m.group(1).strip()

    # Oneworld carrier Iberia offers ...
    m = re.search(r"Oneworld carrier\s+([A-Z][A-Za-z &']+)", text)
    if m:
        return m.group(1).strip()

    return None


def _extract_airline_from_article_classes(soup: BeautifulSoup) -> Optional[str]:
    """Fallback: infer airline from <article> CSS classes (airline-...).

    Ignora clases de alianzas como airline-oneworld, airline-skyteam, etc.
    """
    article = soup.find("article")
    if not article:
        return None
    classes = article.get("class") or []
    airline_names: List[str] = []
    for cls in classes:
        if not isinstance(cls, str) or not cls.startswith("airline-"):
            continue
        slug = cls[len("airline-") :]
        if slug in {"oneworld", "skyteam", "star-alliance"}:
            continue
        parts = [p for p in slug.split("-") if p]
        if not parts:
            continue
        name = " ".join(p.capitalize() for p in parts)
        airline_names.append(name)
    return airline_names[0] if airline_names else None


def _extract_origin_from_article_classes(soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
    """Infer origin city name and IATA from <article> CSS classes.

    Busca clases tipo origins-dublin-airport-dub y devuelve ("Dublin", "DUB").
    """
    article = soup.find("article")
    if not article:
        return None, None
    classes = article.get("class") or []
    for cls in classes:
        if not isinstance(cls, str) or not cls.startswith("origins-"):
            continue
        slug = cls[len("origins-") :]
        parts = [p for p in slug.split("-") if p]
        if not parts:
            continue
        iata: Optional[str] = None
        if len(parts[-1]) == 3 and parts[-1].isalpha():
            iata = parts[-1].upper()
            name_parts = parts[:-1]
        else:
            name_parts = parts

        # Quitar sufijos genéricos como "airport"
        filtered = [p for p in name_parts if p not in {"airport"}]
        if not filtered:
            filtered = name_parts
        city_name = " ".join(p.capitalize() for p in filtered)
        return (city_name or None), iata
    return None, None


def _extract_aircraft_from_body(soup: BeautifulSoup) -> Optional[str]:
    """Heuristic aircraft type extraction from article body."""
    body = soup.find("article") or soup.find("main") or soup.body
    if not body:
        return None
    text = body.get_text(" ", strip=True)
    if not text:
        return None

    # Detect Airbus/Boeing/Embraer with optional dash (p.ej. "Airbus-A320-Serie")
    m = re.search(
        r"(Airbus[\s-]*[A0-9]{3,4}[A-Za-z\-]*|Boeing[\s-]*[0-9]{3,4}[A-Za-z\-]*|Embraer[\s-]*E?[0-9]{1,4}[A-Za-z\-]*)",
        text,
        re.IGNORECASE,
    )
    if m:
        raw = m.group(1).strip()
        # Quitar sufijo genérico "Serie" y normalizar espacios/guiones
        cleaned = re.sub(r"(?i)\bserie\b", "", raw)
        cleaned = cleaned.replace("-", " ")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned or raw
    return None


def _extract_destinations_from_destination_section(soup: BeautifulSoup) -> List[str]:
    """Extract destination city names from 'Destination / Information & Tips' blocks.

    Travel-Dealz suele tener secciones como:
      - "Destination"
      - "Information & Tips for Seoul"
    y, en caso de múltiples destinos, varios bloques similares.
    """
    dests: List[str] = []

    # Look for headings/summary elements containing "Information & Tips for" and
    # capture the city part. En muchos artículos, esto está en un <summary>
    # dentro de <details>, no solo en h2/h3/h4.
    for heading in soup.find_all(["h2", "h3", "h4", "summary"], string=True):
        text = heading.get_text(" ", strip=True)
        if not text:
            continue
        m = re.search(r"Information\s+&\s+Tips\s+for\s+(.+)", text, re.IGNORECASE)
        if m:
            city = m.group(1).strip()
            if city and city not in dests:
                dests.append(city)

    # Bloques cuyo título contiene "Destination" o "Ziel"; extraer texto del
    # propio heading y de su siguiente bloque (enlaces o subtítulos con ciudades).
    for heading in soup.find_all(["h2", "h3", "h4", "summary"], string=True):
        text = heading.get_text(" ", strip=True)
        if not text:
            continue
        if "destination" not in text.lower() and "ziel" not in text.lower():
            continue

        # Si el heading incluye el nombre, p.ej. "Destination: São Paulo"
        m2 = re.search(r"Destination[:\-]\s*(.+)", text, re.IGNORECASE)
        if m2:
            city = m2.group(1).strip()
            if city and city not in dests:
                dests.append(city)

        # Mirar el siguiente bloque por enlaces/títulos con nombres capitalizados
        block = heading.find_next_sibling(["div", "section", "p", "ul", "ol"])
        if block:
            candidates = []
            for tag in block.find_all(["a", "strong", "em", "span", "h5", "h6"], string=True):
                t = tag.get_text(" ", strip=True)
                if not t:
                    continue
                if re.match(r"^[A-Z][A-Za-zÀ-ÖØ-öø-ÿ'\-\s]{2,}$", t):
                    candidates.append(t)
            for li in block.find_all("li"):
                t = li.get_text(" ", strip=True)
                if re.match(r"^[A-Z][A-Za-zÀ-ÖØ-öø-ÿ'\-\s]{2,}$", t or ""):
                    candidates.append(t)
            for c in candidates:
                if c and c not in dests:
                    dests.append(c)

    return dests


def _extract_dates_and_expiry(
    section: Optional[BeautifulSoup],
    heading: Optional[BeautifulSoup] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Extract travel_dates_text and expires_in from Search & Book block.

    - travel_dates_text: the first <p> *after* the "Search & Book" heading.
    - expires_in: sentence mentioning expiry/"on sale until", from full text.
    """
    if not section and not heading:
        return None, None

    # travel_dates_text: first <p> after the heading, if available
    travel_text = None
    if heading is not None:
        first_p = heading.find_next_sibling("p")
        if first_p is not None:
            travel_text = first_p.get_text(" ", strip=True)

            # Algunos artículos usan una letra inicial tipo "drop cap" en un
            # nodo separado justo antes del párrafo, de forma que el <p>
            # contiene "he expiration date..." en lugar de "The expiration...".
            # Corregimos explícitamente este patrón genérico de Travel-Dealz.
            if travel_text.lower().startswith("he expiration date of this offer is not specified"):
                travel_text = "T" + travel_text

    # Fallback: first <p> inside the section
    if not travel_text and section is not None:
        first_p = section.find("p")
        if first_p is not None:
            travel_text = first_p.get_text(" ", strip=True)

    # Text used to detect expiry comes from the whole section
    base = section or (heading.parent if heading is not None else None)
    if not base:
        return travel_text, None

    text = base.get_text(" ", strip=True)
    if not text:
        return travel_text, None

    expires = None
    # Look for sentences with "expire" or "on sale until"
    m = re.search(r"(expire[s]?\s+on\s+[^.]+\.)", text, re.IGNORECASE)
    if m:
        expires = m.group(1).strip()
    else:
        m = re.search(r"(on sale (?:until|through)\s+[^.]+\.)", text, re.IGNORECASE)
        if m:
            expires = m.group(1).strip()

    return travel_text, expires


def _build_itineraries_from_page_text(
    soup: BeautifulSoup,
    title: str,
    destinations: List[str],
    origin_name_meta: Optional[str],
    origin_iata_meta: Optional[str],
    base_url: str,
) -> List[Dict[str, Any]]:
    """Fallback: build coarse itineraries only from page text.

    Pensado para artículos que no tienen booking URLs tipo go2.travel-dealz,
    como el ejemplo de Icelandair donde solo hay enlaces a Google Flights.

    Estrategia muy conservadora:
      - obtener un precio del título (o, si falla, del bloque "Search & Book"),
      - usar la lista de destinos deducida de la sección "Destination",
      - usar el origen oficial extraído de las clases del <article>, si existe.

    No intenta deducir códigos IATA de destino ni fechas precisas porque la
    estructura no es trivial y depende de Google Flights.
    """

    itineraries: List[Dict[str, Any]] = []

    # 1) Precio base: primero intentamos con el título del artículo.
    price, currency = _parse_price_from_text(title or "")

    # También intentamos localizar una booking_url útil (p. ej. Google Flights)
    # en la sección "Search & Book" o, si falla, en todo el artículo.
    booking_url: Optional[str] = None

    # Fallback: intentar encontrar un precio en "Search & Book" si el título
    # no contiene ninguno (o el patrón falla), y de paso detectar booking_url.
    if price is None:
        sb_section = _extract_section_by_heading_any(soup, ["Search & Book", "Suchen & Buchen", "Suchen und Buchen"])
        if sb_section is not None:
            sb_text = sb_section.get_text(" ", strip=True)
            if sb_text:
                price, currency = _parse_price_from_text(sb_text)

            # Buscar primero enlaces de Google Flights dentro del bloque
            for a in sb_section.find_all("a", href=True):
                href = a["href"].strip()
                if not href:
                    continue
                if href.startswith("//"):
                    href = "https:" + href
                if href.startswith("/"):
                    href = urljoin(base_url, href)
                if "google.com/travel/flights" in href or "google.de/travel/flights" in href:
                    booking_url = href
                    break

    # Si aún no tenemos booking_url, buscamos en todo el documento un enlace
    # de Google Flights como último recurso.
    if booking_url is None:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href
            if href.startswith("/"):
                href = urljoin(base_url, href)
            if "google.com/travel/flights" in href or "google.de/travel/flights" in href:
                booking_url = href
                break

    # Si seguimos sin precio, no creamos itinerarios sintéticos.
    if price is None:
        return itineraries

    # Si no hay destinos claros, intenta extraer destino desde el título del artículo
    if not destinations and title:
        _, t_dest = _extract_route_from_title(title)
        if t_dest:
            destinations = [t_dest]

    # 2) Destinos: usamos la lista ya detectada. En artículos con stopover en
    #    Islandia suele aparecer también "Iceland" como destino informativo;
    #    si hay más destinos, lo filtramos para no tratarlo como destino final.
    dests = list(destinations) if destinations else []
    if len(dests) > 1:
        filtered = [d for d in dests if "iceland" not in d.lower()]
        if filtered:
            dests = filtered

    # Si no hay destinos claros, devolvemos un solo itinerario genérico.
    if not dests:
        itineraries.append(
            {
                "origin": origin_name_meta,
                "destination": None,
                "price": price,
                "currency": currency,
                "booking_url": booking_url,
                "origin_iata": origin_iata_meta,
                "destination_iata": None,
            }
        )
        return itineraries

    # 3) Crear un itinerario por destino conocido, compartiendo precio.
    for dest in dests:
        itineraries.append(
            {
                "origin": origin_name_meta,
                "destination": dest,
                "price": price,
                "currency": currency,
                "booking_url": booking_url,
                "origin_iata": origin_iata_meta,
                "destination_iata": None,
            }
        )

    return itineraries


def _parse_travel_dealz_soup(soup: BeautifulSoup, url: str) -> Dict[str, Any]:
    """Parse a Travel-Dealz article from an already-created BeautifulSoup."""

    # Title
    title = ""
    h = soup.find(["h1", "h2"], class_=re.compile("title", re.IGNORECASE)) or soup.find("h1")
    if h and h.get_text(strip=True):
        title = h.get_text(strip=True)

    # Airline, aircraft, destinations
    airline = _extract_airline_from_intro(soup) or _extract_airline_from_article_classes(soup)
    aircraft = _extract_aircraft_from_body(soup)
    destinations = _extract_destinations_from_destination_section(soup)

    # Origen "oficial" del deal desde las clases del <article>, si está disponible
    origin_name_meta, origin_iata_meta = _extract_origin_from_article_classes(soup)

    # Search & Book section: solo para fechas/expiry
    sb_section = _extract_section_by_heading_any(soup, ["Search & Book", "Suchen & Buchen", "Suchen und Buchen"])
    # Prefer explicit ids used by Travel-Dealz if present, then fall back to text search
    sb_heading = (
        soup.find(id="h-search-book")
        or soup.find(id="h-suchen-amp-buchen")
        or soup.find(
            ["h2", "h3", "h4"],
            string=lambda s: isinstance(s, str)
            and (
                "search & book" in s.lower()
                or "suchen & buchen" in s.lower()
                or "suchen und buchen" in s.lower()
            ),
        )
    )
    # Itinerarios: buscar booking URLs en todo el HTML, no solo en Search & Book.
    # Si no hay enlaces tipo go2.travel-dealz, usamos un fallback basado en
    # el propio texto de la página (título + sección Destination).
    itineraries = _extract_itineraries_from_booking_links(soup, base_url=url, fallback_title=title)
    if not itineraries:
        itineraries = _build_itineraries_from_page_text(
            soup=soup,
            title=title,
            destinations=destinations,
            origin_name_meta=origin_name_meta,
            origin_iata_meta=origin_iata_meta,
            base_url=url,
        )
    travel_dates_text, expires_in = _extract_dates_and_expiry(sb_section, heading=sb_heading)

    # Cabin baggage: prefer Search & Book, then fallback to whole article.
    body = soup.find("article") or soup.find("main") or soup.body
    cabin_baggage = _extract_baggage_from_section(sb_section) or _extract_baggage_from_section(body)

    # Miles & Points (DE pages use "Meilen")
    mp_section = _extract_section_by_heading_any(soup, ["Miles & Points", "Meilen & Punkte", "Meilen"])
    miles_value = _extract_miles_from_section(mp_section)

    # If there's a single clear destination, propagate it to each itinerary
    if len(destinations) == 1:
        dest_city = destinations[0]
        for it in itineraries:
            # Solo si no tenemos destino ni destination_iata
            if ("destination" not in it or not it.get("destination")) and not it.get("destination_iata"):
                it["destination"] = dest_city
    elif destinations:
        # Best-effort: si hay varios destinos y faltan destinos en itinerarios,
        # usa el primero SOLO cuando tampoco hay destination_iata.
        for it in itineraries:
            if not it.get("destination") and not it.get("destination_iata"):
                it["destination"] = destinations[0]

    # Si alguna ciudad usada como origen en las líneas de precio coincide con
    # una ciudad de la sección Destination, la tratamos como DESTINO, no como
    # origen: movemos ese valor a destination (si está vacío) y borramos el
    # origen. Esto cubre patrones como "You can reach these airports ..."
    # donde las ciudades de la lista son destinos reales.
    if destinations:
        dest_norm = {_normalize_city_name(c) for c in destinations}
        for it in itineraries:
            orig = it.get("origin")
            if not orig:
                continue
            if _normalize_city_name(orig) in dest_norm:
                if not it.get("destination"):
                    it["destination"] = orig
                it["origin"] = None

    # Si conocemos un origen "oficial" por clases (p.ej. Dublin / DUB),
    # lo usamos para completar o incluso corregir el sentido del itinerario:
    #
    # - caso normal: origin_iata == origin_iata_meta → rellenamos el nombre
    # - caso invertido (como el ejemplo de Norse a CPT): si destination_iata
    #   coincide con origin_iata_meta pero origin_iata no, asumimos que la URL
    #   de booking está "al revés" (CPT→LGW) y la invertimos a LGW→CPT.
    if origin_name_meta and origin_iata_meta:
        for it in itineraries:
            o_iata = it.get("origin_iata")
            d_iata = it.get("destination_iata")

            # Caso 1: el origen ya coincide con el meta → sólo ponemos nombre
            if o_iata == origin_iata_meta:
                if not it.get("origin"):
                    it["origin"] = origin_name_meta
                continue

            # Caso 2: el destino coincide con el meta pero el origen no →
            # interpretamos que el sentido está invertido y lo cambiamos.
            if d_iata == origin_iata_meta and o_iata and d_iata:
                # Guardamos el destino "real" (normalmente la ciudad principal
                # del artículo, p.ej. "Cape Town") antes de tocar nada.
                dest_city = it.get("destination")
                orig_city = it.get("origin")

                # Invertir códigos IATA
                it["origin_iata"], it["destination_iata"] = d_iata, o_iata

                # Origen pasa a ser el meta oficial (p.ej. "London Gatwick")
                it["origin"] = origin_name_meta

                # Destino: preferimos la ciudad que ya estaba en destination,
                # y si no, usamos el antiguo origen si existía.
                if dest_city and dest_city != origin_name_meta:
                    it["destination"] = dest_city
                elif orig_city and orig_city != origin_name_meta:
                    it["destination"] = orig_city

    # Fallback adicional: si sólo hay un itinerario y el título del
    # artículo contiene un patrón reconocible (p.ej. "... from Budapest"),
    # usamos esa información para rellenar origen/destino si siguen vacíos.
    if len(itineraries) == 1:
        it = itineraries[0]
        if isinstance(it, dict):
            o_title, d_title = _extract_route_from_title(title or "")
            if o_title and not it.get("origin"):
                it["origin"] = o_title
            if d_title and not it.get("destination"):
                it["destination"] = d_title

    # Fallback para múltiples itinerarios con el mismo par de IATA:
    # si todas las rutas comparten origin_iata y destination_iata y
    # el título permite extraer un patrón "from X" → "to Y", usamos
    # esos nombres legibles para rellenar origin/destination cuando
    # aún estén vacíos.
    if len(itineraries) > 1 and title:
        o_title, d_title = _extract_route_from_title(title or "")
        if o_title or d_title:
            all_o_iata = {it.get("origin_iata") for it in itineraries if isinstance(it, dict)}
            all_d_iata = {it.get("destination_iata") for it in itineraries if isinstance(it, dict)}

            for it in itineraries:
                if not isinstance(it, dict):
                    continue
                if o_title and len(all_o_iata) == 1 and not it.get("origin"):
                    it["origin"] = o_title
                if d_title and len(all_d_iata) == 1 and not it.get("destination"):
                    it["destination"] = d_title

    # Completar nombres de ciudad a partir de códigos IATA si aún faltan
    for it in itineraries:
        if not isinstance(it, dict):
            continue
        resolved_origin = _resolve_city_from_iata(it.get("origin"), it.get("origin_iata"))
        if resolved_origin:
            # Si el nombre existente no coincide con el IATA, priorizamos el mapeo
            if it.get("origin") and _normalize_city_name(str(it.get("origin"))) != _normalize_city_name(resolved_origin):
                it["origin"] = resolved_origin
            elif not it.get("origin"):
                it["origin"] = resolved_origin

        resolved_dest = _resolve_city_from_iata(it.get("destination"), it.get("destination_iata"))
        if resolved_dest:
            if it.get("destination") and _normalize_city_name(str(it.get("destination"))) != _normalize_city_name(resolved_dest):
                it["destination"] = resolved_dest
            elif not it.get("destination"):
                it["destination"] = resolved_dest

    # Si no hemos identificado destinos globales, deducirlos de las rutas ya resueltas
    if not destinations:
        dests_from_itins: List[str] = []
        for it in itineraries:
            if not isinstance(it, dict):
                continue
            dest_name = it.get("destination")
            if dest_name and dest_name not in dests_from_itins:
                dests_from_itins.append(dest_name)
        if dests_from_itins:
            destinations = dests_from_itins

    # Fallback de precio: si todos los itinerarios tienen price == None,
    # intentamos extraer un precio "global" del cuerpo del artículo
    # (p.ej. "starting at just €1,312 ...") y lo aplicamos a todos.
    if itineraries and all(it.get("price") is None for it in itineraries):
        body = soup.find("article") or soup.find("main") or soup.body
        if body:
            body_text = body.get_text(" ", strip=True)
            if body_text:
                fallback_price, fallback_currency = _parse_price_from_text(body_text)
                if fallback_price is not None:
                    for it in itineraries:
                        it["price"] = fallback_price
                        it["currency"] = fallback_currency

    result: Dict[str, Any] = {
        "status": "ok",
        "url": url,
        "title": title,
        "airline": airline,
        "aircraft": aircraft,
        "destinations": destinations,
        "travel_dates_text": travel_dates_text,
        "expires_in": expires_in,
        "miles": miles_value,
        "cabin_baggage": cabin_baggage,
        "itineraries": itineraries,
    }

    return result


def parse_travel_dealz_article_from_html(html: str, url: str) -> Dict[str, Any]:
    """Parse a Travel-Dealz article from raw HTML.

    Useful for deterministic debugging by saving HTML snapshots and re-running
    extraction logic without depending on live pages.
    """

    if not html or not isinstance(html, str):
        return {"status": "error", "error": "empty_html"}

    soup = BeautifulSoup(html, "html.parser")
    return _parse_travel_dealz_soup(soup, url=url)


def parse_travel_dealz_article_from_file(file_path: str, url: Optional[str] = None) -> Dict[str, Any]:
    """Parse a Travel-Dealz article from a saved HTML file."""

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            html = f.read()
    except Exception as e:
        return {"status": "error", "error": "failed_to_read_file", "details": str(e), "file_path": file_path}

    effective_url = url or f"file:{file_path}"
    return parse_travel_dealz_article_from_html(html, url=effective_url)


def parse_travel_dealz_article(url: str) -> Dict[str, Any]:
    """Scrape a single Travel-Dealz article and return structured data.

    This function is purely scraping-based (no OpenAI).
    """

    html = _fetch_html(url)
    if not html:
        return {"status": "error", "error": "failed_to_fetch"}

    return parse_travel_dealz_article_from_html(html, url=url)
