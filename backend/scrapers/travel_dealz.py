from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, unquote, urljoin
import logging
import time


logger = logging.getLogger("snapcore.scrapers.travel_dealz")


def _fetch_html(url: str, timeout: int = 10, retries: int = 2) -> str | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.7",
    }

    last_err: Exception | None = None
    for attempt in range(max(1, int(retries) + 1)):
        try:
            resp = requests.get(url, headers=headers, timeout=int(timeout))
            if resp.status_code != 200:
                logger.warning("[travel-dealz] non-200 status=%s url=%s", resp.status_code, url)
                return None
            return resp.text
        except Exception as e:
            last_err = e
            # Small backoff to survive transient DNS/connect issues.
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
            continue

    if last_err is not None:
        logger.warning("[travel-dealz] request failed url=%s error=%r", url, last_err)
    return None


def _is_non_flight_deal(title: str, link: str) -> bool:
    """Heurística sencilla para descartar posts que no son vuelos.

    Ejemplos: AirHelp Plus, seguros, servicios legales, etc.
    De momento sólo cubrimos el caso de AirHelp para no ser
    demasiado agresivos.
    """

    t = (title or "").lower()
    l = (link or "").lower()

    # AirHelp, seguros, servicios legales, etc.
    if "airhelp" in t or "airhelp" in l:
        return True

    # Gift cards, vales, códigos de descuento genéricos (no itinerarios)
    giftcard_keywords = [
        "gift card",
        "gift-card",
        "gift cards",
        "gift-cards",
        "voucher",
    ]
    if any(kw in t for kw in giftcard_keywords) or any(kw in l for kw in giftcard_keywords):
        return True

    # Caso explícito reportado: ITA Airways gift cards
    if "ita-airways-gift-cards" in l:
        return True

    return False


def _extract_price(text: str) -> tuple[float | None, str]:
    """Extract price and currency from text.

    Travel-Dealz suele escribir precios como "1,071" o "2.613" donde las
    comas/puntos son separadores de miles, no decimales. Para evitar
    interpretar "1,071" como 1.071 €, conservamos solo los dígitos y
    tratamos el resultado como un entero en la divisa detectada.
    """

    # Look for patterns like €320, 320€, $280, 280 EUR, etc.
    patterns = [
        (r"€\s*(\d[\d.,']*)", "EUR"),
        (r"(\d[\d.,']*)\s*€", "EUR"),
        (r"\$\s*(\d[\d.,']*)", "USD"),
        (r"(\d[\d.,']*)\s*USD", "USD"),
        (r"(\d[\d.,']*)\s*EUR", "EUR"),
        (r"(\d[\d.,']*)\s*CHF", "CHF"),
    ]

    for pattern, currency in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue

        raw = match.group(1)
        # Conservar solo dígitos, tratando ',' '.' y "'" como separadores de miles
        digits = re.sub(r"[^0-9]", "", raw)
        if not digits:
            continue
        try:
            value = float(digits)
        except ValueError:
            continue
        return value, currency

    return None, "EUR"


def _extract_route_from_title(title: str) -> tuple[str | None, str | None]:
    """Best-effort extraction of origin/destination from the deal title.

    Looks for common separators like "Zürich → Kapstadt", "MAD – NYC", etc.
    Returns (origin, destination) or (None, None) if no pattern is found.

    Se evita usar un '-' simple como separador porque aparece dentro
    de palabras ("Non-Stop", "Round-trip", ...) y genera falsos positivos.
    """
    if not title:
        return None, None

    raw = title.replace("\u2192", "→")  # normalize arrow if needed

    separators = [
        "→",        # City → City
        "↔",        # City ↔ City
        " – ",      # City – City (en dash con espacios)
        " - ",      # City - City (hyphen con espacios)
        " to ",     # City to City
    ]

    for sep in separators:
        if sep in raw:
            left, right = raw.split(sep, 1)
            left = left.strip()
            right = right.strip()

            # Si alguna de las partes contiene dígitos o símbolos de moneda,
            # asumimos que no es solo nombre de ciudad y descartamos.
            if any(ch.isdigit() for ch in left + right):
                continue
            if any(sym in (left + right) for sym in ["€", "$", "£", "CHF", "USD", "EUR"]):
                continue

            if len(left) >= 3 and len(right) >= 3:
                return left, right

    # Patrón adicional típico de Travel-Dealz:
    #   "Perth, Australia: €2,786 Star Alliance Business Class from Budapest"
    # Interpretamos lo que va antes de los dos puntos como DESTINO
    # y el fragmento después de "from" como ORIGEN.
    if ":" in raw and " from " in raw.lower():
        dest_part = raw.split(":", 1)[0].strip()
        m = re.search(r"from\s+([A-Za-z\s\-/]+?)(?:$|[,(])", raw, re.IGNORECASE)
        if m:
            origin_part = m.group(1).strip()
            if len(origin_part) >= 3 and len(dest_part) >= 3:
                return origin_part, dest_part

    return None, None


def _extract_image_from_article(article, base_url: str) -> str | None:
    """Try to get a representative image URL from a listing card/article."""
    if not article:
        return None
    img = article.find('img')
    if not img:
        return None
    src = img.get('data-src') or img.get('data-lazy-src') or img.get('src')
    if not src:
        return None
    src = src.strip()
    if src.startswith('//'):
        src = 'https:' + src
    if src.startswith('/'):
        src = urljoin(base_url, src)
    return src


def get_deals(limit: int = 100) -> List[Dict]:
    """Scrape travel deals from travel-dealz.com/category/flights/.

    No content filters: just go page by page and extract
    title, price (if any) and link from each article/card.
    """
    deals: List[Dict] = []

    try:
        base_url = "https://travel-dealz.com"
        category_url = f"{base_url}/category/flights"

        page = 1
        # Menos páginas por defecto para que la API responda rápido, pero si el
        # caller pide un límite alto (p.ej. bulk ingest), necesitamos recorrer
        # más páginas o nunca llegaremos a `limit`.
        # Estimación: ~20 posts/página.
        try:
            pages_needed = max(1, (int(limit) + 19) // 20)
        except Exception:
            pages_needed = 5
        max_pages = max(5, min(50, pages_needed + 1))

        while page <= max_pages and len(deals) < limit:
            # Page 1 is the base category URL, next pages use /page/N/
            if page == 1:
                page_url = f"{category_url}/"
            else:
                page_url = f"{category_url}/page/{page}/"

            html = _fetch_html(page_url, timeout=10, retries=2)
            if not html:
                break

            soup = BeautifulSoup(html, 'html.parser')

            # Generic search for posts/deal cards
            articles = soup.find_all(['article', 'div'], class_=re.compile(r'(post|deal|offer|card)', re.I))
            if not articles:
                logger.warning("[travel-dealz] no articles found url=%s", page_url)
                break

            for article in articles:
                # Title: robust heuristics
                title = ""
                # 1) Heading elements with text
                for tag in ('h1', 'h2', 'h3'):
                    h = article.find(tag)
                    if h and h.get_text(strip=True):
                        title = h.get_text(strip=True)
                        break

                # 2) Link inside heading (common pattern: <h2><a>Title</a></h2>)
                if not title:
                    for tag in ('h1', 'h2', 'h3'):
                        a = article.find(tag)
                        if a:
                            a_link = a.find('a', href=True)
                            if a_link and a_link.get_text(strip=True):
                                title = a_link.get_text(strip=True)
                                break

                # 3) <a rel="bookmark"> or first meaningful link to a /deal/
                if not title:
                    a_book = article.find('a', attrs={'rel': 'bookmark'})
                    if a_book and a_book.get_text(strip=True):
                        title = a_book.get_text(strip=True)
                if not title:
                    # prefer links that point to /deal/ urls and have text
                    for a in article.find_all('a', href=True):
                        href = a['href']
                        txt = a.get_text(strip=True)
                        if txt and '/deal/' in href:
                            title = txt
                            break

                # 4) Any link with reasonable text
                if not title:
                    for a in article.find_all('a', href=True):
                        txt = a.get_text(strip=True)
                        if txt and len(txt) > 3:
                            title = txt
                            break

                # 5) Fallback: block text
                if not title:
                    title = article.get_text(strip=True)[:200]

                # Link: first href in the article/card
                link_elem = article.find('a', href=True)
                link = link_elem['href'] if link_elem else base_url
                if link.startswith('/'):
                    link = base_url.rstrip('/') + link

                # Solo consideramos URLs de tipo /deal/...; ignorar ticker, promos, etc.
                if '/deal/' not in link:
                    continue

                # Descartar posts que claramente no son vuelos (p.ej. AirHelp Plus).
                if _is_non_flight_deal(title, link):
                    continue

                # If we still don't have a title, derive one from the URL slug
                if not title and link:
                    try:
                        parsed = urlparse(link)
                        slug = parsed.path.rstrip('/').split('/')[-1]
                        if slug:
                            title = unquote(slug).replace('-', ' ').strip().title()[:200]
                    except Exception:
                        pass

                # Price (best-effort; no filtering if missing)
                price_text = article.get_text()
                price, currency = _extract_price(price_text)

                # Origin / destination heuristic from title
                origin, destination = _extract_route_from_title(title)

                # Image from listing card if present
                image_url = _extract_image_from_article(article, base_url=base_url)

                deal = {
                    "title": title[:200] if title else "",
                    "price": price,
                    "currency": currency,
                    "link": link,
                    "origin": origin,
                    "destination": destination,
                    "image_url": image_url,
                    "source": "Travel-Dealz",
                }
                deals.append(deal)

                if len(deals) >= limit:
                    return deals

            page += 1

    except Exception as e:
        # In case of error, just return whatever we already collected
        logger.warning("[travel-dealz] unexpected error=%r", e)

    return deals


def get_deals_de(limit: int = 100) -> List[Dict]:
    """Scrape travel deals from https://travel-dealz.de/kategorie/fluge/.

    Mismo comportamiento que get_deals() pero apuntando al dominio .de.
    Sin filtros de contenido: recorre página por página y extrae
    título, precio (si lo hay) y enlace de cada artículo/tarjeta.
    """
    deals: List[Dict] = []

    try:
        base_url = "https://travel-dealz.de"
        category_url = f"{base_url}/kategorie/fluge"

        page = 1
        # Igual que en .com: crecer max_pages cuando el límite es alto.
        try:
            pages_needed = max(1, (int(limit) + 19) // 20)
        except Exception:
            pages_needed = 5
        max_pages = max(5, min(50, pages_needed + 1))

        while page <= max_pages and len(deals) < limit:
            if page == 1:
                page_url = f"{category_url}/"
            else:
                page_url = f"{category_url}/page/{page}/"

            html = _fetch_html(page_url, timeout=10, retries=2)
            if not html:
                # Keep log prefix compatible with existing messages.
                logger.warning("[travel-dealz.de] request failed url=%s", page_url)
                break

            soup = BeautifulSoup(html, 'html.parser')

            articles = soup.find_all(['article', 'div'], class_=re.compile(r'(post|deal|offer|card)', re.I))
            if not articles:
                logger.warning("[travel-dealz.de] no articles found url=%s", page_url)
                break

            for article in articles:
                title_elem = article.find(['h1', 'h2', 'h3', 'a'], class_=re.compile(r'(title|heading)', re.I))
                if not title_elem:
                    title_elem = article.find(['h1', 'h2', 'h3', 'a'])

                title = title_elem.get_text(strip=True) if title_elem else article.get_text(strip=True)[:200]

                link_elem = article.find('a', href=True)
                link = link_elem['href'] if link_elem else base_url
                if link.startswith('/'):
                    link = base_url.rstrip('/') + link

                # Solo consideramos URLs de tipo /deal/...; ignorar ticker, promos, etc.
                if '/deal/' not in link:
                    continue

                # Descartar posts que claramente no son vuelos (p.ej. AirHelp Plus).
                if _is_non_flight_deal(title, link):
                    continue

                # If we still don't have a title, derive one from the URL slug
                if not title and link:
                    try:
                        parsed = urlparse(link)
                        slug = parsed.path.rstrip('/').split('/')[-1]
                        if slug:
                            title = unquote(slug).replace('-', ' ').strip().title()[:200]
                    except Exception:
                        pass

                price_text = article.get_text()
                price, currency = _extract_price(price_text)

                origin, destination = _extract_route_from_title(title)
                image_url = _extract_image_from_article(article, base_url=base_url)

                deals.append({
                    "title": title[:200] if title else "",
                    "price": price,
                    "currency": currency,
                    "link": link,
                    "origin": origin,
                    "destination": destination,
                    "image_url": image_url,
                    "source": "Travel-Dealz.de",
                })

                if len(deals) >= limit:
                    return deals

            page += 1

    except Exception as e:
        logger.warning("[travel-dealz.de] unexpected error=%r", e)

    return deals

