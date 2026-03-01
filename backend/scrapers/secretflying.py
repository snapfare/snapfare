from typing import List, Dict, Optional
import os
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv, find_dotenv

# Ensure .env is loaded even if caller forgot to do it.
load_dotenv(find_dotenv(), override=False)

try:
    # SDK oficial de ScrapingBee (opcional, pero ya no es la
    # opción principal si usamos ScraperAPI).
    from scrapingbee import ScrapingBeeClient  # type: ignore
except Exception:  # pragma: no cover - entorno sin scrapingbee instalado
    ScrapingBeeClient = None  # type: ignore

SWISS_KEYWORDS = ["switzerland", "zurich", "geneva", "basel", "zürich", "genève", "schweiz", "suisse", "svizzera"]
EXCLUDE_KEYWORDS = ["bewertung", "review", "guide", "test", "erfahrung", "bericht", "example", "placeholder", "sample"]
EXCLUDE_PATHS = ["/blog/", "/bewertung/", "/news/", "/guide/", "/test/"]


def _is_valid_deal_url(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    for exclude in EXCLUDE_PATHS:
        if exclude in url_lower:
            return False
    return True


def _extract_price(text: str) -> tuple[float | None, str]:
    patterns = [
        (r'€\s*(\d+(?:[.,]\d+)?)', 'EUR'),
        (r'(\d+(?:[.,]\d+)?)\s*€', 'EUR'),
        (r'\$\s*(\d+(?:[.,]\d+)?)', 'USD'),
        (r'(\d+(?:[.,]\d+)?)\s*USD', 'USD'),
        (r'(\d+(?:[.,]\d+)?)\s*EUR', 'EUR'),
        (r'(\d+(?:[.,]\d+)?)\s*CHF', 'CHF'),
        (r'£\s*(\d+(?:[.,]\d+)?)', 'GBP'),
        (r'(\d+(?:[.,]\d+)?)\s*GBP', 'GBP'),
    ]
    for pattern, currency in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            price_str = match.group(1).replace(',', '.')
            try:
                return float(price_str), currency
            except ValueError:
                continue
    return None, 'USD'


def _extract_route_from_title(title: str) -> tuple[str | None, str | None]:
    """Heuristic origin/destination extraction for SecretFlying titles."""
    if not title:
        return None, None

    raw = title.replace("\u2192", "→")
    separators = ["→", "↔", " – ", " - ", " –", "-", " to "]
    for sep in separators:
        if sep in raw:
            left, right = raw.split(sep, 1)
            left = left.strip()
            right = right.strip()

            # Muchos títulos incluyen el precio y el texto de oferta
            # pegados al destino, por ejemplo:
            #   "Barcelona or Madrid, Spain to Kochi or Mumbai, India from only €219 roundtrip"
            #   "Beijing, China to Dublin, Ireland for only $462 USD roundtrip"
            # Queremos que destination sea solo la parte geográfica.
            right = re.sub(r"\bfrom only\b.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfrom\b\s+€?\d.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfrom\b\s+\$?\d.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfor only\b.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfor\b\s+€?\d.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfor\b\s+\$?\d.*$", "", right, flags=re.IGNORECASE).strip()

            if len(left) >= 3 and len(right) >= 3:
                return left, right
    return None, None


def _parse_articles_from_soup(soup: BeautifulSoup, base_url: str, deals: List[Dict], limit: int) -> List[Dict]:
    articles = soup.find_all(['article', 'div'], class_=re.compile(r'(post|deal|flight|entry)', re.I))
    if not articles:
        return deals

    for article in articles:
        title = ""
        for tag in ('h1', 'h2', 'h3'):
            h = article.find(tag)
            if h and h.get_text(strip=True):
                title = h.get_text(strip=True)
                break

        if not title:
            for tag in ('h1', 'h2', 'h3'):
                a = article.find(tag)
                if a:
                    a_link = a.find('a', href=True)
                    if a_link and a_link.get_text(strip=True):
                        title = a_link.get_text(strip=True)
                        break

        if not title:
            a_book = article.find('a', attrs={'rel': 'bookmark'})
            if a_book and a_book.get_text(strip=True):
                title = a_book.get_text(strip=True)

        if not title:
            for a in article.find_all('a', href=True):
                href = a['href']
                txt = a.get_text(strip=True)
                if txt and '/deal/' in href:
                    title = txt
                    break

        if not title:
            for a in article.find_all('a', href=True):
                txt = a.get_text(strip=True)
                if txt and len(txt) > 3:
                    title = txt
                    break

        if not title:
            title = article.get_text(strip=True)[:200]

        # Selección explícita de enlaces:
        # - "link": URL del post de SecretFlying (https://www.secretflying.com/posts/...)
        # - "booking_url": primer enlace externo (Skyscanner, aerolínea, etc.).
        link = None
        booking_url = None
        for a in article.find_all('a', href=True):
            href = (a.get('href') or '').strip()
            if not href:
                continue

            absolute = href
            if href.startswith('/'):
                absolute = base_url.rstrip('/') + href

            lower = absolute.lower()

            # Enlace interno a SecretFlying
            if 'secretflying.com' in lower:
                # Preferimos claramente los posts
                if '/posts/' in lower:
                    if link is None:
                        link = absolute
                # Si aún no tenemos link de post, aceptamos el primero interno
                elif link is None:
                    link = absolute
                continue

            # Primer enlace externo => candidato a booking_url
            if booking_url is None:
                booking_url = absolute

        # Algunos bloques estructurales (p.ej. el buscador "From / To / When / Show past deals")
        # no representan un deal real y acaban generando títulos basura como
        # "FromToWhenShow past deals" y enlaces a la home. Los filtramos aquí.
        normalized_title = (title or "").replace(" ", "").lower()
        if not link or link == base_url:
            # Si el bloque no tiene enlace específico a un post/artículo, lo ignoramos.
            continue
        if "fromtowhenshowpastdeals" in normalized_title:
            continue

        # A veces el título que capturamos es sólo la fecha ("December 30, 2025").
        # En ese caso, derivamos un título más útil a partir del slug de la URL.
        if title and re.fullmatch(r"[A-Za-z]+ \d{1,2}, \d{4}", title.strip()):
            try:
                parsed = urlparse(link)
                slug = parsed.path.rstrip('/').split('/')[-1]
                if slug:
                    pretty = slug.replace('-', ' ').strip()
                    if pretty:
                        # Primera letra mayúscula, resto sin tocar.
                        title = pretty[0].upper() + pretty[1:]
            except Exception:
                pass

        price_text = article.get_text()
        price, currency = _extract_price(price_text)

        origin, destination = _extract_route_from_title(title)

        # Try to grab an image from the listing/card itself
        image_url = None
        img = article.find('img')
        if img and img.get('src'):
            src = img.get('data-src') or img.get('data-lazy-src') or img.get('src')
            if src:
                src = src.strip()
                if src.startswith('//'):
                    src = 'https:' + src
                if src.startswith('/'):
                    src = urljoin(base_url, src)
                image_url = src

        deal = {
            "title": title[:200] if title else "",
            "price": price,
            "currency": currency,
            "link": link,
            "booking_url": booking_url,
            "origin": origin,
            "destination": destination,
            "image_url": image_url,
            "source": "SecretFlying",
        }
        deals.append(deal)

        if len(deals) >= limit:
            return deals

    return deals


def _get_deals_requests(limit: int = 100, session_proxies: Optional[Dict[str, str]] = None) -> List[Dict]:
    """Fetch listing pages with plain requests, optionally using HTTP proxies.

    If ``session_proxies`` is provided, it is passed to ``requests.Session`` as
    the ``proxies`` argument (used for Apify Proxy, etc.).
    """

    deals: List[Dict] = []
    try:
        base_url = "https://www.secretflying.com"
        page = 1
        max_pages = 20

        while page <= max_pages and len(deals) < limit:
            if page == 1:
                page_url = f"{base_url}/"
            else:
                page_url = f"{base_url}/page/{page}/"

            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.google.com/',
                'DNT': '1',
                'Connection': 'keep-alive',
            })
            if session_proxies:
                session.proxies.update(session_proxies)

            try:
                response = session.get(page_url, timeout=10)
            except Exception as e:
                print(f"[secretflying] _get_deals_requests: error fetching {page_url}: {e!r}")
                break

            # Si obtenemos 403 en la petición directa, podemos intentar
            # un único fallback con ScrapingBee *sólo para páginas de
            # listado*, siempre que haya SCRAPINGBEE_API_KEY
            # configurada. Esto permite seguir usando este camino
            # "ligero" para discovery de URLs sin depender de
            # ScraperAPI ni Playwright.
            if response.status_code == 403 and not session_proxies:
                api_key = os.getenv("SCRAPINGBEE_API_KEY")
                if api_key and ScrapingBeeClient is not None:
                    print(
                        f"[secretflying] _get_deals_requests: HTTP 403 for {page_url}, "
                        "retrying via ScrapingBee"
                    )

                    try:
                        client = ScrapingBeeClient(api_key=api_key)

                        render_js_env = os.getenv("SCRAPINGBEE_RENDER_JS")
                        if render_js_env is None:
                            render_js = True
                        else:
                            render_js = render_js_env.strip().lower() in {
                                "1",
                                "true",
                                "yes",
                                "on",
                            }

                        js_wait_ms = int(os.getenv("SCRAPINGBEE_JS_WAIT_MS", "2000"))
                        country_code = os.getenv("SCRAPINGBEE_COUNTRY_CODE", "us")

                        js_scenario = {
                            "instructions": [
                                {"wait": js_wait_ms},
                            ]
                        }

                        params: Dict[str, object] = {
                            "js_scenario": js_scenario,
                            "stealth_proxy": "True",
                            "country_code": country_code,
                        }
                        if render_js:
                            params["render_js"] = "true"

                        try:
                            response = client.get(page_url, params=params)
                        except Exception as e2:
                            print(
                                f"[secretflying] _get_deals_requests: ScrapingBee error for {page_url}: {e2!r}"
                            )
                            break

                        if response.status_code != 200:
                            print(
                                f"[secretflying] _get_deals_requests: ScrapingBee HTTP {response.status_code} "
                                f"for {page_url}"
                            )
                            try:
                                snippet = response.text[:400]
                                print(
                                    f"[secretflying] _get_deals_requests: body snippet via ScrapingBee: {snippet!r}"
                                )
                            except Exception:
                                pass
                            break
                    except Exception as e2:
                        print(
                            f"[secretflying] _get_deals_requests: unexpected ScrapingBee setup error for {page_url}: {e2!r}"
                        )
                        break
                else:
                    print(
                        f"[secretflying] _get_deals_requests: HTTP 403 for {page_url} and "
                        "no SCRAPINGBEE_API_KEY available; giving up"
                    )
                    break

            if response.status_code != 200:
                print(f"[secretflying] _get_deals_requests: HTTP {response.status_code} for {page_url}")
                try:
                    snippet = response.text[:400]
                    print(f"[secretflying] _get_deals_requests: body snippet: {snippet!r}")
                except Exception:
                    pass
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            _parse_articles_from_soup(soup, base_url, deals, limit)

            page += 1

    except Exception as e:
        print(f"[secretflying] _get_deals_requests: unexpected error: {e!r}")

    return deals


def _get_deals_scrapingbee(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via ScrapingBee API.

    Requires SCRAPINGBEE_API_KEY in the environment.

    Nota: mantenido por compatibilidad, pero la vía recomendada ahora
    es usar SCRAPERAPI_KEY y _get_deals_scraperapi.
    """

    api_key = os.getenv("SCRAPINGBEE_API_KEY")
    if not api_key or ScrapingBeeClient is None:
        raise RuntimeError("SCRAPINGBEE_API_KEY or ScrapingBeeClient not available")

    base_url = "https://www.secretflying.com"

    render_js_env = os.getenv("SCRAPINGBEE_RENDER_JS")
    if render_js_env is None:
        render_js = True
    else:
        render_js = render_js_env.strip().lower() in {"1", "true", "yes", "on"}

    js_wait_ms = int(os.getenv("SCRAPINGBEE_JS_WAIT_MS", "2000"))
    country_code = os.getenv("SCRAPINGBEE_COUNTRY_CODE", "us")

    client = ScrapingBeeClient(api_key=api_key)

    deals: List[Dict] = []
    page = 1
    max_pages = 5

    while page <= max_pages and len(deals) < limit:
        if page == 1:
            page_url = f"{base_url}/"
        else:
            page_url = f"{base_url}/page/{page}/"

        js_scenario = {
            "instructions": [
                {"wait": js_wait_ms},
            ]
        }

        params: Dict[str, object] = {
            "js_scenario": js_scenario,
            "stealth_proxy": "True",
            "country_code": country_code,
        }
        if render_js:
            params["render_js"] = "true"

        try:
            resp = client.get(page_url, params=params)
        except Exception as e:
            print(f"[secretflying] _get_deals_scrapingbee: request failed for {page_url}: {e!r}")
            break

        if resp.status_code != 200:
            print(f"[secretflying] _get_deals_scrapingbee: HTTP {resp.status_code} for {page_url}")
            try:
                snippet = resp.text[:400]
                print(f"[secretflying] _get_deals_scrapingbee: body snippet: {snippet!r}")
            except Exception:
                pass
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        _parse_articles_from_soup(soup, base_url, deals, limit)

        page += 1

    return deals


def _get_deals_apify(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via Apify Proxy.

    Uses APIFY_API_KEY from the environment and routes traffic through
    proxy.apify.com so that SecretFlying sees a proxied request.
    """

    api_key = os.getenv("APIFY_API_KEY")
    if not api_key:
        raise RuntimeError("APIFY_API_KEY not available in environment")

    proxy_url = os.getenv("APIFY_PROXY_URL") or f"http://auto:{api_key}@proxy.apify.com:8000"

    proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }

    print("[secretflying] _get_deals_apify: using Apify proxy")
    return _get_deals_requests(limit=limit, session_proxies=proxies)


def _get_deals_scrapingant(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via ScrapingAnt.

    Looks for SCRAPINGANT_API_KEY (or lowercase scrapingant_api_key) in the environment.
    """

    api_key = os.getenv("SCRAPINGANT_API_KEY") or os.getenv("scrapingant_api_key")
    if not api_key:
        raise RuntimeError("SCRAPINGANT_API_KEY not available in environment")

    base_url = "https://www.secretflying.com"
    scrapingant_endpoint = os.getenv("SCRAPINGANT_ENDPOINT", "https://api.scrapingant.com/v2/general")

    deals: List[Dict] = []
    page = 1
    max_pages = 5

    while page <= max_pages and len(deals) < limit:
        if page == 1:
            page_url = f"{base_url}/"
        else:
            page_url = f"{base_url}/page/{page}/"

        params = {
            "url": page_url,
            # Activamos navegador completo; si quieres sólo HTTP plano,
            # cambia este flag a "false".
            "browser": "true",
        }

        try:
            resp = requests.get(
                scrapingant_endpoint,
                params=params,
                headers={"x-api-key": api_key},
                timeout=30,
            )
        except Exception as e:
            print(f"[secretflying] _get_deals_scrapingant: request failed for {page_url}: {e!r}")
            break

        if resp.status_code != 200:
            print(f"[secretflying] _get_deals_scrapingant: HTTP {resp.status_code} for {page_url}")
            try:
                snippet = resp.text[:400]
                print(f"[secretflying] _get_deals_scrapingant: body snippet: {snippet!r}")
            except Exception:
                pass
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        _parse_articles_from_soup(soup, base_url, deals, limit)

        page += 1

    return deals


def _get_deals_scraperapi(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via ScraperAPI.

    Requires SCRAPERAPI_KEY in the environment.
    """

    api_key = os.getenv("SCRAPERAPI_KEY")
    if not api_key:
        raise RuntimeError("SCRAPERAPI_KEY not available in environment")

    base_url = "https://www.secretflying.com"
    scraperapi_endpoint = os.getenv("SCRAPERAPI_ENDPOINT", "https://api.scraperapi.com")

    country_code = os.getenv("SCRAPERAPI_COUNTRY_CODE", "us")

    deals: List[Dict] = []
    page = 1
    max_pages = 5

    while page <= max_pages and len(deals) < limit:
        if page == 1:
            page_url = f"{base_url}/"
        else:
            page_url = f"{base_url}/page/{page}/"

        params = {
            "api_key": api_key,
            "url": page_url,
            "render": "true",
            "country_code": country_code,
        }

        try:
            resp = requests.get(scraperapi_endpoint, params=params, timeout=20)
        except Exception as e:
            print(f"[secretflying] _get_deals_scraperapi: request failed for {page_url}: {e!r}")
            break

        if resp.status_code != 200:
            print(f"[secretflying] _get_deals_scraperapi: HTTP {resp.status_code} for {page_url}")
            try:
                snippet = resp.text[:400]
                print(f"[secretflying] _get_deals_scraperapi: body snippet: {snippet!r}")
            except Exception:
                pass
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        _parse_articles_from_soup(soup, base_url, deals, limit)

        page += 1

    return deals


def get_deals_playwright(limit: int = 100, headless: bool = True) -> List[Dict]:
    """Use Playwright to fetch pages (better for sites with anti-bot JS checks).

    Note: after installing `playwright` package, run `playwright install` once to install browser binaries.
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        print(f"[secretflying] get_deals_playwright: failed to import Playwright: {e!r}")
        raise

    deals: List[Dict] = []
    base_url = "https://www.secretflying.com"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            page = context.new_page()

            page_num = 1
            max_pages = 20

            while page_num <= max_pages and len(deals) < limit:
                if page_num == 1:
                    page_url = f"{base_url}/"
                else:
                    page_url = f"{base_url}/page/{page_num}/"

                try:
                    page.goto(page_url, wait_until='networkidle', timeout=20000)
                except Exception as e:
                    print(f"[secretflying] get_deals_playwright: error navigating to {page_url}: {e!r}")
                    break

                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                _parse_articles_from_soup(soup, base_url, deals, limit)

                page_num += 1

            try:
                context.close()
                browser.close()
            except Exception:
                pass

    except Exception as e:
        # Let the caller handle fallback, but log the reason
        print(f"[secretflying] get_deals_playwright: unexpected error: {e!r}")
        raise

    return deals


def get_deals(limit: int = 100) -> List[Dict]:
    """Get SecretFlying deals using only ScrapingBee.

    - Requires SCRAPINGBEE_API_KEY.
    - Falls back to plain requests (which still retries with ScrapingBee on 403)
      if ScrapingBee fails, so we at least attempt a lightweight pull.
    """

    api_key = os.getenv("SCRAPINGBEE_API_KEY")
    if not api_key or ScrapingBeeClient is None:
        raise RuntimeError("SCRAPINGBEE_API_KEY not available or ScrapingBeeClient not installed")

    print(
        "[secretflying] get_deals start",
        f"limit={limit}",
        f"has_api_key={bool(api_key)}",
        f"ScrapingBeeClient_loaded={ScrapingBeeClient is not None}",
    )

    try:
        deals = _get_deals_scrapingbee(limit=limit)
        if deals:
            print(f"[secretflying] get_deals primary_scrapingbee_count={len(deals)}")
            return deals
    except Exception as e:
        print(f"[secretflying] get_deals: _get_deals_scrapingbee failed: {e!r}")

    # Fallback: plain requests with ScrapingBee retry on 403 inside _get_deals_requests
    return _get_deals_requests(limit=limit)

