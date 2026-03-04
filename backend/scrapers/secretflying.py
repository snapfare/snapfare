from typing import List, Dict, Optional, Callable
import logging
import os
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv, find_dotenv

# Ensure .env is loaded even if caller forgot to do it.
load_dotenv(find_dotenv(), override=False)

try:
    # Official ScrapingBee SDK (optional)
    from scrapingbee import ScrapingBeeClient  # type: ignore
except Exception:  # pragma: no cover
    ScrapingBeeClient = None  # type: ignore

logger = logging.getLogger("snapcore.scrapers.secretflying")

BASE_URL = "https://www.secretflying.com"


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
            try:
                return float(match.group(1).replace(',', '.')), currency
            except ValueError:
                continue
    return None, 'USD'


def _extract_route_from_title(title: str) -> tuple[str | None, str | None]:
    """Heuristic origin/destination extraction for SecretFlying titles."""
    if not title:
        return None, None

    raw = title.replace("\u2192", "→")
    for sep in ("→", "↔", " – ", " - ", " –", "-", " to "):
        if sep in raw:
            left, right = raw.split(sep, 1)
            left, right = left.strip(), right.strip()
            # Strip trailing price/offer fragments from the destination
            right = re.sub(r"\bfrom only\b.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfrom\b\s+[€$]?\d.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfor only\b.*$", "", right, flags=re.IGNORECASE).strip()
            right = re.sub(r"\bfor\b\s+[€$]?\d.*$", "", right, flags=re.IGNORECASE).strip()
            if len(left) >= 3 and len(right) >= 3:
                return left, right
    return None, None


def _extract_title_from_article(article) -> str:
    """Extract title text from an article element using progressive fallbacks."""
    for tag in ('h1', 'h2', 'h3'):
        h = article.find(tag)
        if h and h.get_text(strip=True):
            return h.get_text(strip=True)

    for tag in ('h1', 'h2', 'h3'):
        a = article.find(tag)
        if a:
            a_link = a.find('a', href=True)
            if a_link and a_link.get_text(strip=True):
                return a_link.get_text(strip=True)

    a_book = article.find('a', attrs={'rel': 'bookmark'})
    if a_book and a_book.get_text(strip=True):
        return a_book.get_text(strip=True)

    for a in article.find_all('a', href=True):
        txt = a.get_text(strip=True)
        if txt and '/deal/' in a['href']:
            return txt

    for a in article.find_all('a', href=True):
        txt = a.get_text(strip=True)
        if txt and len(txt) > 3:
            return txt

    return article.get_text(strip=True)[:200]


def _parse_articles_from_soup(soup: BeautifulSoup, base_url: str, deals: List[Dict], limit: int) -> List[Dict]:
    articles = soup.find_all(['article', 'div'], class_=re.compile(r'(post|deal|flight|entry)', re.I))
    if not articles:
        return deals

    for article in articles:
        title = _extract_title_from_article(article)

        # Separate "link" (SecretFlying post URL) from "booking_url" (first external link).
        link = None
        booking_url = None
        for a in article.find_all('a', href=True):
            href = (a.get('href') or '').strip()
            if not href:
                continue
            absolute = base_url.rstrip('/') + href if href.startswith('/') else href
            if 'secretflying.com' in absolute.lower():
                if '/posts/' in absolute.lower():
                    if link is None:
                        link = absolute
                elif link is None:
                    link = absolute
                continue
            if booking_url is None:
                booking_url = absolute

        # Skip structural blocks like the search form ("FromToWhenShow past deals")
        normalized_title = (title or "").replace(" ", "").lower()
        if not link or link == base_url:
            continue
        if "fromtowhenshowpastdeals" in normalized_title:
            continue

        # Replace a bare date title (e.g. "December 30, 2025") with the URL slug.
        if title and re.fullmatch(r"[A-Za-z]+ \d{1,2}, \d{4}", title.strip()):
            try:
                slug = urlparse(link).path.rstrip('/').split('/')[-1]
                if slug:
                    pretty = slug.replace('-', ' ').strip()
                    if pretty:
                        title = pretty[0].upper() + pretty[1:]
            except Exception:
                pass

        price, currency = _extract_price(article.get_text())
        origin, destination = _extract_route_from_title(title)

        image_url = None
        img = article.find('img')
        if img:
            src = img.get('data-src') or img.get('data-lazy-src') or img.get('src')
            if src:
                src = src.strip()
                if src.startswith('//'): src = 'https:' + src
                if src.startswith('/'): src = urljoin(base_url, src)
                image_url = src

        deals.append({
            "title": title[:200] if title else "",
            "price": price,
            "currency": currency,
            "link": link,
            "booking_url": booking_url,
            "origin": origin,
            "destination": destination,
            "image_url": image_url,
            "source": "SecretFlying",
        })

        if len(deals) >= limit:
            return deals

    return deals


def _paginate_html(
    fetch_fn: Callable[[str], Optional[str]],
    base_url: str,
    limit: int,
    max_pages: int = 5,
    label: str = "secretflying",
) -> List[Dict]:
    """Generic pagination helper. fetch_fn(page_url) should return HTML or None."""
    deals: List[Dict] = []
    for page_num in range(1, max_pages + 1):
        if len(deals) >= limit:
            break
        page_url = f"{base_url}/" if page_num == 1 else f"{base_url}/page/{page_num}/"
        try:
            html = fetch_fn(page_url)
        except Exception as e:
            logger.warning("[secretflying] %s: error for %s: %r", label, page_url, e)
            break
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")
        _parse_articles_from_soup(soup, base_url, deals, limit)

    return deals


def _get_deals_requests(limit: int = 100, session_proxies: Optional[Dict[str, str]] = None) -> List[Dict]:
    """Fetch listing pages with plain requests, optionally using HTTP proxies.

    If ``session_proxies`` is provided, it is passed to ``requests.Session`` as
    the ``proxies`` argument (used for Apify Proxy, etc.).
    """
    deals: List[Dict] = []
    try:
        page = 1
        max_pages = 20

        while page <= max_pages and len(deals) < limit:
            page_url = f"{BASE_URL}/" if page == 1 else f"{BASE_URL}/page/{page}/"

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
                logger.warning("[secretflying] _get_deals_requests: error fetching %s: %r", page_url, e)
                break

            # On 403 (anti-bot), retry once via ScrapingBee if a key is available.
            if response.status_code == 403 and not session_proxies:
                api_key = os.getenv("SCRAPINGBEE_API_KEY")
                if api_key and ScrapingBeeClient is not None:
                    logger.info("[secretflying] HTTP 403 for %s, retrying via ScrapingBee", page_url)
                    try:
                        client = ScrapingBeeClient(api_key=api_key)
                        render_js = os.getenv("SCRAPINGBEE_RENDER_JS", "true").strip().lower() in {"1", "true", "yes", "on"}
                        js_wait_ms = int(os.getenv("SCRAPINGBEE_JS_WAIT_MS", "2000"))
                        params: Dict = {
                            "js_scenario": {"instructions": [{"wait": js_wait_ms}]},
                            "stealth_proxy": "True",
                            "country_code": os.getenv("SCRAPINGBEE_COUNTRY_CODE", "us"),
                        }
                        if render_js:
                            params["render_js"] = "true"
                        response = client.get(page_url, params=params)
                        if response.status_code != 200:
                            logger.warning("[secretflying] ScrapingBee HTTP %s for %s", response.status_code, page_url)
                            break
                    except Exception as e2:
                        logger.warning("[secretflying] ScrapingBee error for %s: %r", page_url, e2)
                        break
                else:
                    logger.warning("[secretflying] HTTP 403 for %s, no SCRAPINGBEE_API_KEY", page_url)
                    break

            if response.status_code != 200:
                logger.warning("[secretflying] _get_deals_requests: HTTP %s for %s", response.status_code, page_url)
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            _parse_articles_from_soup(soup, BASE_URL, deals, limit)
            page += 1

    except Exception as e:
        logger.warning("[secretflying] _get_deals_requests: unexpected error: %r", e)

    return deals


def _get_deals_scrapingbee(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via ScrapingBee API."""
    api_key = os.getenv("SCRAPINGBEE_API_KEY")
    if not api_key or ScrapingBeeClient is None:
        raise RuntimeError("SCRAPINGBEE_API_KEY or ScrapingBeeClient not available")

    render_js = os.getenv("SCRAPINGBEE_RENDER_JS", "true").strip().lower() in {"1", "true", "yes", "on"}
    js_wait_ms = int(os.getenv("SCRAPINGBEE_JS_WAIT_MS", "2000"))
    client = ScrapingBeeClient(api_key=api_key)
    params: Dict = {
        "js_scenario": {"instructions": [{"wait": js_wait_ms}]},
        "stealth_proxy": "True",
        "country_code": os.getenv("SCRAPINGBEE_COUNTRY_CODE", "us"),
    }
    if render_js:
        params["render_js"] = "true"

    def fetch(page_url: str) -> Optional[str]:
        resp = client.get(page_url, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"ScrapingBee HTTP {resp.status_code}")
        return resp.text

    return _paginate_html(fetch, BASE_URL, limit, max_pages=5, label="_get_deals_scrapingbee")


def _get_deals_apify(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via Apify Proxy."""
    api_key = os.getenv("APIFY_API_KEY")
    if not api_key:
        raise RuntimeError("APIFY_API_KEY not available in environment")

    proxy_url = os.getenv("APIFY_PROXY_URL") or f"http://auto:{api_key}@proxy.apify.com:8000"
    logger.info("[secretflying] _get_deals_apify: using Apify proxy")
    return _get_deals_requests(limit=limit, session_proxies={"http": proxy_url, "https": proxy_url})


def _get_deals_scrapingant(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via ScrapingAnt."""
    api_key = os.getenv("SCRAPINGANT_API_KEY") or os.getenv("scrapingant_api_key")
    if not api_key:
        raise RuntimeError("SCRAPINGANT_API_KEY not available in environment")

    endpoint = os.getenv("SCRAPINGANT_ENDPOINT", "https://api.scrapingant.com/v2/general")

    def fetch(page_url: str) -> Optional[str]:
        resp = requests.get(endpoint, params={"url": page_url, "browser": "true"},
                            headers={"x-api-key": api_key}, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"ScrapingAnt HTTP {resp.status_code}")
        return resp.text

    return _paginate_html(fetch, BASE_URL, limit, max_pages=5, label="_get_deals_scrapingant")


def _get_deals_scraperapi(limit: int = 100) -> List[Dict]:
    """Fetch SecretFlying listing pages via ScraperAPI."""
    api_key = os.getenv("SCRAPERAPI_KEY")
    if not api_key:
        raise RuntimeError("SCRAPERAPI_KEY not available in environment")

    endpoint = os.getenv("SCRAPERAPI_ENDPOINT", "https://api.scraperapi.com")

    def fetch(page_url: str) -> Optional[str]:
        resp = requests.get(endpoint, params={
            "api_key": api_key, "url": page_url, "render": "true",
            "country_code": os.getenv("SCRAPERAPI_COUNTRY_CODE", "us"),
        }, timeout=20)
        if resp.status_code != 200:
            raise RuntimeError(f"ScraperAPI HTTP {resp.status_code}")
        return resp.text

    return _paginate_html(fetch, BASE_URL, limit, max_pages=5, label="_get_deals_scraperapi")


def get_deals_playwright(limit: int = 100, headless: bool = True) -> List[Dict]:
    """Use Playwright to fetch pages (better for sites with anti-bot JS checks).

    Note: run `playwright install` once to install browser binaries.
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        logger.error("[secretflying] get_deals_playwright: failed to import Playwright: %r", e)
        raise

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        pw_page = context.new_page()

        def fetch(page_url: str) -> Optional[str]:
            pw_page.goto(page_url, wait_until='networkidle', timeout=20000)
            return pw_page.content()

        try:
            deals = _paginate_html(fetch, BASE_URL, limit, max_pages=20, label="playwright")
        finally:
            try:
                context.close()
                browser.close()
            except Exception:
                pass

    return deals


def get_deals(limit: int = 100) -> List[Dict]:
    """Get SecretFlying deals. Tries ScrapingBee first, falls back to plain requests."""
    api_key = os.getenv("SCRAPINGBEE_API_KEY")
    if not api_key or ScrapingBeeClient is None:
        raise RuntimeError("SCRAPINGBEE_API_KEY not available or ScrapingBeeClient not installed")

    logger.info("[secretflying] get_deals start limit=%d", limit)

    try:
        deals = _get_deals_scrapingbee(limit=limit)
        if deals:
            logger.info("[secretflying] get_deals scrapingbee_count=%d", len(deals))
            return deals
    except Exception as e:
        logger.warning("[secretflying] _get_deals_scrapingbee failed: %r", e)

    # Fallback: plain requests (with ScrapingBee retry on 403 built in)
    return _get_deals_requests(limit=limit)
