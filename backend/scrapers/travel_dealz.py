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
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
            continue

    if last_err is not None:
        logger.warning("[travel-dealz] request failed url=%s error=%r", url, last_err)
    return None


def _is_non_flight_deal(title: str, link: str) -> bool:
    """Simple heuristic to discard posts that are not flights.

    Examples: AirHelp Plus, insurance, legal services, gift cards.
    """
    t = (title or "").lower()
    l = (link or "").lower()

    if "airhelp" in t or "airhelp" in l:
        return True

    for kw in ("gift card", "gift-card", "gift cards", "gift-cards", "voucher"):
        if kw in t or kw in l:
            return True

    if "ita-airways-gift-cards" in l:
        return True

    return False


def _extract_price(text: str) -> tuple[float | None, str]:
    """Extract price and currency from text.

    Travel-Dealz often writes prices like "1,071" or "2.613" where
    commas/dots are thousands separators, not decimals. We strip all
    non-digit characters and treat the result as an integer.
    """
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
        digits = re.sub(r"[^0-9]", "", match.group(1))
        if not digits:
            continue
        try:
            return float(digits), currency
        except ValueError:
            continue

    return None, "EUR"


def _extract_route_from_title(title: str) -> tuple[str | None, str | None]:
    """Best-effort extraction of origin/destination from the deal title.

    Looks for separators like "Zürich → Kapstadt", "MAD – NYC", etc.
    Returns (origin, destination) or (None, None) if no pattern is found.

    Plain '-' is avoided as separator because it appears inside words
    ("Non-Stop", "Round-trip", ...) and generates false positives.
    """
    if not title:
        return None, None

    raw = title.replace("\u2192", "→")

    separators = ["→", "↔", " – ", " - ", " to "]
    for sep in separators:
        if sep in raw:
            left, right = raw.split(sep, 1)
            left, right = left.strip(), right.strip()
            if any(ch.isdigit() for ch in left + right):
                continue
            if any(sym in (left + right) for sym in ["€", "$", "£", "CHF", "USD", "EUR"]):
                continue
            if len(left) >= 3 and len(right) >= 3:
                return left, right

    # Pattern: "Perth, Australia: €2,786 Star Alliance Business Class from Budapest"
    # Interpret everything before ':' as DESTINATION, "from X" fragment as ORIGIN.
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
    img = article.find("img")
    if not img:
        return None
    src = img.get("data-src") or img.get("data-lazy-src") or img.get("src")
    if not src:
        return None
    src = src.strip()
    if src.startswith("//"):
        src = "https:" + src
    if src.startswith("/"):
        src = urljoin(base_url, src)
    return src


def _extract_title_from_article(article) -> str:
    """Extract title text from an article/card element using progressive fallbacks."""
    # 1) Heading elements with text
    for tag in ("h1", "h2", "h3"):
        h = article.find(tag)
        if h and h.get_text(strip=True):
            return h.get_text(strip=True)

    # 2) Link inside heading
    for tag in ("h1", "h2", "h3"):
        a = article.find(tag)
        if a:
            a_link = a.find("a", href=True)
            if a_link and a_link.get_text(strip=True):
                return a_link.get_text(strip=True)

    # 3) Bookmark link or /deal/ link
    a_book = article.find("a", attrs={"rel": "bookmark"})
    if a_book and a_book.get_text(strip=True):
        return a_book.get_text(strip=True)
    for a in article.find_all("a", href=True):
        txt = a.get_text(strip=True)
        if txt and "/deal/" in a["href"]:
            return txt

    # 4) Any link with reasonable text
    for a in article.find_all("a", href=True):
        txt = a.get_text(strip=True)
        if txt and len(txt) > 3:
            return txt

    # 5) Fallback: block text
    return article.get_text(strip=True)[:200]


def _slug_title(link: str) -> str | None:
    """Derive a readable title from a deal URL slug."""
    try:
        slug = urlparse(link).path.rstrip("/").split("/")[-1]
        if slug:
            return unquote(slug).replace("-", " ").strip().title()[:200]
    except Exception:
        pass
    return None


def _scrape_category(base_url: str, category_url: str, limit: int) -> List[Dict]:
    """Paginate a Travel-Dealz category listing and return raw deal dicts."""
    deals: List[Dict] = []
    log_prefix = "[travel-dealz.de]" if ".de" in base_url else "[travel-dealz]"

    try:
        pages_needed = max(1, (int(limit) + 19) // 20)
    except Exception:
        pages_needed = 5
    max_pages = max(5, min(50, pages_needed + 1))

    page = 1
    while page <= max_pages and len(deals) < limit:
        page_url = f"{category_url}/" if page == 1 else f"{category_url}/page/{page}/"

        html = _fetch_html(page_url, timeout=10, retries=2)
        if not html:
            logger.warning("%s request failed url=%s", log_prefix, page_url)
            break

        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all(["article", "div"], class_=re.compile(r"(post|deal|offer|card)", re.I))
        if not articles:
            logger.warning("%s no articles found url=%s", log_prefix, page_url)
            break

        for article in articles:
            title = _extract_title_from_article(article)

            link_elem = article.find("a", href=True)
            link = link_elem["href"] if link_elem else base_url
            if link.startswith("/"):
                link = base_url.rstrip("/") + link

            if "/deal/" not in link:
                continue
            if _is_non_flight_deal(title, link):
                continue

            if not title and link:
                title = _slug_title(link) or ""

            # Correct date-as-title (e.g. "December 30, 2025") using URL slug.
            if title and re.fullmatch(r"[A-Za-z]+ \d{1,2}, \d{4}", title.strip()):
                slug_t = _slug_title(link)
                if slug_t:
                    title = slug_t

            price, currency = _extract_price(article.get_text())
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
                "source": "Travel-Dealz.de" if ".de" in base_url else "Travel-Dealz",
            })

            if len(deals) >= limit:
                return deals

        page += 1

    return deals


def get_deals(limit: int = 100) -> List[Dict]:
    """Scrape travel deals from travel-dealz.com/category/flights/."""
    try:
        return _scrape_category("https://travel-dealz.com", "https://travel-dealz.com/category/flights", limit)
    except Exception as e:
        logger.warning("[travel-dealz] unexpected error=%r", e)
        return []


def get_deals_de(limit: int = 100) -> List[Dict]:
    """Scrape travel deals from travel-dealz.de/kategorie/fluge/."""
    try:
        return _scrape_category("https://travel-dealz.de", "https://travel-dealz.de/kategorie/fluge", limit)
    except Exception as e:
        logger.warning("[travel-dealz.de] unexpected error=%r", e)
        return []
