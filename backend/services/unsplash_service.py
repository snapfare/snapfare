"""Unsplash destination image fetcher.

Fetches a landscape photo URL for a destination city using the Unsplash Search API.
Returns the CDN URL directly — no download required.

Rate limit: 50 requests/hour on the free tier (sliding-window enforced).
Requires UNSPLASH_ACCESS_KEY in environment.

Image URLs are cached in backend/data/unsplash_cache.json so that each destination
is fetched only once across all pipeline runs.
"""

import json
import os
import time
import logging
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

logger = logging.getLogger("snapcore.services.unsplash")

_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY")
_API_BASE = "https://api.unsplash.com"

# Persistent image cache — keyed by normalised city name (lowercase stripped)
_CACHE_FILE = Path(__file__).parent.parent / "data" / "unsplash_cache.json"
_image_cache: dict[str, str] = {}


def _load_cache() -> None:
    """Load the on-disk cache into the in-memory dict (called once at module import)."""
    if _CACHE_FILE.exists():
        try:
            _image_cache.update(json.loads(_CACHE_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass


def _save_cache() -> None:
    """Persist the in-memory cache to disk."""
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(
            json.dumps(_image_cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("Failed to write Unsplash cache: %r", exc)


# Load cache at import time
_load_cache()

# Sliding-window rate limiter: track timestamps of recent API calls.
_call_timestamps: list[float] = []
_RATE_LIMIT = 48  # leave a 2-request buffer below the 50/hour free tier


def _check_rate_limit() -> None:
    """Block until the rate limit allows another request."""
    now = time.time()
    window_start = now - 3600.0

    # Purge calls older than 1 hour
    while _call_timestamps and _call_timestamps[0] < window_start:
        _call_timestamps.pop(0)

    if len(_call_timestamps) >= _RATE_LIMIT:
        # Sleep until the oldest call falls outside the 1-hour window
        sleep_for = _call_timestamps[0] - window_start + 0.5
        logger.info(
            "Unsplash rate limit reached (%d calls/hour). Sleeping %.0fs.",
            _RATE_LIMIT,
            sleep_for,
        )
        time.sleep(max(sleep_for, 0))
        _check_rate_limit()  # re-check after sleep


def fetch_destination_image(city_name: str) -> Optional[str]:
    """Return a landscape photo CDN URL for *city_name* from Unsplash.

    Results are cached in backend/data/unsplash_cache.json to avoid
    re-fetching the same destination on subsequent pipeline runs.

    Returns None when:
    - UNSPLASH_ACCESS_KEY is not configured
    - The API returns no results for the query
    - Any network or API error occurs
    """

    if not city_name or not city_name.strip():
        return None

    cache_key = city_name.strip().lower()

    # Return cached result without hitting the API
    if cache_key in _image_cache:
        logger.debug("Unsplash cache hit for '%s'", city_name)
        return _image_cache[cache_key]

    if not _ACCESS_KEY:
        logger.debug("UNSPLASH_ACCESS_KEY not configured — skipping image fetch")
        return None

    _check_rate_limit()

    query = city_name.strip()
    try:
        resp = requests.get(
            f"{_API_BASE}/search/photos",
            params={
                "query": query,
                "orientation": "landscape",
                "per_page": 1,
                "content_filter": "high",
            },
            headers={"Authorization": f"Client-ID {_ACCESS_KEY}"},
            timeout=10,
        )
        _call_timestamps.append(time.time())

        if resp.status_code == 429:
            logger.warning("Unsplash rate limit exceeded (HTTP 429) for '%s'", query)
            return None

        if resp.status_code != 200:
            logger.warning(
                "Unsplash API error for '%s': HTTP %s", query, resp.status_code
            )
            return None

        data = resp.json()
        results = data.get("results") or []
        if not results:
            logger.debug("Unsplash: no results for '%s'", query)
            return None

        url = (results[0].get("urls") or {}).get("regular")
        if url:
            logger.debug("Unsplash: fetched image for '%s': %s", query, url)
            _image_cache[cache_key] = url
            _save_cache()
        return url or None

    except Exception as exc:
        logger.warning("Unsplash fetch failed for '%s': %r", query, exc)
        return None
