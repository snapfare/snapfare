import os
import json
import time
import random
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


try:  # Optional dependency (v1 client)
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover - runtime guard
    OpenAI = None  # type: ignore

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OpenAI and OPENAI_API_KEY else None


def _truthy_env(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _is_missing(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, str) and val.strip() in {"", "—", "-"}:
        return True
    return False


def _deterministic_program_miles(distance_miles: int, program: str) -> Optional[int]:
    """Very conservative deterministic estimate for miles-credit.

    We intentionally keep this simple (and bias low) to avoid implying exact
    program charts.

    Only supports the programs requested by the user.
    """

    if not distance_miles or distance_miles <= 0:
        return None

    p = (program or "").strip().lower()
    base = max(int(round(distance_miles)), 500)

    # Economy Light: credit often reduced vs full-fare; keep conservative.
    if "miles&more" in p or "miles & more" in p:
        factor = 1.0
    elif "flying blue" in p or "flyingblue" in p:
        factor = 0.75
    else:
        return None

    est = int(round(base * factor / 100.0) * 100)
    return max(est, 500)


def _chunked(items: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


# ------------------------------
# OpenAI throttling / retries
# ------------------------------

_LAST_OPENAI_CALL_TS: float | None = None


def _openai_min_seconds_between_calls() -> float:
    """Minimum delay between OpenAI requests.

    This is a pragmatic safeguard against rate limiting when processing
    multiple deals quickly.
    """
    raw = os.getenv("OPENAI_MIN_SECONDS_BETWEEN_CALLS", "")
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except Exception:
        return 0.0


def _openai_max_retries() -> int:
    raw = os.getenv("OPENAI_MAX_RETRIES", "2")
    try:
        return max(0, int(raw))
    except Exception:
        return 2


def _sleep_if_needed_for_throttle() -> None:
    global _LAST_OPENAI_CALL_TS

    min_delay = _openai_min_seconds_between_calls()
    if min_delay <= 0:
        return
    now = time.time()
    if _LAST_OPENAI_CALL_TS is None:
        return
    elapsed = now - _LAST_OPENAI_CALL_TS
    remaining = min_delay - elapsed
    if remaining > 0:
        time.sleep(remaining)


def _mark_openai_call() -> None:
    global _LAST_OPENAI_CALL_TS
    _LAST_OPENAI_CALL_TS = time.time()


def _looks_like_rate_limit_error(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    return (
        "rate limit" in msg
        or "429" in msg
        or "too many requests" in msg
        or "request too large" in msg
        or "temporarily unavailable" in msg
    )


def _looks_like_insufficient_quota(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    return "insufficient_quota" in msg or "quota" in msg and "insufficient" in msg


def _openai_batch_size() -> int:
    raw = os.getenv("DEALS_LLM_BATCH_SIZE", "5")
    try:
        return max(1, min(10, int(raw)))
    except Exception:
        return 5


def _strip_json_wrappers(content: str) -> str:
    content = (content or "").strip()
    if content.startswith("```"):
        content = content.strip("`\n ")
        if content.lower().startswith("json"):
            content = content[4:].lstrip("\n")
    return content


def _call_openai_fill_missing_batch(items: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Single OpenAI call to fill/validate missing fields for multiple deals.

    Returns mapping: item_id -> fields dict.
    On any error returns {}.
    """
    if not openai_client or not items:
        return {}

    allucinate = _truthy_env("DEALS_LLM_ALLUCINATE")

    system_prompt = (
        "You extract/validate flight-deal fields from provided inputs. "
        "The 'article_text' may be real article text OR a synthetic context built from structured fields. "
        "Return STRICT JSON only (no markdown). "
        + (
            "When information is not explicitly stated, you MAY provide best-effort estimates "
            "as long as they are consistent with the provided context. "
            "Never output obviously fabricated specifics (e.g., exact dates) if not grounded. "
            "If you truly cannot infer a value, return null."
            if allucinate
            else "Do not invent facts. If a field cannot be derived, return null."
        )
    )

    user_payload = {
        "items": items,
        "output_schema": {
            "items": [
                {
                    "id": "int",
                    "fields": "object (keys are a subset of requested_fields; values may be null)",
                }
            ]
        },
        "global_rules": [
            "Only fill keys listed in requested_fields for each item.",

            (
                "If 'article_text' is synthetic/brief, you may still infer plausible values from existing structured fields."
                if allucinate
                else "If 'article_text' is synthetic/brief, be conservative and prefer null over guessing."
            ),

            "Return JSON with top-level key 'items' as a list.",
            "Do not invent facts. If a field cannot be derived, return null.",
        ],
    }

    try:
        content = _openai_chat_completion(
            model=os.getenv("DEALS_ENRICH_OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": "Fill missing fields and return JSON.\n\nInput JSON:\n" + json.dumps(user_payload, ensure_ascii=False),
                },
            ],
            temperature=0.0,
        )
    except Exception:
        return {}

    if not content:
        return {}

    raw = _strip_json_wrappers(content)
    try:
        data = json.loads(raw)
    except Exception as e:
        snippet = raw[:300] + ("…" if len(raw) > 300 else "")
        print(f"[llm] OpenAI returned non-JSON output; skipping. error={type(e).__name__} snippet={snippet!r}")
        return {}

    if isinstance(data, dict) and isinstance(data.get("items"), list):
        out: Dict[int, Dict[str, Any]] = {}
        for it in data["items"]:
            if not isinstance(it, dict):
                continue
            item_id = it.get("id")
            fields = it.get("fields")
            if isinstance(item_id, int) and isinstance(fields, dict):
                out[item_id] = fields
        return out

    if isinstance(data, list):
        out2: Dict[int, Dict[str, Any]] = {}
        for it in data:
            if not isinstance(it, dict):
                continue
            item_id = it.get("id")
            fields = it.get("fields")
            if isinstance(item_id, int) and isinstance(fields, dict):
                out2[item_id] = fields
        return out2

    # Unexpected schema
    snippet = raw[:300] + ("…" if len(raw) > 300 else "")
    print(f"[llm] OpenAI response schema mismatch; skipping. snippet={snippet!r}")
    return {}


def _openai_chat_completion(
    *,
    messages: List[Dict[str, str]],

    temperature: float,
    model: str,
) -> Optional[str]:
    """Throttled + retried OpenAI chat completion.

    Returns the raw assistant content (string) or None on failure.
    """
    if not openai_client:
        return None

    max_retries = _openai_max_retries()
    base_backoff = float(os.getenv("OPENAI_RETRY_BACKOFF_SECONDS", "2") or 2)

    for attempt in range(max_retries + 1):
        try:
            _sleep_if_needed_for_throttle()
            completion = openai_client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
            )
            _mark_openai_call()
            content = completion.choices[0].message.content  # type: ignore[index]
            return content
        except Exception as e:
            # Avoid leaking secrets; keep messages short.
            msg = str(e)
            msg = msg[:300] + ("…" if len(msg) > 300 else "")

            if _looks_like_insufficient_quota(e):
                print(f"[llm] OpenAI quota/billing issue: {type(e).__name__}: {msg}")
                return None

            # If we hit rate limits / transient failures, backoff and retry.
            if attempt < max_retries and _looks_like_rate_limit_error(e):
                print(f"[llm] OpenAI rate-limited (retrying): {type(e).__name__}: {msg}")
                sleep_s = base_backoff * (2**attempt) + random.random()
                time.sleep(sleep_s)
                continue

            if attempt >= max_retries:
                print(f"[llm] OpenAI call failed: {type(e).__name__}: {msg}")
            return None



def _fetch_article_html(url: str, timeout: int = 10) -> Optional[str]:
    """Fetch raw HTML for a deal article.

    Best-effort: on any error, return None.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception:
        return None


def _extract_image_and_text(html: str, base_url: str) -> Tuple[Optional[str], str]:
    """Extract a main image URL and article text from HTML.

    - Image: prefers og:image, then twitter:image, then first image in article/main.
    - Text: extracts visible text from main/article, truncated for LLM.
    """
    soup = BeautifulSoup(html, "html.parser")

    # --- Image ---
    image_url: Optional[str] = None

    def _abs(u: str) -> str:
        return urljoin(base_url, u)

    # 1) Open Graph image
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        image_url = _abs(og["content"].strip())

    # 2) Twitter card image
    if not image_url:
        tw = soup.find("meta", attrs={"name": "twitter:image"})
        if tw and tw.get("content"):
            image_url = _abs(tw["content"].strip())

    # 3) First image in <article> or <main>
    if not image_url:
        container = soup.find("article") or soup.find("main") or soup.body
        if container:
            img = container.find("img", src=True)
            if img:
                image_url = _abs(img["src"].strip())

    # --- Text ---
    article_text = ""
    container = soup.find("article") or soup.find("main") or soup.body
    if container:
        # Remove script/style/nav/footer to avoid noise
        for tag in container.find_all(["script", "style", "nav", "footer", "header", "noscript"]):
            tag.decompose()
        article_text = container.get_text(" ", strip=True)

    # Hard truncate to keep LLM payload reasonable
    max_chars = int(os.getenv("DEALS_ENRICH_TEXT_MAX_CHARS", "4000"))
    if len(article_text) > max_chars:
        article_text = article_text[:max_chars]

    return image_url, article_text


def _call_openai_structure(title: str, article_text: str) -> Dict[str, Any]:
    """Call OpenAI to extract structured fields from a deal article.

    Returns a dict with stable keys. On error, returns {}.
    """
    if not openai_client:
        return {}

    system_prompt = (
        "You are a travel deal analyst. Given the title and article text "
        "for a flight deal, extract structured information. Respond with "
        "STRICT JSON only, no markdown, no comments."
    )

    # We support both a simple flat view and a detailed list of itineraries.
    # Each itinerary represents one origin-destination pair and price.
    user_payload = {
        "title": title,
        "article_text": article_text,
        "instructions": {
            "fields": [
                "origin_city",
                "origin_iata",
                "destination_city",
                "destination_iata",
                "airline_name",
                "cabin_class",
                "travel_dates_summary",
                "expires_in",
                "itineraries",
            ]
        },
    }

    try:
        content = _openai_chat_completion(
            model=os.getenv("DEALS_ENRICH_OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        "Extract a JSON object with the following keys: "
                        "origin_city, origin_iata, destination_city, destination_iata, "
                        "airline_name, cabin_class (Economy/Business/Premium Economy/First or null), "
                        "travel_dates_summary, expires_in, itineraries.\n\n"
                        "The value of 'itineraries' MUST be a list of objects. "
                        "Each itinerary object represents a single concrete routing and "
                        "should have, when possible, these keys: origin, destination, "
                        "roundtrip (boolean or null), price (number or null), currency, "
                        "booking_url, airline, cabin_class, "
                        "departure_date (ISO date or null), return_date (ISO date or null), "
                        "travel_dates_text, expires_in.\n\n"
                        "If there are multiple origins or multiple destinations, "
                        "create one itinerary per combination mentioned in the text.\n\n"
                        "Input JSON:\n" + json.dumps(user_payload, ensure_ascii=False)
                    ),
                },
            ],
            temperature=0.1,
        )
        if not content:
            return {}
        # Some models might wrap JSON in code fences; strip common wrappers.
        data = json.loads(_strip_json_wrappers(content))
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def _call_openai_baggage_review(title: str, article_text: str) -> Dict[str, Any]:
    """Call OpenAI to validate baggage inclusion/allowance from article text.

    Returns only baggage-related fields. On error, returns {}.
    """
    if not openai_client:
        return {}

    system_prompt = (
        "You are a travel deal analyst. Read the deal text and decide whether "
        "checked baggage is INCLUDED. Return STRICT JSON only."
    )

    user_payload = {
        "title": title,
        "article_text": article_text,
        "instructions": {
            "fields": [
                "baggage_included",
                "baggage_pieces_included",
                "baggage_allowance_kg",
                "baggage_allowance_display",
                "baggage_summary",
            ],
            "rules": [
                "Set baggage_included=false if the text says checked luggage is NOT included, even if it can be purchased.",
                "Set baggage_included=true only if the text explicitly says it is included.",
                "If unsure or not mentioned, use null.",
            ],
        },
    }

    try:
        content = _openai_chat_completion(
            model=os.getenv("DEALS_ENRICH_OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        "Extract baggage info as JSON with keys: "
                        "baggage_included (true/false/null), baggage_pieces_included (int|null), "
                        "baggage_allowance_kg (number|null), baggage_allowance_display (string|null), "
                        "baggage_summary (string|null).\n\n"
                        "Input JSON:\n" + json.dumps(user_payload, ensure_ascii=False)
                    ),
                },
            ],
            temperature=0.0,
        )
        if not content:
            return {}
        data = json.loads(_strip_json_wrappers(content))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _call_openai_validate_all(title: str, article_text: str) -> Dict[str, Any]:
    """Call OpenAI to validate and correct extracted fields from article text.

    Returns a dict with structured fields. On error, returns {}.
    """
    if not openai_client:
        return {}

    system_prompt = (
        "You validate extracted flight-deal fields. Return ONLY JSON. "
        "Do not invent facts that are not stated."
    )

    user_payload = {
        "title": title,
        "article_text": article_text,
        "instructions": {
            "fields": [
                "origin_city",
                "origin_iata",
                "destination_city",
                "destination_iata",
                "airline_name",
                "cabin_class",
                "travel_dates_summary",
                "expires_in",
            ],
            "rules": [
                "If unsure, use null for that field.",
            ],
        },
    }

    try:
        content = _openai_chat_completion(
            model=os.getenv("DEALS_ENRICH_OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        "Validate fields and return JSON with keys: origin_city, origin_iata, destination_city, "
                        "destination_iata, airline_name, cabin_class (Economy/Business/Premium Economy/First or null), "
                        "travel_dates_summary, expires_in.\n\n"
                        "Input JSON:\n" + json.dumps(user_payload, ensure_ascii=False)
                    ),
                },
            ],
            temperature=0.0,
        )
        if not content:
            return {}
        data = json.loads(_strip_json_wrappers(content))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _call_openai_miles_estimate(title: str, article_text: str) -> Dict[str, Any]:
    """Call OpenAI to estimate miles for a deal.

    Returns:
      - miles_estimate: int | null
      - miles_program: str | null

    On any error returns {}.
    """
    if not openai_client:
        return {}

    system_prompt = (
        "You are a travel deal analyst. Estimate flown miles for the main routing described in the deal. "
        "Return STRICT JSON only."
    )

    user_payload = {
        "title": title,
        "article_text": article_text,
        "instructions": {
            "fields": ["miles_estimate", "miles_program"],
            "rules": [
                "miles_estimate must be an integer number of miles (not km).",
                "If the routing is unclear, set miles_estimate to null.",
                "miles_program is optional (e.g. 'AAdvantage', 'Miles&More'); if unknown, null.",
            ],
        },
    }

    try:
        content = _openai_chat_completion(
            model=os.getenv("DEALS_ENRICH_OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": (
                        "Return JSON with keys: miles_estimate (int|null), miles_program (string|null).\n\n"
                        "Input JSON:\n" + json.dumps(user_payload, ensure_ascii=False)
                    ),
                },
            ],
            temperature=0.0,
        )
        if not content:
            return {}
        data = json.loads(_strip_json_wrappers(content))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _requested_llm_fields_for_deal(deal: Dict[str, Any]) -> List[str]:
    """Compute which fields we want the LLM to fill for this deal.

    Only requests IATA codes and cabin_class — everything else (baggage, miles,
    aircraft) is now fully deterministic and not sent to the LLM.
    """

    allucinate = _truthy_env("DEALS_LLM_ALLUCINATE")
    llm_validate_all = _truthy_env("DEALS_LLM_VALIDATE_ALL") or allucinate

    requested: List[str] = []

    def _need_validate_all(d: Dict[str, Any]) -> bool:
        core = [
            d.get("origin_iata"),
            d.get("destination_iata"),
            d.get("airline"),
            d.get("cabin_class"),
        ]
        return any(_is_missing(x) for x in core)

    if llm_validate_all and _need_validate_all(deal):
        requested.extend(
            [
                "origin_city",
                "origin_iata",
                "destination_city",
                "destination_iata",
                "airline_name",
                "cabin_class",
                "travel_dates_summary",
                "expires_in",
            ]
        )

    # De-dup while preserving order
    seen: set[str] = set()
    out: List[str] = []
    for f in requested:
        if f in seen:
            continue
        seen.add(f)
        out.append(f)
    return out


def _build_llm_context_from_deal(deal: Dict[str, Any]) -> str:
    """Build a short synthetic context string from structured deal fields.

    Used when we don't have article text (e.g. Amadeus rows).
    """

    parts: List[str] = []

    title = str(deal.get("title") or "").strip()
    if title:
        parts.append(f"Title: {title}")

    source = str(deal.get("source") or "").strip()
    if source:
        parts.append(f"Source: {source}")

    oi = str(deal.get("origin_iata") or "").strip().upper()
    di = str(deal.get("destination_iata") or "").strip().upper()
    origin = str(deal.get("origin") or "").strip()
    dest = str(deal.get("destination") or "").strip()

    route_bits: List[str] = []
    if oi and di:
        route_bits.append(f"{oi}->{di}")
    if origin or dest:
        route_bits.append(f"{origin} -> {dest}".strip())
    if route_bits:
        parts.append("Route: " + " | ".join([b for b in route_bits if b and b != "->"]))

    date_out = str(deal.get("date_out") or deal.get("departure_date") or "").strip()
    date_in = str(deal.get("date_in") or deal.get("return_date") or "").strip()
    if date_out or date_in:
        parts.append(f"Dates: {date_out} – {date_in}".strip())

    cabin = str(deal.get("cabin_class") or "").strip()
    airline = str(deal.get("airline") or "").strip()
    aircraft = str(deal.get("aircraft") or "").strip()
    if cabin:
        parts.append(f"Cabin: {cabin}")
    if airline:
        parts.append(f"Airline: {airline}")
    if aircraft:
        parts.append(f"Aircraft: {aircraft}")

    price = deal.get("price")
    currency = str(deal.get("currency") or "").strip().upper()
    if price not in (None, ""):
        parts.append(f"Price: {price} {currency}".strip())

    baggage = str(deal.get("baggage_allowance_display") or deal.get("baggage_summary") or "").strip()
    if baggage:
        parts.append(f"Baggage: {baggage}")

    return "\n".join(parts).strip()


def enrich_deal(deal: Dict[str, Any]) -> Dict[str, Any]:
    """Return a new deal dict enriched with image + structured fields.

    Safe by design: on any error it just returns the original deal.
    """
    link = deal.get("link")
    source = str(deal.get("source") or "").lower()

    # By default we do NOT call OpenAI for Duffel benchmark rows.
    # Duffel deals typically have no article text to ground the model, so
    # enrichment would be guesswork. You can override this explicitly.
    allow_llm_for_duffel = _truthy_env("DEALS_LLM_ENRICH_DUFFEL")
    llm_allowed_for_deal = (source not in ("duffel", "amadeus")) or allow_llm_for_duffel

    # --- Normal enrichment (if there is a link and HTML) ---
    html = None
    article_text = None
    image_url = None
    llm_data: Dict[str, Any] = {}
    if isinstance(link, str) and link:
        html = _fetch_article_html(link)
        if html:
            image_url, article_text = _extract_image_and_text(html, base_url=link)

    # Fallback context (e.g. Amadeus rows often don't have article text)
    context_text = ""
    try:
        context_text = _build_llm_context_from_deal(deal)
    except Exception:
        context_text = ""

    # Apply the same LLM field policy to all sources.
    # (Previously Amadeus skipped baggage fields by default.)
    requested_fields = _requested_llm_fields_for_deal(deal)
    allow_openai = bool(openai_client and llm_allowed_for_deal and requested_fields and ((article_text and str(article_text).strip()) or context_text))

    # In a single call per deal: we only request the missing fields.
    if allow_openai and requested_fields:
        payload_item = {
            "id": 0,
            "title": str(deal.get("title") or ""),
            "source": source,
            "requested_fields": requested_fields,
            "existing": {
                "origin": deal.get("origin"),
                "destination": deal.get("destination"),
                "origin_iata": deal.get("origin_iata"),
                "destination_iata": deal.get("destination_iata"),
                "airline": deal.get("airline"),
                "aircraft": deal.get("aircraft"),
                "cabin_class": deal.get("cabin_class"),
                "miles": deal.get("miles"),
                "price": deal.get("price"),
                "currency": deal.get("currency"),
                "date_out": deal.get("date_out") or deal.get("departure_date"),
                "date_in": deal.get("date_in") or deal.get("return_date"),
            },
            "article_text": (article_text or context_text or ""),
        }
        llm_map = _call_openai_fill_missing_batch([payload_item])
        llm_data = llm_map.get(0, {}) if isinstance(llm_map, dict) else {}

    enriched = dict(deal)
    if image_url:
        enriched["image_url"] = image_url

    # Merge structured fields — only fields that are still LLM targets
    base_keys = [
        "origin_city",
        "origin_iata",
        "destination_city",
        "destination_iata",
        "airline_name",
        "cabin_class",
        "travel_dates_summary",
        "expires_in",
    ]

    used_keys: Dict[str, Any] = {}

    # Persist-safe: only write a small set of fields at top-level.
    # Everything else goes to llm_enriched_fields (JSON) to avoid DB schema drift.
    TOP_LEVEL_SAFE_FIELDS = {
        "origin_iata",
        "destination_iata",
        "cabin_class",
    }

    llm_fields_out: Dict[str, Any] = {}
    cur_llm_fields = enriched.get("llm_enriched_fields")
    if isinstance(cur_llm_fields, dict):
        llm_fields_out.update(cur_llm_fields)

    for key in base_keys:
        if key not in llm_data:
            continue

        val = llm_data.get(key)
        if val in (None, ""):
            continue

        # IATA sanity: only accept 3-letter codes
        if key in {"origin_iata", "destination_iata"}:
            if not isinstance(val, str) or len(val.strip()) != 3 or not val.strip().isalpha():
                continue

        # Only override origin/destination city when missing or clearly bad
        if key == "origin_city":
            cur = deal.get("origin") or deal.get("origin_city")
            if isinstance(cur, str) and cur.strip() and not cur.strip().lower().startswith("from "):
                continue
        if key == "destination_city":
            cur = deal.get("destination") or deal.get("destination_city")
            if isinstance(cur, str) and cur.strip():
                # Don't override if already a readable city name
                if not (len(cur.strip()) == 3 and cur.strip().isalpha()):
                    continue

        # Keep LLM-only fields in JSON; optionally mirror to top-level for safe fields.
        llm_fields_out[key] = val
        if key in TOP_LEVEL_SAFE_FIELDS and _is_missing(enriched.get(key)):
            enriched[key] = val
        used_keys[key] = val

    # Mark deals that have been enriched via LLM
    if llm_data or enriched.get("llm_enriched_fallback"):
        enriched["llm_enriched"] = True
        if used_keys:
            enriched["llm_enriched_fields"] = used_keys
        if llm_fields_out:
            merged_json = dict(enriched.get("llm_enriched_fields") or {})
            if isinstance(merged_json, dict):
                merged_json.update(llm_fields_out)
                enriched["llm_enriched_fields"] = merged_json
        enriched["llm_enrichment_version"] = os.getenv(
            "DEALS_LLM_ENRICHMENT_VERSION",
            "v2-iata-cabin-2026-03-03",
        )

    return enriched


def enrich_deals_batch(deals: List[Dict[str, Any]], max_items: Optional[int] = None) -> List[Dict[str, Any]]:
    """Enrich a batch of deals.

    - Only the first `max_items` are enriched (to cap OpenAI usage).
    - Others are passed through unchanged.
    """
    if not deals:
        return []

    # Helpful operator diagnostics: if LLM flags are enabled but OpenAI isn't configured,
    # make it explicit (without leaking secrets).
    if not openai_client:
        if any(_truthy_env(k) for k in ("DEALS_LLM_ENRICH_DUFFEL", "DEALS_LLM_VALIDATE_ALL")):
            print("[llm] OpenAI client not configured; skipping LLM enrichment (check openai package + OPENAI_API_KEY).")

    if max_items is None or max_items <= 0:
        max_items = len(deals)

    # Precompute cheap context and decide which deals need LLM.
    batch_size = _openai_batch_size()
    pre: List[Dict[str, Any]] = [dict(d) for d in deals]

    pending_items: List[Dict[str, Any]] = []
    pending_index_to_id: Dict[int, int] = {}
    index_context: Dict[int, Dict[str, Any]] = {}

    for idx, d in enumerate(pre):
        # Pass-through items beyond cap
        if idx >= max_items:
            index_context[idx] = {"image_url": None, "llm_data": {}}
            continue

        link = d.get("link")
        source = str(d.get("source") or "").lower()

        allow_llm_for_duffel = _truthy_env("DEALS_LLM_ENRICH_DUFFEL")
        llm_allowed_for_deal = (source not in ("duffel", "amadeus")) or allow_llm_for_duffel

        html = None
        article_text = None
        image_url = None
        if isinstance(link, str) and link:
            html = _fetch_article_html(link)
            if html:
                image_url, article_text = _extract_image_and_text(html, base_url=link)

        # Fallback synthetic context (useful when there's no article_text)
        try:
            context_text = _build_llm_context_from_deal(d)
        except Exception:
            context_text = ""

        requested_fields = _requested_llm_fields_for_deal(d)
        allow_openai = bool(
            openai_client
            and llm_allowed_for_deal
            and requested_fields
            and (
                (article_text and str(article_text).strip() != "")
                or (context_text and context_text.strip() != "")
            )
        )

        # Debuggability: Duffel rows have no article text; make skip reasons visible.
        if (
            source in ("duffel", "amadeus")
            and _truthy_env("DEALS_LLM_ENRICH_DUFFEL")
            and requested_fields
            and not allow_openai
        ):
            print(
                "[llm] Duffel LLM skipped: "
                f"openai_client={bool(openai_client)} "
                f"llm_allowed={bool(llm_allowed_for_deal)} "
                f"requested_fields={requested_fields!r} "
                f"has_article_text={bool(article_text and str(article_text).strip())} "
                f"has_context_text={bool(context_text and context_text.strip())}"
            )

        if allow_openai and requested_fields:
            item_id = idx
            pending_index_to_id[idx] = item_id
            pending_items.append(
                {
                    "id": item_id,
                    "title": str(d.get("title") or ""),
                    "source": source,
                    "requested_fields": requested_fields,
                    "computed": {
                        "cabin_class": d.get("cabin_class"),
                    },
                    "rules": [
                        "Do not overwrite existing facts; only fill requested_fields.",
                    ],
                    "existing": {
                        "origin": d.get("origin"),
                        "destination": d.get("destination"),
                        "origin_iata": d.get("origin_iata"),
                        "destination_iata": d.get("destination_iata"),
                        "airline": d.get("airline"),
                        "cabin_class": d.get("cabin_class"),
                        "price": d.get("price"),
                        "currency": d.get("currency"),
                        "date_out": d.get("date_out") or d.get("departure_date"),
                        "date_in": d.get("date_in") or d.get("return_date"),
                    },
                    "article_text": (article_text or context_text or ""),
                }
            )
        index_context[idx] = {"image_url": image_url, "llm_data": {}}

    llm_results: Dict[int, Dict[str, Any]] = {}
    if pending_items:
        for chunk in _chunked(pending_items, batch_size):
            llm_results.update(_call_openai_fill_missing_batch(chunk))

    # Finalize: apply LLM fields and local fallbacks using enrich_deal logic,
    # but avoid extra OpenAI calls.
    result: List[Dict[str, Any]] = []
    for idx, d in enumerate(pre):
        if idx >= max_items:
            result.append(dict(d))
            continue

        ctx = index_context.get(idx) or {}
        image_url = ctx.get("image_url")
        llm_data = llm_results.get(idx, {}) if llm_results else {}

        # Apply image + llm_data by reusing the same enrichment logic, but
        # without triggering OpenAI again.
        try:
            base = dict(d)
            if image_url and not base.get("image_url"):
                base["image_url"] = image_url

            # Temporarily inject llm_data by calling the same merge/fallback logic
            # through a local copy of enrich_deal implementation.
            # We do this by setting link to empty to prevent re-fetching.
            base_no_fetch = dict(base)
            base_no_fetch["link"] = None

            # Use the merge+fallback portion from enrich_deal by calling enrich_deal,
            # but since link is None it won't do OpenAI. Then we merge llm_data
            # by applying it as if it came from OpenAI.
            enriched = enrich_deal(base_no_fetch)

            # Preserve identity fields from the original deal.
            # `base_no_fetch["link"]` is intentionally None to avoid re-fetching,
            # but we must keep the original article URL for persistence and
            # deduplication.
            if base.get("link") and not enriched.get("link"):
                enriched["link"] = base.get("link")
            if isinstance(llm_data, dict) and llm_data:
                # Re-run merge loop similarly to enrich_deal (minimal duplication):
                # We rely on enrich_deal's existing checks by placing llm_data
                # into llm_enriched_fields via a second pass.
                # The simplest safe approach is to apply missing-only writes here.
                llm_fields_out: Dict[str, Any] = {}
                cur_llm_fields = enriched.get("llm_enriched_fields")
                if isinstance(cur_llm_fields, dict):
                    llm_fields_out.update(cur_llm_fields)

                # Promote only schema-safe fields to top-level.
                PROMOTE_SAFE_FIELDS = {
                    "origin_iata",
                    "destination_iata",
                    "cabin_class",
                }

                for k, v in llm_data.items():
                    if v in (None, ""):
                        continue
                    if k in {"origin_iata", "destination_iata"}:
                        if not isinstance(v, str) or len(v.strip()) != 3 or not v.strip().isalpha():
                            continue
                    llm_fields_out[k] = v
                    if k in PROMOTE_SAFE_FIELDS and _is_missing(enriched.get(k)):
                        enriched[k] = v

                # Mark LLM meta
                enriched["llm_enriched"] = True
                enriched["llm_enriched_fields"] = llm_fields_out
                enriched["llm_enrichment_version"] = os.getenv(
                    "DEALS_LLM_ENRICHMENT_VERSION",
                    "v2-iata-cabin-2026-03-03",
                )

            result.append(enriched)
        except Exception:
            result.append(dict(d))

    return result
