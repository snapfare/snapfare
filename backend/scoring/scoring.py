# python
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple


def _parse_iso8601_duration_to_minutes(duration: str) -> int:
    """
    Parse ISO 8601 duration strings like 'PT2H30M' to minutes.
    Returns 0 for invalid/empty input.
    """
    if not duration or not duration.startswith("P"):
        return 0
    hours = 0
    minutes = 0
    # match hours and minutes (seconds ignored)
    m_h = re.search(r'(\d+)H', duration)
    m_m = re.search(r'(\d+)M', duration)
    if m_h:
        hours = int(m_h.group(1))
    if m_m:
        minutes = int(m_m.group(1))
    return hours * 60 + minutes


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def get_best_amadeus_flights(amadeus_results: List[Dict[str, Any]],
                     top_n: int = 5) -> List[Dict[str, Any]]:
    """
    Rank Amadeus flight offers and return the best `top_n` offers.
    Each returned item includes:
      - 'offer': original offer dict
      - 'score': computed score (lower is better)
      - 'price': total price as float
      - 'duration_mins': total duration in minutes (sum of all segments)
      - 'stops': total number of stops (sum of segments-1 per itinerary)

    Scoring combines normalized price, duration and stops (equal weights).
    """
    if not amadeus_results:
        return []

    parsed_offers = []
    for offer in amadeus_results:
        # Price: try common Amadeus fields
        price_total = 0.0
        price = offer.get("price") or {}
        price_total = _safe_float(price.get("total") or price.get("grandTotal") or price.get("totalPrice"), 0.0)

        # Duration & stops: iterate itineraries -> segments
        total_minutes = 0
        total_stops = 0
        for itin in offer.get("itineraries", []):
            segments = itin.get("segments", []) or []
            for seg in segments:
                # segment duration may be in 'duration' (ISO 8601) or 'carrierCode'...
                seg_dur = seg.get("duration") or seg.get("segmentDuration") or ""
                total_minutes += _parse_iso8601_duration_to_minutes(seg_dur)
            # stops for this itinerary is number of intermediate stops = segments - 1
            total_stops += max(0, len(segments) - 1)

        parsed_offers.append({
            "offer": offer,
            "price": price_total,
            "duration_mins": total_minutes,
            "stops": total_stops,
            "score": 0.0,  # placeholder
        })

    # Gather min/max for normalization
    prices = [o["price"] for o in parsed_offers]
    durations = [o["duration_mins"] for o in parsed_offers]
    stops = [o["stops"] for o in parsed_offers]

    min_price, max_price = min(prices), max(prices)
    min_dur, max_dur = min(durations), max(durations)
    min_stops, max_stops = min(stops), max(stops)

    def _normalize(value: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return (value - lo) / (hi - lo)

    # weights can be tuned
    w_price = 0.45
    w_duration = 0.40
    w_stops = 0.15

    for o in parsed_offers:
        n_price = _normalize(o["price"], min_price, max_price)
        n_dur = _normalize(o["duration_mins"], min_dur, max_dur)
        n_stops = _normalize(o["stops"], min_stops, max_stops)
        # lower raw metrics should produce lower score => combine normalized values
        o["score"] = w_price * n_price + w_duration * n_dur + w_stops * n_stops

    # sort by ascending score (best first) and return top_n
    parsed_offers.sort(key=lambda x: x["score"])
    return parsed_offers[: top_n if top_n > 0 else 0]


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
