import re
from typing import Any, Dict, Optional


_NO_BAGGAGE_PAT = re.compile(
    r"\b(?:no\s+(?:checked\s+)?(?:baggage|luggage)|does\s+not\s+include\s+(?:checked\s+)?(?:baggage|luggage)|"
    r"kein(?:es)?\s+(?:aufgabegepäck|gepäck)|ohne\s+gepäck|"
    r"zzgl\.?\s+gepäck|zuz(?:ü|u)gl\.?\s+gepäck|"
    r"nur\s+(?:noch\s+)?(?:handgepäck|personal\s+item)|"
    r"enthält\s+.*\s+nur\s+.*\s+(?:handgepäck|personal\s+item))\b",
    re.IGNORECASE,
)

_PIECES_X_KG_PAT = re.compile(
    r"(?P<pieces>\d+)\s*(?:x|×)\s*(?P<kg>\d+(?:[\.,]\d+)?)\s*kg\b",
    re.IGNORECASE,
)

_KG_PAT = re.compile(r"(?P<kg>\d+(?:[\.,]\d+)?)\s*kg\b", re.IGNORECASE)


def _to_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        s = val.strip()
        if s.isdigit():
            return int(s)
    return None


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, bool):
        return float(int(val))
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = val.strip().replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None
    return None


def _prepend_hand_luggage(checked_str: str, deal: Dict[str, Any]) -> str:
    """Prepend hand/cabin luggage kg to a checked-baggage display string.

    Looks up the hand luggage kg from airline+cabin defaults, then returns
    e.g. "8 kg + 1×23 kg".  If no hand luggage default is found, returns
    checked_str unchanged.
    """
    # Pipeline stores airline as a full name; we use the stored hand_luggage_kg field if present.
    hand_kg = _to_int(deal.get("hand_luggage_kg"))
    if hand_kg is None:
        cabin = deal.get("cabin_class")
        # Try to look up by stored IATA code (airline_iata field may exist)
        iata = str(deal.get("airline_iata") or "").strip().upper()
        defaults = get_baggage_defaults(iata if iata else None, cabin)
        hand_kg = defaults.get("hand_luggage_kg", 8)
    if hand_kg and hand_kg > 0:
        return f"{hand_kg} kg + {checked_str}"
    return checked_str


def format_baggage_short_de(deal: Dict[str, Any]) -> Optional[str]:
    """Return a short baggage allowance string for UI snippets.

    Format: 'hand_kg kg + pieces×checked_kg kg'  (e.g. '8 kg + 1×23 kg')

    - Prefer structured numeric fields (pieces/kg) when available.
    - If a free-text summary is present, extract only the numeric kg info.
    - Any non-numeric fallback text is returned in German.
    """

    # We do NOT trust/require a boolean baggage_included flag here.
    # Instead, we only declare "no checked baggage" when the deal's own
    # text/fields explicitly say so.
    no_checked_baggage = False

    raw_texts = [
        deal.get("baggage_allowance_display"),
        deal.get("baggage_summary"),
    ]

    # If any text field explicitly states a pieces×kg pattern, trust that first.
    # This avoids cases where structured fields got normalized incorrectly
    # (e.g., kg stored as total instead of per-piece).
    for raw in raw_texts:
        if not raw:
            continue
        text = str(raw).strip()
        if not text:
            continue

        # Strip parenthetical notes like fees.
        text = re.sub(r"\s*\([^)]*\)", "", text).strip()

        matches = list(_PIECES_X_KG_PAT.finditer(text))
        if not matches:
            continue

        parts: list[str] = []
        for m in matches:
            pieces_s = m.group("pieces")
            kg_s = m.group("kg").replace(",", ".")
            try:
                kg_f = float(kg_s)
                if kg_f.is_integer():
                    kg_disp = str(int(kg_f))
                else:
                    kg_disp = str(kg_f)
            except Exception:
                kg_disp = kg_s
            parts.append(f"{pieces_s}×{kg_disp} kg")

        # Dedupe while preserving order
        seen2: set[str] = set()
        compact2: list[str] = []
        for p in parts:
            if p not in seen2:
                compact2.append(p)
                seen2.add(p)

        checked_str = " + ".join(compact2)
        # Also respect explicit 'no checked baggage' wording.
        if _NO_BAGGAGE_PAT.search(text):
            return f"{checked_str} (ohne Aufgabegepäck)"
        return _prepend_hand_luggage(checked_str, deal)

    # Detect "no checked baggage" from any available text field.
    for raw in raw_texts:
        if not raw:
            continue
        text = str(raw).strip()
        if not text:
            continue
        if _NO_BAGGAGE_PAT.search(text):
            no_checked_baggage = True
            break

    pieces = _to_int(deal.get("baggage_pieces_included"))
    kg_val = _to_float(deal.get("baggage_allowance_kg"))
    if pieces and kg_val and pieces > 0 and kg_val > 0:
        kg_int = int(kg_val) if float(kg_val).is_integer() else kg_val
        checked_str = f"{pieces}×{kg_int} kg"
        if no_checked_baggage:
            return f"{checked_str} (ohne Aufgabegepäck)"
        return _prepend_hand_luggage(checked_str, deal)
    if kg_val and kg_val > 0:
        kg_int = int(kg_val) if float(kg_val).is_integer() else kg_val
        checked_str = f"{kg_int} kg"
        if no_checked_baggage:
            return f"{checked_str} (ohne Aufgabegepäck)"
        return _prepend_hand_luggage(checked_str, deal)

    for raw in raw_texts:
        if not raw:
            continue
        text = str(raw).strip()
        if not text:
            continue

        # Strip parenthetical notes like fees.
        text = re.sub(r"\s*\([^)]*\)", "", text).strip()

        kgs = [k.group("kg").replace(",", ".") for k in _KG_PAT.finditer(text)]
        if kgs:
            is_cabin_only = bool(_NO_BAGGAGE_PAT.search(text))
            mentions_cabin = any(tok in text.lower() for tok in ["handgepäck", "cabin", "carry-on", "carry on"])

            # Keep order, dedupe while preserving.
            seen: set[str] = set()
            compact: list[str] = []
            for kg in kgs:
                kg_disp = kg
                try:
                    kg_f = float(kg)
                    if kg_f.is_integer():
                        kg_disp = str(int(kg_f))
                except Exception:
                    pass
                if kg_disp not in seen:
                    compact.append(kg_disp)
                    seen.add(kg_disp)

            if len(compact) == 1:
                checked_str = f"{compact[0]} kg"
            else:
                checked_str = " + ".join([f"{c} kg" for c in compact])
            if mentions_cabin and len(compact) == 1:
                checked_str = f"{checked_str} Handgepäck"

            if no_checked_baggage or is_cabin_only:
                return f"{checked_str} (ohne Aufgabegepäck)"
            return _prepend_hand_luggage(checked_str, deal)

        if _NO_BAGGAGE_PAT.search(text):
            return "Kein Aufgabegepäck"

    if no_checked_baggage:
        return "Kein Aufgabegepäck"

    # Fall back to hard-coded defaults by cabin class
    return _default_baggage_by_cabin(deal.get("cabin_class"))


# Airline + cabin-class specific baggage defaults.
# Key: (airline_iata_upper, cabin_key_lower) → (hand_kg, checked_pieces, checked_kg)
# Use ("*", cabin_key) as fallback when airline not listed.
_AIRLINE_CABIN_BAGGAGE: dict[tuple, tuple] = {
    # (airline_iata, cabin_class_lower) → (hand_kg, checked_pieces, checked_kg)
    ("BA", "economy"):         (8,  0,  0),   # BA Economy: hand bag only, no checked included
    ("BA", "premium economy"): (12, 1, 23),
    ("BA", "business"):        (12, 2, 32),
    ("BA", "first"):           (14, 3, 32),
    ("LH", "economy"):         (8,  1, 23),
    ("LH", "premium economy"): (8,  2, 23),
    ("LH", "business"):        (12, 2, 32),
    ("LH", "first"):           (14, 3, 32),
    ("LX", "economy"):         (8,  1, 23),
    ("LX", "premium economy"): (8,  2, 23),
    ("LX", "business"):        (12, 2, 32),
    ("LX", "first"):           (14, 3, 32),
    ("OS", "economy"):         (8,  1, 23),
    ("OS", "business"):        (12, 2, 32),
    ("EK", "economy"):         (7,  2, 23),
    ("EK", "business"):        (14, 2, 32),
    ("EK", "first"):           (14, 3, 32),
    ("TK", "economy"):         (8,  1, 30),
    ("TK", "business"):        (12, 2, 32),
    ("QR", "economy"):         (7,  1, 23),
    ("QR", "premium economy"): (10, 1, 23),
    ("QR", "business"):        (14, 2, 32),
    ("QR", "first"):           (14, 3, 32),
    ("EY", "economy"):         (7,  1, 23),
    ("EY", "business"):        (14, 2, 32),
    ("SQ", "economy"):         (7,  1, 23),
    ("SQ", "business"):        (14, 2, 32),
    ("AF", "economy"):         (12, 1, 23),
    ("AF", "business"):        (12, 2, 32),
    ("KL", "economy"):         (12, 1, 23),
    ("KL", "business"):        (12, 2, 32),
    # Generic fallbacks by cabin only
    ("*", "economy"):          (8,  1, 23),
    ("*", "premium economy"):  (10, 1, 23),
    ("*", "business"):         (12, 2, 32),
    ("*", "first"):            (14, 3, 32),
}


def _cabin_key(cabin_class: Any) -> str:
    """Normalize cabin_class to lowercase key used in baggage lookup."""
    c = (cabin_class or "").strip().upper()
    if c in {"BUSINESS", "J", "C", "D", "Z", "R"}:
        return "business"
    if c in {"PREMIUM ECONOMY", "PREMIUM_ECONOMY", "W", "P"}:
        return "premium economy"
    if c in {"FIRST", "F"}:
        return "first"
    return "economy"


def get_baggage_defaults(airline_iata: Optional[str], cabin_class: Any) -> Dict[str, Any]:
    """Return structured baggage defaults for a given airline + cabin.

    Returns a dict with keys:
      baggage_included (bool), baggage_pieces_included (int),
      baggage_allowance_kg (float), hand_luggage_kg (int)
    """
    code = (airline_iata or "").strip().upper()
    cabin = _cabin_key(cabin_class)

    hand_kg, checked_pieces, checked_kg = (
        _AIRLINE_CABIN_BAGGAGE.get((code, cabin))
        or _AIRLINE_CABIN_BAGGAGE.get(("*", cabin))
        or (8, 1, 23)
    )
    return {
        "baggage_included": checked_pieces > 0,
        "baggage_pieces_included": checked_pieces,
        "baggage_allowance_kg": int(checked_kg) if checked_kg > 0 else 0,
        "hand_luggage_kg": hand_kg,
    }


# Default baggage allowances per cabin class (German display strings).
# Kept for backward compatibility but format_baggage_short_de now uses structured data.
_CABIN_BAGGAGE_DEFAULTS: dict[str, str] = {
    "economy": "1×23 kg",
    "premium economy": "2×23 kg",
    "business": "2×32 kg",
    "first": "3×32 kg",
}


def _default_baggage_by_cabin(cabin_class: Any) -> Optional[str]:
    """Return the cabin-class default baggage string, or None if cabin is unknown."""
    cabin = (cabin_class or "").strip().upper()
    airline = None  # no airline context available at this point
    if cabin in {"BUSINESS", "J", "C", "D", "Z"}:
        cabin_norm = "business"
    elif cabin in {"PREMIUM ECONOMY", "PREMIUM_ECONOMY", "W", "P"}:
        cabin_norm = "premium economy"
    elif cabin in {"FIRST", "F"}:
        cabin_norm = "first"
    elif cabin in {"ECONOMY", "Y", "M", "H", "K", "L", "Q", "T", "V", "X", "B", "E", "N", "O", "S"}:
        cabin_norm = "economy"
    else:
        return None
    defaults = get_baggage_defaults(airline, cabin_norm)
    return _fmt_baggage(
        defaults["hand_luggage_kg"],
        defaults["baggage_pieces_included"],
        int(defaults["baggage_allowance_kg"]),
        defaults["baggage_included"],
    )


def _fmt_baggage(hand_kg: int, checked_pieces: int, checked_kg: int, baggage_included: bool) -> str:
    """Format a baggage display string: 'hand_kg kg + pieces×checked_kg kg'."""
    if not baggage_included or checked_pieces == 0 or checked_kg == 0:
        if hand_kg > 0:
            return f"{hand_kg} kg Handgepäck"
        return "Kein Aufgabegepäck"
    checked_str = f"{checked_pieces}×{checked_kg} kg"
    if hand_kg > 0:
        return f"{hand_kg} kg + {checked_str}"
    return checked_str
