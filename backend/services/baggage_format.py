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


def format_baggage_short_de(deal: Dict[str, Any]) -> Optional[str]:
    """Return a short baggage allowance string for UI snippets.

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
        deal.get("cabin_baggage"),
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

        parsed = " + ".join(compact2)
        # Also respect explicit 'no checked baggage' wording.
        if _NO_BAGGAGE_PAT.search(text):
            return f"{parsed} (ohne Aufgabegepäck)"
        return parsed

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
        parsed = f"{pieces}×{kg_int} kg"
        if no_checked_baggage:
            return f"{parsed} (ohne Aufgabegepäck)"
        return parsed
    if kg_val and kg_val > 0:
        kg_int = int(kg_val) if float(kg_val).is_integer() else kg_val
        parsed = f"{kg_int} kg"
        if no_checked_baggage:
            return f"{parsed} (ohne Aufgabegepäck)"
        return parsed

    for raw in raw_texts:
        if not raw:
            continue
        text = str(raw).strip()
        if not text:
            continue

        # Strip parenthetical notes like fees.
        text = re.sub(r"\s*\([^)]*\)", "", text).strip()

        # pieces×kg handled above (preferred path)

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
                parsed = f"{compact[0]} kg"
            else:
                parsed = " + ".join([f"{c} kg" for c in compact])
            if mentions_cabin and len(compact) == 1:
                parsed = f"{parsed} Handgepäck"

            if no_checked_baggage or is_cabin_only:
                return f"{parsed} (ohne Aufgabegepäck)"
            return parsed

        if _NO_BAGGAGE_PAT.search(text):
            return "Kein Aufgabegepäck"

    if no_checked_baggage:
        # The deal says there is no checked baggage, but we couldn't extract
        # a reliable numeric cabin allowance.
        return "Kein Aufgabegepäck"

    # No reliable baggage info
    return None
