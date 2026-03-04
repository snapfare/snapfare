import os
import re
from typing import Any, Dict, List

try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None

SUPABASE_URL = os.getenv("SUPABASE_URL")
# Prefer service role key (bypasses RLS for backend writes); fall back to anon key
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
    or os.getenv("SUPABASE_PUBLISHABLE_KEY")
)
# Client type may not be available at runtime if import fails, so avoid
# using it directly in a runtime-evaluated type expression.
_client: Any = None
if create_client and SUPABASE_URL and SUPABASE_KEY:
    _client = create_client(SUPABASE_URL, SUPABASE_KEY)


def _normalize_iata_code(val: Any) -> Any:
    if not isinstance(val, str):
        return None
    code = val.strip().upper()
    if len(code) != 3 or not code.isalpha():
        return None
    return code


def save_deals(table: str, deals: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not _client:
        return {"status": "disabled", "reason": "Supabase not configured"}

    # For tables with unique constraint on booking_url we use upsert to avoid 23505 errors.
    is_upsert = table in {"deals_traveldealz", "deals_secretflying", "deals"}

    cleaned: List[Dict[str, Any]] = [d for d in deals if isinstance(d, dict)]
    # Defensive normalization: IATA codes are often used for joining/scoring.
    # Ensure we never persist whitespace/newline-polluted codes.
    for row in cleaned:
        try:
            if "origin_iata" in row:
                row["origin_iata"] = _normalize_iata_code(row.get("origin_iata"))
            if "destination_iata" in row:
                row["destination_iata"] = _normalize_iata_code(row.get("destination_iata"))
        except Exception:
            pass
    stripped: set[str] = set()
    original_error: str | None = None

    # Backward/forward compatibility: if the DB schema cache doesn't have a
    # column, PostgREST returns: "Could not find the '<col>' column ...".
    # Strip missing column(s) and retry a few times (some errors surface one col at a time).
    for _ in range(5):
        try:
            if is_upsert:
                rsp = _client.table(table).upsert(cleaned, on_conflict="booking_url").execute()
            else:
                rsp = _client.table(table).insert(cleaned).execute()
            out: Dict[str, Any] = {"status": "ok", "data": rsp.data}
            if stripped:
                out["schema_stripped"] = sorted(stripped)
            if original_error:
                out["original_error"] = original_error
            return out
        except Exception as e:
            msg = str(e)
            if original_error is None:
                original_error = msg

            missing_cols = set(re.findall(r"Could not find the '([^']+)' column", msg))
            if not missing_cols or not cleaned:
                return {"status": "error", "error": msg, "original_error": original_error}

            stripped.update(missing_cols)
            cleaned = [{k: v for k, v in row.items() if k not in missing_cols} for row in cleaned]

    return {"status": "error", "error": original_error or "Unknown error"}


def test_connection(table: str | None = None) -> Dict[str, Any]:
    """Return basic connection status. Optionally attempt a lightweight query."""
    if not _client:
        return {
            "status": "disabled",
            "reason": "Supabase not configured",
            "env": {
                "SUPABASE_URL": bool(SUPABASE_URL),
                "SUPABASE_KEY": bool(SUPABASE_KEY),
            },
        }
    if not table:
        return {"status": "ok", "client_created": True}
    try:
        rsp = _client.table(table).select("*").limit(1).execute()
        return {"status": "ok", "sample": rsp.data}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_deals(table: str, limit: int = 10) -> Dict[str, Any]:
    """Retrieve deals from Supabase table."""
    if not _client:
        return {"status": "disabled", "reason": "Supabase not configured"}
    try:
        # First try to order by created_at if it exists in the table
        try:
            rsp = (
                _client
                .table(table)
                .select("*")
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
        except Exception:
            # If the column doesn't exist or ordering fails, do a simple query
            rsp = _client.table(table).select("*").limit(limit).execute()

        return {"status": "ok", "count": len(rsp.data), "deals": rsp.data}
    except Exception as e:
        return {"status": "error", "error": str(e), "deals": []}

