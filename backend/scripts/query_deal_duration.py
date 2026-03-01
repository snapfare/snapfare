import sys
import json
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

# Ensure backend package is importable
THIS_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = THIS_DIR.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Load env so Supabase credentials are available
load_dotenv(find_dotenv())

from database.supabase_db import _client


def main() -> int:
    if not _client:
        print("Supabase client is not configured.")
        return 1

    if len(sys.argv) < 2:
        print("Usage: python -m backend.scripts.query_deal_duration <deal_link>")
        return 1

    link = sys.argv[1].strip()
    try:
        rsp = (
            _client.table("deals")
            .select(
                "id, link, source, flight_duration_minutes, flight_duration_display"
            )
            .eq("link", link)
            .order("id", desc=True)
            .limit(10)
            .execute()
        )
    except Exception as exc:  # pragma: no cover - best-effort diagnostic
        print(f"Query failed: {exc}")
        return 1

    data = getattr(rsp, "data", []) or []
    print(json.dumps(data, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
