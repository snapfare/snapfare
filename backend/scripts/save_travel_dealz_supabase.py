import os
import sys

from dotenv import load_dotenv, find_dotenv


def _ensure_project_root_on_path() -> None:
    """Add project root to sys.path so we can import backend.*"""
    this_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(this_dir, os.pardir, os.pardir))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


_ensure_project_root_on_path()

from backend.services.travel_dealz_ingest import ingest_travel_dealz  # noqa: E402


def main(limit: int = 100) -> None:
    """Small wrapper around services.travel_dealz_ingest for ad-hoc runs."""
    load_dotenv(find_dotenv())

    print(f"Running Travel-Dealz ingest with limit={limit}...")
    result = ingest_travel_dealz(limit=limit)
    print("Result:")
    print(result)


if __name__ == "__main__":
    try:
        limit_env = int(os.getenv("TRAVEL_DEALZ_LIMIT", "100"))
    except ValueError:
        limit_env = 100
    main(limit=limit_env)
