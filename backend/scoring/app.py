"""Legacy dev entry-point — now delegates to the Duffel pipeline.

Run from the repo root:
  python -m backend.scoring.app
"""
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from typing import List
from scoring.duffel_api import get_flight_offers
from scoring.scoring import get_best_amadeus_flights
from scoring.html_output import offer_to_html, build_full_html


def save_offers_html(offer_htmls: List[str], out_file: Path | str = Path("HTML_output") / "offers.html", overwrite: bool = True) -> Path:
    """Save the combined HTML to `out_file`. Returns the path written."""
    out = Path(out_file)
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.exists() and not overwrite:
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out = out.with_name(f"{out.stem}-{timestamp}{out.suffix}")

    html = build_full_html(offer_htmls)
    out.write_text(html, encoding="utf-8")
    return out

if __name__ == "__main__":
    import os
    load_dotenv(dotenv_path=str(Path(__file__).parent.parent / ".env"))
    access_token = os.getenv("DUFFEL_API_KEY", "")
    duffel_flights = get_flight_offers(
        origin_location_code="ZRH",
        destination_location_code="LAX",
        departure_date="2025-12-01",
        return_date="2025-12-31",
        adults=1,
        access_token=access_token,
    )
    top_5 = get_best_amadeus_flights(duffel_flights)

    offer_htmls = []
    for flight in top_5:
        html = offer_to_html(flight.get("offer"))
        offer_htmls.append(html)

    save_offers_html(offer_htmls)
