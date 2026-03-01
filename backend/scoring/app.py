from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from typing import List
from amadeus_api import get_flight_offers
from scoring import get_best_amadeus_flights
from html_output import offer_to_html, build_full_html


def save_offers_html(offer_htmls: List[str], out_file: Path | str = Path("HTML_output") / "offers.html", overwrite: bool = True) -> Path:
    """
    Save the combined HTML to `out_file`. Returns the path written.
    If overwrite is False and the file exists, a timestamped filename will be used.
    """
    out = Path(out_file)
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.exists() and not overwrite:
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out = out.with_name(f"{out.stem}-{timestamp}{out.suffix}")

    html = build_full_html(offer_htmls)
    out.write_text(html, encoding="utf-8")
    return out

if __name__ == "__main__":
    load_dotenv(dotenv_path=str(Path(__file__).parent / ".env"))
    amadeus_flights = get_flight_offers(
        origin_location_code="ZRH",
        destination_location_code="LAX",
        departure_date="2025-12-01",
        returnDate="2025-12-31",
        duration="7,14,21",
        adults=1
    )
    top_5_amadeus_flights = get_best_amadeus_flights(amadeus_flights)

    offer_htmls = []
    for idx, flight in enumerate(top_5_amadeus_flights, start=1):
        html = offer_to_html(flight.get("offer"))
        offer_htmls.append(html)

    save_offers_html(offer_htmls)