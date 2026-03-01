import os
from typing import Any, Dict

try:
    from amadeus import Client
except Exception:
    Client = None

ENV = os.getenv("AMADEUS_ENV", "test")
CLIENT_ID = os.getenv("AMADEUS_CLIENT_ID")
CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET")

_client = None
if Client and CLIENT_ID and CLIENT_SECRET:
    _client = Client(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, hostname=ENV)


def search_flights(origin: str, destination: str, departure_date: str) -> Dict[str, Any]:
    if not _client:
        return {"status": "disabled", "reason": "Amadeus not configured"}
    try:
        # Basic example using flight-offers search v2
        rsp = _client.shopping.flight_offers_search.get(
            originLocationCode=origin,
            destinationLocationCode=destination,
            departureDate=departure_date,
            adults=1,
        )
        return {"status": "ok", "data": rsp.data}
    except Exception as e:
        return {"status": "error", "error": str(e)}
