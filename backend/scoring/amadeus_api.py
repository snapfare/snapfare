from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv
from amadeus import Client, ResponseError
from urllib.parse import urlencode
import requests


# Load the nearest .env file (typically the same directory as the app when running)
load_dotenv(find_dotenv())

def get_flight_offers(origin_location_code: str,
                      destination_location_code: str,
                      departure_date: str,
                      returnDate: str,
                      duration: str,
                      adults: int,
                      client_id: str | None = None,
                      client_secret: str | None = None):
    """
    Fetch flight offers from Amadeus and return response.data.
    Credentials are read from environment variables:
            - AMADEUS_API_KEY / AMADEUS_API_SECRET (legacy names)
            - AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET (same as backend services)

    Optional client_id/client_secret arguments override env vars.
    Raises ResponseError on API errors and RuntimeError if credentials are missing.
    """
    cid = client_id or os.getenv("AMADEUS_API_KEY") or os.getenv("AMADEUS_CLIENT_ID")
    csecret = client_secret or os.getenv("AMADEUS_API_SECRET") or os.getenv("AMADEUS_CLIENT_SECRET")

    if not cid or not csecret:
        raise RuntimeError(
            "Missing Amadeus credentials in environment (AMADEUS_API_KEY / "
            "AMADEUS_API_SECRET or AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET)"
        )

    ## TODO: Switch to "production" hostname when going live
    amadeus = Client(client_id=cid, client_secret=csecret, hostname="test")

    #offers = get_cheapest_flight_for_range(origin_location_code, destination_location_code, departure_date, duration, adults, amadeus)
    #offers = http_get_flight_offers_example(cid, csecret, origin_location_code, destination_location_code, departure_date, duration, hostname="https://test.api.amadeus.com")
    offers = get_flights_for_range(origin_location_code, destination_location_code, departure_date, returnDate, adults, amadeus)
    return offers

def get_cheapest_flight_for_range(origin_location_code: str,
                      destination_location_code: str,
                      departure_date: str,
                      duration: str,
                      adults: int = 1,
                      amadeus: Client = None):
    try:
        response = amadeus.shopping.flight_dates.get(
            originLocationCode=origin_location_code,
            destinationLocationCode=destination_location_code,
            departureDate=departure_date,
            duration=duration,
            adults=adults
        )
        return response.data
    except ResponseError as error:
        # re-raise to let the caller handle logging / retry / user-facing messages
        raise

def get_flights_for_range(origin_location_code: str,
                                  destination_location_code: str,
                                  departure_date: str,
                                  returnDate: str,
                                  adults: int = 1,
                                  amadeus: Client = None):
    try:
        response = amadeus.shopping.flight_offers_search.get(
            originLocationCode=origin_location_code,
            destinationLocationCode=destination_location_code,
            departureDate=departure_date,
            returnDate=returnDate,
            adults=adults
        )
        return response.data
    except ResponseError as error:
        # re-raise to let the caller handle logging / retry / user-facing messages
        raise

def http_get_flight_offers_example(client_id: str,
                                   client_secret: str,
                                   origin_location_code: str,
                                   destination_location_code: str,
                                   departure_date: str,
                                   duration: int,
                                   hostname: str = "https://test.api.amadeus.com") -> dict | None:
    """
    Obtain an access token and call Amadeus v1/shopping/flight-dates.
    Token body is URL-encoded per the Amadeus docs.
    Returns parsed JSON on 200, None on 404, raises on other HTTP errors.
    """
    if not client_id or not client_secret:
        raise RuntimeError("Missing client_id/client_secret for token request")

    token_url = f"{hostname}/v1/security/oauth2/token"
    token_payload = urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    })

    # 1) Request access token (body must be x-www-form-urlencoded)
    token_resp = requests.post(token_url, data=token_payload,
                               headers={"Content-Type": "application/x-www-form-urlencoded"},
                               timeout=10)
    token_resp.raise_for_status()
    token = token_resp.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to obtain access token from Amadeus")

    # 2) Call the flight-dates endpoint
    url = f"{hostname}/v1/shopping/flight-dates?origin={origin_location_code}&destination={destination_location_code}&departureDate={departure_date}&oneWay=false&duration={duration}"
    params = {
        "origin": origin_location_code,
        "destination": destination_location_code,
        "departureDate": departure_date,
        "duration": str(duration)
    }
    #url = "https://test.api.amadeus.com/v1/shopping/flight-destinations?origin=MAD&departureDate=2025-12-01,2025-12-31&oneWay=false&duration=1,15"
    #url = "https://test.api.amadeus.com/v1/shopping/flight-dates?origin=MAD&destination=MUC&departureDate=2025-12-15"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)

    # 3) Handle response codes
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
