from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import os
from dotenv import load_dotenv, find_dotenv

# Load environment variables from project root .env (fallback to defaults)
load_dotenv(find_dotenv())

app = FastAPI(title="snapcore backend", version="0.1.0")

# Templates
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Import services and scrapers
try:
    from services.duffel_service import search_flights
except Exception:
    def search_flights(*args, **kwargs):
        return {"status": "disabled", "reason": "Duffel not configured"}

try:
    from services.openai_service import generate_suggestions
except Exception:
    def generate_suggestions(*args, **kwargs):
        return {"status": "disabled", "reason": "OpenAI not configured"}

try:
    from scrapers.travel_dealz import (
        get_deals as get_travel_dealz,
        get_deals_de as get_travel_dealz_de,
    )
    from scrapers.secretflying import get_deals as get_secretflying
except Exception:
    def get_travel_dealz(limit: int = 10):
        return []

    def get_travel_dealz_de(limit: int = 10):
        return []

    def get_secretflying(limit: int = 10):
        return []

try:
    from services.deals_pipeline import run_deals_pipeline, render_html_snippet
except Exception:
    def run_deals_pipeline(*args, **kwargs):
        return {"status": "disabled", "reason": "Deals pipeline not available"}

    def render_html_snippet(*args, **kwargs):
        return "<!-- Deals pipeline not available -->"

try:
    from database.supabase_db import save_deals, test_connection as supabase_test_connection, get_deals as get_saved_deals
except Exception:
    def save_deals(*args, **kwargs):
        return {"status": "disabled", "reason": "Supabase not configured"}
    def supabase_test_connection(*args, **kwargs):
        return {"status": "disabled", "reason": "Supabase not configured"}
    def get_saved_deals(*args, **kwargs):
        return {"status": "disabled", "reason": "Supabase not configured"}

# Parse SCRAPING_URL env to enable/disable scrapers
def _parse_scraping_sources():
    raw = os.getenv("SCRAPING_URL", "").strip()
    if not raw:
        return {"travel-dealz", "secretflying"}
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    enabled = set()
    for p in parts:
        if "travel-dealz" in p:
            enabled.add("travel-dealz")
        if "secretflying" in p:
            enabled.add("secretflying")
    # If nothing matched, default to all
    return enabled or {"travel-dealz", "secretflying"}

ENABLED_SOURCES = _parse_scraping_sources()


def _parse_source_param(raw: str | None) -> set[str] | None:
    """Parse the `source` query parameter.

    Accepts both simple numeric codes and names:
      - "1", "travel-dealz"  -> {"travel-dealz"}
      - "2", "secretflying"  -> {"secretflying"}
      - "all", "both", "*" -> {"travel-dealz", "secretflying"}

    Returns None if not recognized, to fall back to the default behavior
    based on SCRAPING_URL.
    """

    if not raw:
        return None

    v = raw.strip().lower()
    if v in {"1", "travel-dealz", "travel_dealz", "travel"}:
        return {"travel-dealz"}
    if v in {"2", "secretflying", "secret-flying", "secret"}:
        return {"secretflying"}
    if v in {"all", "both", "*", "3"}:
        return {"travel-dealz", "secretflying"}
    return None

# Scraping configuration via environment variables
DEFAULT_DEALS_LIMIT = int(os.getenv("DEALS_DEFAULT_LIMIT", "10"))
MAX_DEALS_LIMIT = int(os.getenv("DEALS_MAX_LIMIT", "200"))
PERSIST_DEFAULT = os.getenv("DEALS_PERSIST_DEFAULT", "true").strip().lower() in {"1", "true", "yes", "on"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "app_name": "snapcore"})


@app.get("/deals")
async def deals(
    limit: int = Query(DEFAULT_DEALS_LIMIT, ge=1, le=MAX_DEALS_LIMIT),
    persist: bool = Query(PERSIST_DEFAULT),
    source: str | None = Query(None, description="Source: 1/travel-dealz, 2/secretflying, all/both"),
):
    """Scrape and return flight deals, optionally persist to Supabase."""
    override_sources = _parse_source_param(source)
    sources_enabled = override_sources or ENABLED_SOURCES

    print(
        f"[deals] Starting scrape with limit={limit}, persist={persist}, "
        f"sources={sources_enabled} (override={override_sources is not None})"
    )

    data = []
    if "travel-dealz" in sources_enabled:
        # .com and .de
        td_com = get_travel_dealz(limit=limit)
        td_de = get_travel_dealz_de(limit=limit)
        data.extend(td_com)
        data.extend(td_de)
        print(f"[deals] Travel-Dealz: +{len(td_com) + len(td_de)} deals (total={len(data)})")
    if "secretflying" in sources_enabled:
        sf = get_secretflying(limit=limit)
        data.extend(sf)
        print(f"[deals] SecretFlying: +{len(sf)} deals (total={len(data)})")

    # Respect the total limit of exposed/inserted deals
    if len(data) > limit:
        data = data[:limit]
        print(f"[deals] Truncated to global limit={limit}")

    result = {
        "count": len(data),
        "deals": data,
        "sources_enabled": list(sources_enabled),
    }

    # Auto-persist to Supabase by default
    if persist and data:
        supabase_payload = [
            {
                "title": d.get("title"),
                "price": d.get("price"),
                "currency": d.get("currency"),
                "link": d.get("link"),
            }
            for d in data
        ]
        save_result = save_deals("deals", supabase_payload)
        persisted_ok = save_result.get("status") == "ok"
        result["persisted"] = persisted_ok
        result["persisted_count"] = len(supabase_payload) if persisted_ok else 0
        print(f"[deals] Persist result: status={save_result.get('status')}, count={result['persisted_count']}")
        if not persisted_ok:
            result["persist_info"] = save_result

    return JSONResponse(content=result)


@app.get("/deals/saved")
async def deals_saved(limit: int = Query(10, ge=1, le=100)):
    """Retrieve saved deals from Supabase database (deals table)."""
    result = get_saved_deals("deals", limit=limit)
    return JSONResponse(content=result)


@app.get("/deals/saved/secretflying")
async def deals_saved_secretflying(limit: int = Query(10, ge=1, le=100)):
    """Retrieve saved SecretFlying deals from Supabase database (deals table)."""
    result = get_saved_deals("deals", limit=limit)
    return JSONResponse(content=result)


@app.get("/search")
async def search(origin: str, destination: str, departure_date: str):
    """Search flights using Amadeus API."""
    result = search_flights(origin=origin, destination=destination, departure_date=departure_date)
    return JSONResponse(content={"result": result})


@app.post("/suggest")
async def suggest(preferences: dict):
    """Generate travel suggestions using OpenAI."""
    suggestions = generate_suggestions(preferences)
    return JSONResponse(content={"suggestions": suggestions})


@app.get("/pipeline/deals")
async def deals_pipeline(
    limit: int = Query(DEFAULT_DEALS_LIMIT, ge=1, le=MAX_DEALS_LIMIT),
    persist: bool = Query(PERSIST_DEFAULT),
    enrich: bool | None = Query(None),
    source: str | None = Query(None, description="Source: 1/travel-dealz, 2/secretflying, all/both"),
):
    """Run the structured deals pipeline (scrape, score, enrich, HTML)."""
    override_sources = _parse_source_param(source)
    result = run_deals_pipeline(
        limit=limit,
        persist=persist,
        max_items_html=limit,
        enrich=enrich,
        sources=override_sources,
    )
    return JSONResponse(content=result)


@app.get("/pipeline/deals/html", response_class=HTMLResponse)
async def deals_pipeline_html(
    limit: int = Query(DEFAULT_DEALS_LIMIT, ge=1, le=MAX_DEALS_LIMIT),
    persist: bool = Query(False),
    enrich: bool | None = Query(None),
    source: str | None = Query(None, description="Source: 1/travel-dealz, 2/secretflying, all/both"),
):
    """Return only the HTML snippet from the deals pipeline.

    By default this endpoint does *not* persist to Supabase to make it
    safe to call frequently from previews or CMS integrations.
    """
    override_sources = _parse_source_param(source)
    result = run_deals_pipeline(
        limit=limit,
        persist=persist,
        max_items_html=limit,
        enrich=enrich,
        sources=override_sources,
    )
    html = result.get("html_snippet") or "<!-- No deals available -->"
    return HTMLResponse(content=html)


@app.get("/supabase/test")
async def supabase_test(table: str | None = None):
    """Test Supabase connection and optionally query a table."""
    result = supabase_test_connection(table)
    return JSONResponse(content={"supabase": result})
