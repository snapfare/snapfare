# SnapFare Backend

Python pipeline for scraping, enriching, scoring, and persisting flight deals.

---

## Architecture Overview

```
app.py (root)
  └── backend/scripts/run.py          — unified CLI; reads run_config.json

  scrapers/
    travel_dealz.py                   — paginate Travel-Dealz .com + .de listings
    secretflying.py                   — paginate SecretFlying (ScrapingBee / proxies)

  services/
    travel_dealz_article_parser.py    — parse individual Travel-Dealz deal pages
    secretflying_article_parser.py    — parse individual SecretFlying deal posts
    deals_pipeline.py                 — orchestrate scrape → parse → enrich → score → persist
    deals_enrichment.py               — OpenAI LLM enrichment (fill / correct modes, gpt-4o-mini)
    duffel_service.py                 — Duffel flight price benchmark client (raw HTTP, Duffel API v2)
    unsplash_service.py               — Unsplash destination image fetcher (JSON cache)
    skyscanner_links.py               — Skyscanner affiliate deep-link builder (2 adults, .ch domain)
    openai_service.py                 — OpenAI client with throttling and retry
    baggage_format.py                 — normalize baggage allowance to German display string
    email_sender.py                   — transactional email delivery

  scoring/
    scoring.py                        — deal scoring (40% price / 40% duration / 20% stops)
    miles_utils.py                    — great-circle distance + FFP miles estimation (cabin-aware rates)
    duffel_api.py                     — Duffel Offers API client (raw HTTP, NOT the duffel-api SDK)
    html_output.py                    — render deal cards (render_deal_card / build_deals_html)

  database/
    supabase_db.py                    — Supabase upsert (prefers SERVICE_ROLE_KEY)
```

### Pipeline data flow

```
Travel-Dealz listing  ──┐
SecretFlying listing  ──┤
                        ▼
               deals_pipeline.py
                        │
              ┌─────────▼──────────┐
              │  Article parsers   │  (extract: route, price, dates, cabin,
              │  (TravelDealz /    │   baggage, IATA codes, booking URL)
              │   SecretFlying)    │
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │  Deterministic     │  (miles estimate, baggage format,
              │  enrichment        │   scoring, IATA normalization,
              │                    │   Unsplash images, Skyscanner links)
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │  LLM enrichment    │  (optional; fills missing fields /
              │  (OpenAI)          │   validates ambiguous ones)
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │  Duffel benchmarks │  (optional; adds cheapest known price
              │                    │   for route/month comparison)
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │  Supabase upsert   │  (public.deals — unique on booking_url)
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │  HTML output       │  deals_free.html / deals_premium.html
              └────────────────────┘
```

---

## Setup

```bash
# From repo root
python -m venv .venv
.venv\Scripts\Activate.ps1     # Windows
# source .venv/bin/activate    # macOS/Linux

pip install -r backend/requirements.txt
playwright install              # only needed if using Playwright scraping
```

Copy `.env.example` → `.env` at the repo root and fill in credentials.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes (backend) | Bypasses RLS for upserts |
| `SUPABASE_ANON_KEY` | Fallback | Used if service role key not set |
| `DUFFEL_API_KEY` | For Duffel | Duffel API v2 access token |
| `DUFFEL_ASSUME_BAGGAGE` | No | Set `true` to assume baggage included for Duffel rows |
| `OPENAI_API_KEY` | For LLM modes | GPT enrichment (gpt-4o-mini) |
| `OPENAI_MIN_SECONDS_BETWEEN_CALLS` | No | Rate limit throttle (default: 3) |
| `UNSPLASH_ACCESS_KEY` | For images | Destination photos from Unsplash |
| `SKYSCANNER_AFFILIATE_ID` | For links | Skyscanner affiliate deep-link ID |
| `SCRAPINGBEE_API_KEY` | For SecretFlying | ScrapingBee anti-bot bypass |
| `APIFY_API_KEY` | Optional | Apify proxy for scraping |
| `SCRAPERAPI_KEY` | Optional | ScraperAPI fallback |
| `SCRAPINGANT_API_KEY` | Optional | ScrapingAnt fallback |
| `DISPLAY_CURRENCY` | No | Currency for HTML display (default: CHF) |
| `FX_EUR_TO_CHF` | No | EUR→CHF conversion rate (default: 0.93) |
| `ORIGIN_IATA_FILTER` | No | Comma-separated origin airports (default: ZRH,GVA,BSL,BRN,LUG,SIR) |

---

## CLI Commands

All commands are run from the repo root:

```bash
python app.py                               # runs default_command + default_mode from run_config.json
python app.py deals-html --mode <mode>      # end-to-end: scrape + parse + persist + generate HTML
python app.py pipeline --mode <mode>        # scrape + parse + persist (no HTML)
python app.py duffel-refresh --mode <mode>  # Duffel benchmark refresh only
python app.py html-from-db --mode <mode>    # render HTML from Supabase (no scraping)
python app.py html-snippet --mode <mode>    # generate a single deal HTML snippet
python app.py server                        # start FastAPI server (uvicorn)
```

---

## Run Modes (`run_config.json`)

Modes configure every aspect of a pipeline run. Key fields:

| Field | Description |
|-------|-------------|
| `persist` | Whether to write to Supabase |
| `sources` | Which scrapers to run (`travel-dealz`, `secretflying`) |
| `scrape.traveldealz` / `.secretflying` | How many articles to scrape |
| `done_limit` | Max `source_articles` entries considered "already done" |
| `scraping_overfetch_travel_dealz_min/max` | How many listing items to fetch before filtering |
| `llm.action` | `off`, `fill` (add missing fields), or `correct` (validate all fields) |
| `llm.max_items` | Max deals sent to LLM in one run |
| `duffel.calls` | How many Duffel benchmark searches to run |
| `duffel.origins` | Origin airports for Duffel (e.g. `["ZRH"]`) |
| `html.max_items` | Max deals shown in HTML output |
| `html.display_currency` | Currency for display (default: `CHF`) |

### Available modes

| Mode | Purpose |
|------|---------|
| `smoke_no_llm_1_each` | Quick smoke test: 1 deal, no LLM, no persist |
| `verify_td1_df1_persist_no_llm` | Verify: 1 TravelDealz deal, 1 Duffel call, persist, no LLM |
| `verify_td1_df1_persist_llm_fill` | Same + LLM fills missing fields |
| `verify_td1_df1_persist_llm_correct` | Same + LLM validates/corrects all fields |
| `verify_td2_df1_persist_no_llm` | 2 TravelDealz deals (1x .de + 1x .com), 1 Duffel call |
| `verify_td3_persist_no_llm` | 3 TravelDealz deals, no Duffel, no LLM |
| `verify_td3_next_persist_no_llm` | Next 3 TravelDealz deals (skipping already processed) |
| `full-no-llm` | Full run: 50 TravelDealz + 10 Duffel calls, persist, no LLM |
| `full` | Same + LLM enrichment (fill mode) |
| `duffel-only` | Duffel refresh only (25 calls) |

---

## Key Tables (Supabase)

| Table | Description |
|-------|-------------|
| `deals` | Unified deals from all sources (upsert on `booking_url`) |
| `source_articles` | Tracking table for processed article URLs per source |
| `subscribers` | Newsletter subscribers |

The `deals` table key columns:

| Column | Type | Notes |
|--------|------|-------|
| `booking_url` | text (unique) | Primary key equivalent; NULL for TravelDealz (uses `link`) |
| `skyscanner_url` | text | Skyscanner affiliate deep-link (CTA for all deals) |
| `source` | text | `travel-dealz`, `secretflying`, `duffel` |
| `title` | text | Deal headline |
| `price` | numeric | Price in CHF |
| `origin_iata` / `destination_iata` | text | 3-letter IATA codes |
| `airline` | text | Airline name |
| `aircraft` | text | Aircraft type (from Duffel; blank for TravelDealz if not in article) |
| `cabin_class` | text | Economy, Business, Premium Economy, First |
| `stops` | integer | 0 = nonstop |
| `flight_duration_minutes` | integer | Total flight duration |
| `flight_duration_display` | text | Formatted duration string (e.g. "14h 30m") |
| `travel_period_display` | text | Human-readable travel window |
| `baggage_included` | boolean | Whether checked baggage is included |
| `baggage_allowance_kg` | integer | Checked baggage kg allowance |
| `baggage_pieces_included` | integer | Number of checked bags |
| `image` | text | Unsplash destination image URL |
| `miles` | text | FFP miles estimate (e.g. "British Airways Avios · 3'149") |
| `tier` | text | `free` or `premium` |
| `scoring` | text | 0–100 score string (higher = better deal) |
| `created_at` | timestamptz | Auto-set on insert |

---

## Development Tips

**Check Supabase connectivity:**
```bash
python -c "from backend.database.supabase_db import test_connection; print(test_connection('deals'))"
```

**Inspect the 3 most recent deals:**
```bash
python -c "from backend.database.supabase_db import get_deals; import json; print(json.dumps(get_deals('deals', 3), indent=2))"
```

**Run a quick smoke test (no writes):**
```bash
python app.py pipeline --mode smoke_no_llm_1_each
```

**Run a persist + HTML test:**
```bash
python app.py deals-html --mode verify_td2_df1_persist_no_llm
```
