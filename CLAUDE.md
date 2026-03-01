# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository structure

Monorepo combining the React frontend and Python pipeline backend:

- `frontend/` — Vite + React + TypeScript website (Netlify deploys this)
- `backend/` — Python pipeline: scraping, scoring, enrichment, Supabase persistence
- `app.py` — Python launcher (entry point for the pipeline)
- `run_config.json` — All run modes and defaults for the pipeline
- `patterns.json` — Airport/region travel patterns for Amadeus
- `netlify.toml` — Tells Netlify to build from `frontend/`

Both the frontend and backend point to the **same Supabase project** (`wwoowwnjrepokmjgxhlw`).

---

## Frontend (`frontend/`)

### Setup & dev

```bash
cd frontend
npm install
npm run dev      # dev server at http://localhost:8080
npm run build    # production build
npm run lint     # ESLint
npm run preview  # preview production build
```

Env vars (in `frontend/.env.local`, gitignored):
```
VITE_SUPABASE_URL=https://wwoowwnjrepokmjgxhlw.supabase.co
VITE_SUPABASE_ANON_KEY=<anon_key>
```

Also set these in Netlify dashboard → Site settings → Environment variables.

### Architecture

- **Framework**: Vite + React 18 + TypeScript; path alias `@` → `src/`
- **UI**: Tailwind CSS + shadcn/ui (Radix UI) in `src/components/ui/`
- **State/data**: TanStack React Query
- **Backend**: Supabase client in `src/integrations/supabase/client.ts` (uses `VITE_SUPABASE_*` env vars); types in `src/integrations/supabase/types.ts`
- **Edge function**: `supabase/functions/send-confirmation-email` — sends double opt-in emails via Resend

**Routes** (`src/App.tsx`):

| Path | Page |
|------|------|
| `/` | Index (landing) |
| `/premium` | Premium upgrade |
| `/auth` | Auth |
| `/dashboard` | Subscriber dashboard |
| `/confirmed` | Post-confirmation landing |
| `/impressum`, `/datenschutz` | Legal pages |

**Supabase migrations** live in `frontend/supabase/migrations/`. To apply them, paste into the Supabase SQL Editor for project `wwoowwnjrepokmjgxhlw`.

---

## Backend (`backend/`)

### Setup

```powershell
# From repo root
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r backend/requirements.txt
```

Copy `.env.example` → `.env` at the repo root and fill in all values. Critically:
- `SUPABASE_URL` + `SUPABASE_ANON_KEY` → the unified Supabase project
- `SUPABASE_SERVICE_ROLE_KEY` → get from Supabase dashboard → project settings → API (backend uses this to bypass RLS when upserting deals)

### Running the pipeline

```bash
python app.py                          # uses defaults from run_config.json
python app.py deals-html --mode <mode> # end-to-end pipeline + HTML snippets
python app.py pipeline --mode <mode>   # pipeline only, prints summary
python app.py server --mode <mode>     # start FastAPI (uvicorn)
python app.py amadeus-refresh          # Amadeus gap-filler only
```

The active command and mode are set in `run_config.json` (`default_command` / `default_mode`).

### Architecture

```
app.py                          → thin entrypoint → backend/scripts/run.py
backend/
  scripts/run.py                → unified CLI launcher; reads run_config.json, dispatches subcommands
  scrapers/
    travel_dealz.py             → scrapes Travel-Dealz (.de / .com)
    secretflying.py             → scrapes SecretFlying
  services/
    deals_pipeline.py           → core pipeline: scrape → parse → enrich → score → persist
    deals_enrichment.py         → OpenAI enrichment (fill / correct modes)
    amadeus_service.py          → Amadeus flight price benchmarking
    openai_service.py           → OpenAI client with throttling/retry
    travel_dealz_article_parser.py / secretflying_article_parser.py
    baggage_format.py
  scoring/
    scoring.py                  → deal scoring (weighted: 45% price, 40% duration, 15% stops)
    miles_utils.py              → great-circle distance, FFP miles estimates
  database/
    supabase_db.py              → Supabase upserts; prefers SUPABASE_SERVICE_ROLE_KEY to bypass RLS
  scripts/
    run.py                      → main launcher
    fill_amadeus_gaps_from_patterns_v2.py  → Amadeus gap-filler subprocess
    generate_deals_html*.py     → HTML snippet writers
    (other operational scripts)
```

Pipeline flow: scraper → article parser → `deals_pipeline.py` (normalize → deterministic enrich → optional LLM enrich → score) → Supabase upsert (`deals` table) → HTML snippets.

---

## Netlify deployment

`netlify.toml` at repo root configures Netlify to:
- Build from `frontend/` directory
- Run `npm run build`
- Publish `frontend/dist/`

Required env vars in Netlify: `VITE_SUPABASE_URL`, `VITE_SUPABASE_ANON_KEY`.

---

## Core business rules (non-negotiable)

1. **Supabase is the single source of truth.** All subscriber state (`status`, `tier`), payment state, and consent lives in Supabase. Email providers and the frontend mirror state only.

2. **Double opt-in before any marketing.** Users must not receive marketing emails or be added to newsletter lists until `subscribers.status = 'active'`.

3. **Unsubscribe is a hard stop.** A user with `status = 'unsubscribed'` must never be re-added to any list by any automated flow.

4. **`mark_paid` is the only Premium grant path.** Premium status (`tier = 'premium'`) is set exclusively by the `mark_paid` backend function after `payments.status = 'paid'`.

5. **Tracking requires explicit consent.** Analytics and advertising scripts are loaded dynamically only after consent is granted (handled in `frontend/index.html`).

6. **Backend logic must be idempotent.** All backend functions must be safe to call multiple times.
