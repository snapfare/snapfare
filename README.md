# SnapFare

SnapFare delivers personalised flight deals from Switzerland to subscribers. Users sign up, set their travel preferences, and receive curated deals via a dashboard and AI agent. The system combines a Python scraping/enrichment pipeline with a React frontend backed by Supabase.

---

## Repository layout

```
snapfare/
├── frontend/          React + Vite + TypeScript (Netlify-deployed)
│   ├── src/           Pages, components, hooks, integrations
│   └── supabase/      Edge functions + DB migrations
├── backend/           Python pipeline: scrape → enrich → score → persist
├── app.py             Pipeline entry point
├── run_config.json    Pipeline run modes
├── patterns.json      Travel patterns for Duffel benchmarks
└── netlify.toml       Netlify build config (builds from frontend/)
```

Both `frontend/` and `backend/` target the same Supabase project.

---

## Quick start

### Frontend

```bash
cd frontend
npm install
npm run dev        # http://localhost:8080
```

Create `frontend/.env.local`:
```
VITE_SUPABASE_URL=https://<project>.supabase.co
VITE_SUPABASE_ANON_KEY=<anon_key>
```

### Backend pipeline

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
pip install -r backend/requirements.txt

cp .env.example .env        # fill in credentials
python app.py               # runs default mode from run_config.json
```

---

## Deployment

| Target | Method |
|--------|--------|
| Frontend | Netlify auto-deploys on push to `main` (config in `netlify.toml`) |
| Edge functions | `npx supabase functions deploy <name> --project-ref <ref>` |
| Backend pipeline | Run manually or schedule via cron |

Required Netlify env vars: `VITE_SUPABASE_URL`, `VITE_SUPABASE_ANON_KEY`.

---

## Docs

- [Frontend README](frontend/README.md) — setup, architecture, routes, components, edge functions
- [Backend README](backend/README.md) — pipeline architecture, CLI commands, run modes, env vars
