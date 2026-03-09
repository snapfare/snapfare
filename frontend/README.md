# SnapFare Frontend

React + Vite + TypeScript web application. Deployed to Netlify from the `main` branch.

---

## Setup & dev

```bash
cd frontend
npm install
npm run dev        # dev server → http://localhost:8080
npm run build      # production build → dist/
npm run lint       # ESLint
npm run preview    # preview production build locally
```

Create `frontend/.env.local` (gitignored):
```
VITE_SUPABASE_URL=https://<project>.supabase.co
VITE_SUPABASE_ANON_KEY=<anon_key>
```

---

## Architecture

| Layer | Detail |
|-------|--------|
| Framework | Vite + React 18 + TypeScript |
| UI | Tailwind CSS + shadcn/ui (Radix UI primitives) in `src/components/ui/` |
| State / data | TanStack React Query + Supabase JS client |
| Auth | Supabase Auth (email + password, password reset) |
| Path alias | `@` → `src/` |

---

## Routes

| Path | Page | Notes |
|------|------|-------|
| `/` | `Index` | Landing page, waitlist signup |
| `/auth` | `Auth` | Login / register / password reset |
| `/reset-password` | `ResetPassword` | Password update after email link |
| `/dashboard` | `Dashboard` | Authenticated user dashboard |
| `/premium` | `Premium` | Premium upgrade (TWINT) |
| `/confirmed` | `Confirmed` | Post-confirmation landing |
| `/impressum` | `Impressum` | Legal imprint |
| `/datenschutz` | `Datenschutz` | Privacy policy |
| `*` | `NotFound` | 404 fallback |

---

## Pages & components

### Key pages

- **`Dashboard.tsx`** — Main authenticated view. Shows personalised deals, travel preference settings panel, and the AI agent chat panel. Includes onboarding modal on first visit.
- **`Auth.tsx`** — Tabbed login/register form with password strength indicator and German error messages.
- **`Index.tsx`** — Public landing page with waitlist signup and 3 sample deals.

### Key components

| File | Purpose |
|------|---------|
| `DealCard.tsx` | Flight deal card with image, route, price, cabin badge, baggage, and booking CTA. Accepts `compact` prop for chat panel use. |
| `DealsChatPanel.tsx` | AI agent chat panel. 10 messages/day limit (24h window, DB-backed). Persists conversations per user. Agent has invisible memory of past queries. |
| `dashboard/OnboardingScreen.tsx` | 3-step onboarding modal: budget → cabin class → preferred regions. |

### Hooks

| File | Purpose |
|------|---------|
| `usePersonalizedDeals.ts` | Fetches deals from Supabase filtered by user preferences (origins, regions, price, cabin). |
| `usePremiumGate.ts` | Checks auth state and subscriber tier. Returns `isAuthenticated`, `isPremium`, `tier`, `user`. |

### Utilities (`src/lib/`)

| File | Purpose |
|------|---------|
| `utils.ts` | `cn()` (Tailwind merge), `getTimeGreeting()` (time-based German greeting), `getPasswordStrength()` (password strength check) |
| `regionMapping.ts` | Maps IATA airport codes to display regions (Europe, Asia/Pacific, Americas, etc.) |

---

## Supabase tables (used by frontend)

| Table | Purpose |
|-------|---------|
| `deals` | Curated flight deals from the backend pipeline |
| `subscribers` | Newsletter subscribers with status (`pending` / `active` / `unsubscribed`) and tier (`free` / `premium`) |
| `payments` | Payment records (`pending` / `paid`) |
| `user_preferences` | Per-user preferences: origins, regions, budget, cabin class, trip length, seasons |
| `agent_conversations` | AI agent chat history per user (used for 24h limit counting and agent memory) |
| `waitlist` | Landing page email waitlist |

---

## Edge functions (`supabase/functions/`)

| Function | Trigger | Purpose |
|----------|---------|---------|
| `send-confirmation-email` | HTTP POST | Generates double opt-in token, stores subscriber as `pending`, sends confirmation email via Resend |
| `confirm` | HTTP GET (link click) | Validates token, sets subscriber to `active` |
| `deals-chat` | HTTP POST (auth required) | OpenAI GPT-4o-mini agent with tool calling: queries `deals` table first, falls back to Duffel live search. Loads user preferences + query history as context. |
| `check-premium` | HTTP POST | Verifies subscriber premium status by email |

---

## Deployment

Netlify auto-deploys from `main` via the native Git integration (`netlify.toml` at repo root).

Required Netlify environment variables:

| Variable | Value |
|----------|-------|
| `VITE_SUPABASE_URL` | Supabase project URL |
| `VITE_SUPABASE_ANON_KEY` | Supabase anon public key |

To deploy edge functions manually:
```bash
npx supabase functions deploy deals-chat --project-ref <project_ref>
```

---

## Business rules (non-negotiable)

1. **Supabase is the single source of truth.** Subscriber state, payment state, and consent live in Supabase. Email providers and the frontend only mirror state.
2. **Double opt-in before any marketing.** Users must not receive marketing emails until `subscribers.status = 'active'`.
3. **Unsubscribe is a hard stop.** A `status = 'unsubscribed'` user must never be re-added to any list by any automated flow.
4. **`mark_paid` is the only Premium grant path.** `tier = 'premium'` is set only by the backend after `payments.status = 'paid'`. The frontend never grants Premium.
5. **Tracking requires explicit consent.** Analytics and ad pixels are loaded dynamically only after user consent (handled in `index.html`).
6. **Backend logic must be idempotent.** All edge functions must be safe to call multiple times without creating duplicates or unintended state changes.
