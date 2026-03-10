import { serve } from "https://deno.land/std@0.190.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import OpenAI from "https://esm.sh/openai@4";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
};

const supabaseAdmin = createClient(
  Deno.env.get("SUPABASE_URL")!,
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
);

const openai = new OpenAI({ apiKey: Deno.env.get("OPENAI_API_KEY")! });
const DUFFEL_API_KEY = Deno.env.get("DUFFEL_API_KEY")!;
console.log("[startup] SUPABASE_SERVICE_ROLE_KEY present:", !!Deno.env.get("SUPABASE_SERVICE_ROLE_KEY"));

// Fallback exchange rates (used if live fetch fails)
const FALLBACK_TO_CHF: Record<string, number> = {
  CHF: 1.0,
  EUR: 0.91,
  USD: 0.88,
  GBP: 1.12,
};

// Cached live rates (fetched once per cold start)
let liveToCHF: Record<string, number> | null = null;
let liveRatesFetchedAt = 0;
const RATE_CACHE_MS = 60 * 60 * 1000; // 1 hour

async function fetchLiveRates(): Promise<Record<string, number>> {
  const now = Date.now();
  if (liveToCHF && now - liveRatesFetchedAt < RATE_CACHE_MS) return liveToCHF;
  try {
    const res = await fetch("https://api.frankfurter.dev/v1/latest?from=CHF&to=EUR,USD,GBP");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    // API returns CHF→X rates, we need X→CHF (invert)
    liveToCHF = { CHF: 1.0 };
    for (const [cur, rate] of Object.entries(data.rates as Record<string, number>)) {
      liveToCHF[cur] = +(1 / rate).toFixed(4);
    }
    liveRatesFetchedAt = now;
    console.log("Live FX rates fetched:", liveToCHF);
    return liveToCHF;
  } catch (err) {
    console.warn("FX rate fetch failed, using fallbacks:", err);
    return FALLBACK_TO_CHF;
  }
}

// Synchronous accessor — uses cached live rates or fallback
function getToChf(): Record<string, number> {
  return liveToCHF ?? FALLBACK_TO_CHF;
}

interface Message {
  role: "user" | "assistant";
  content: string;
}

interface Deal {
  id: number;
  title: string;
  origin_iata: string;
  destination_iata: string;
  origin: string;
  destination: string;
  airline: string;
  cabin_class: string;
  price: number;
  currency: string;
  stops: number | null;
  flight_duration_display: string | null;
  baggage_included: boolean | null;
  baggage_allowance_kg: number | null;
  baggage_pieces_included: number | null;
  aircraft: string | null;
  image: string | null;
  tier: string;
  travel_period_display: string | null;
  skyscanner_url: string | null;
  miles: string | null;
  scoring: string | null;
}

// Tool: fetch deals from Supabase
async function getDeals(params: {
  origin_iata?: string | string[];
  destination_iata?: string | string[];
  destination_text?: string;
  max_price?: number;
  cabin_class?: string;
  tier?: string;
  limit?: number;
}): Promise<Deal[]> {
  let query = supabaseAdmin
    .from("deals")
    .select(
      "id,title,origin_iata,destination_iata,origin,destination,airline,cabin_class,price,currency,stops,flight_duration_display,baggage_included,baggage_allowance_kg,baggage_pieces_included,aircraft,image,tier,travel_period_display,skyscanner_url,miles,scoring"
    )
    .order("scoring", { ascending: false })
    .limit(params.limit ?? 5);

  if (params.origin_iata) {
    const origins = Array.isArray(params.origin_iata)
      ? params.origin_iata
      : [params.origin_iata];
    query = query.in("origin_iata", origins);
  }

  if (params.destination_iata) {
    const dests = Array.isArray(params.destination_iata)
      ? params.destination_iata
      : [params.destination_iata];
    query = query.in("destination_iata", dests);
  }

  // Fuzzy text search on destination city/country name (used when no exact IATA known)
  if (params.destination_text && !params.destination_iata) {
    query = query.ilike("destination", `%${params.destination_text}%`);
  }

  if (params.max_price) {
    query = query.lte("price", params.max_price);
  }

  if (params.cabin_class) {
    query = query.ilike("cabin_class", `%${params.cabin_class}%`);
  }

  if (params.tier) {
    query = query.eq("tier", params.tier);
  }

  const { data, error } = await query;
  if (error) {
    console.error("getDeals error:", error);
    return [];
  }
  return (data as Deal[]) ?? [];
}

function buildSkyscannerUrl(
  origin: string,
  destination: string,
  departureDate: string,
  returnDate?: string,
  cabinClass?: string
): string {
  const toSkyDate = (d: string) => d.replace(/-/g, "").slice(2); // 2026-04-15 → 260415
  const cabinMap: Record<string, string> = {
    economy: "economy", business: "business", first: "first", premium_economy: "premiumeconomy",
  };
  const cabin = cabinMap[(cabinClass ?? "economy").toLowerCase()] ?? "economy";
  const path = returnDate
    ? `${origin.toLowerCase()}/${destination.toLowerCase()}/${toSkyDate(departureDate)}/${toSkyDate(returnDate)}/`
    : `${origin.toLowerCase()}/${destination.toLowerCase()}/${toSkyDate(departureDate)}/`;
  return `https://www.skyscanner.ch/transport/fluge/${path}?adultsv2=2&cabinclass=${cabin}`;
}

// Parse ISO 8601 duration (e.g. "PT14H30M") into human-readable German string
function parseDuration(iso: string | undefined): string | null {
  if (!iso) return null;
  const m = iso.match(/PT(?:(\d+)H)?(?:(\d+)M)?/);
  if (!m) return null;
  const h = parseInt(m[1] ?? "0");
  const min = parseInt(m[2] ?? "0");
  if (h > 0 && min > 0) return `${h} Std ${min} Min`;
  if (h > 0) return `${h} Std`;
  return `${min} Min`;
}

// Tool: search Duffel API for live flight prices.
// Results are saved to chat_deals (user-scoped, separate from public deals table).
async function searchDuffel(
  params: {
    origin: string;
    destination: string;
    departure_date: string;
    return_date?: string;
    cabin_class?: string;
  },
  userId: string
): Promise<{ summary: string; deals: Deal[] }> {
  if (!DUFFEL_API_KEY) return { summary: "Duffel API not configured", deals: [] };

  try {
    const slices: object[] = [
      {
        origin: params.origin,
        destination: params.destination,
        departure_date: params.departure_date,
      },
    ];

    if (params.return_date) {
      slices.push({
        origin: params.destination,
        destination: params.origin,
        departure_date: params.return_date,
      });
    }

    const body = {
      data: {
        slices,
        passengers: [{ type: "adult" }],
        cabin_class: params.cabin_class ?? "economy",
        max_connections: 1,
      },
    };

    const response = await fetch("https://api.duffel.com/air/offer_requests", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${DUFFEL_API_KEY}`,
        "Content-Type": "application/json",
        "Duffel-Version": "v2",
        Accept: "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      return { summary: `Duffel search failed: ${response.status}`, deals: [] };
    }

    const json = await response.json();
    const offers = json.data?.offers ?? [];

    if (offers.length === 0) {
      return { summary: "No flights found for this route/date combination.", deals: [] };
    }

    // Top 1 cheapest option
    // deno-lint-ignore no-explicit-any
    const top1: any[] = offers
      .sort((a: { total_amount: string }, b: { total_amount: string }) =>
        parseFloat(a.total_amount) - parseFloat(b.total_amount)
      )
      .slice(0, 1);

    const rates = getToChf();

    // Build rows to insert into chat_deals (user-scoped, never shown in public deals section)
    const rows = top1.map((o) => {
      const firstSlice = o.slices?.[0];
      const firstSeg = firstSlice?.segments?.[0];
      const carrier = firstSeg?.operating_carrier?.iata_code ?? "?";
      const rate = rates[o.total_currency] ?? 1.0;
      const priceChf = Math.round(parseFloat(o.total_amount) * rate);

      // Extract enrichment fields from Duffel offer
      const stopsCount = firstSlice ? Math.max(0, (firstSlice.segments?.length ?? 1) - 1) : null;
      const durationDisplay = parseDuration(firstSlice?.duration);
      const aircraftCode: string | null = firstSeg?.aircraft?.iata_code ?? null;

      // Baggage: checked bags from first passenger
      const baggages: { type: string; quantity: number }[] = o.passengers?.[0]?.baggages ?? [];
      const checkedBag = baggages.find((b) => b.type === "checked");
      const baggageIncluded: boolean | null = baggages.length > 0 ? (checkedBag != null && checkedBag.quantity > 0) : null;
      const baggagePieces: number | null = checkedBag?.quantity ?? null;
      // Duffel doesn't give kg allowance directly in offer_requests — leave null
      const baggageKg: null = null;

      return {
        user_id: userId,
        title: `${carrier}: ${params.origin}→${params.destination}`,
        price: priceChf,
        currency: "CHF",
        origin_iata: params.origin,
        destination_iata: params.destination,
        origin: params.origin,
        destination: params.destination,
        airline: carrier,
        cabin_class: params.cabin_class ?? "economy",
        travel_period_display: params.return_date
          ? `${params.departure_date} – ${params.return_date}`
          : params.departure_date,
        skyscanner_url: buildSkyscannerUrl(
          params.origin,
          params.destination,
          params.departure_date,
          params.return_date,
          params.cabin_class
        ),
        stops: stopsCount,
        flight_duration_display: durationDisplay,
        aircraft: aircraftCode,
        baggage_included: baggageIncluded,
        baggage_pieces_included: baggagePieces,
        baggage_allowance_kg: baggageKg,
      };
    });

    // Insert into chat_deals and get back the assigned IDs
    console.log(`[search_duffel] inserting ${rows.length} rows for user ${userId}`);
    const { data: inserted, error } = await supabaseAdmin
      .from("chat_deals")
      .insert(rows)
      .select("id,title,origin_iata,destination_iata,origin,destination,airline,cabin_class,price,currency,travel_period_display,skyscanner_url,stops,flight_duration_display,aircraft,baggage_included,baggage_pieces_included,baggage_allowance_kg");

    if (error || !inserted || inserted.length === 0) {
      console.error("[search_duffel] chat_deals insert error:", {
        message: error?.message,
        code: error?.code,
        details: error?.details,
        hint: error?.hint,
      });
      // Fallback: build in-memory deals with synthetic IDs so the chat still works
      const fallbackDeals: Deal[] = rows.map((row, i) => ({
        id: 900000 + i,
        title: row.title,
        origin_iata: row.origin_iata,
        destination_iata: row.destination_iata,
        origin: row.origin,
        destination: row.destination,
        airline: row.airline,
        cabin_class: row.cabin_class,
        price: row.price,
        currency: "CHF",
        stops: row.stops ?? null,
        flight_duration_display: row.flight_duration_display ?? null,
        baggage_included: row.baggage_included ?? null,
        baggage_allowance_kg: row.baggage_allowance_kg ?? null,
        baggage_pieces_included: row.baggage_pieces_included ?? null,
        aircraft: row.aircraft ?? null,
        image: null,
        tier: "free",
        travel_period_display: row.travel_period_display ?? null,
        skyscanner_url: row.skyscanner_url ?? null,
        miles: null,
        scoring: null,
      }));
      const fallbackSummary =
        `${fallbackDeals.length} Flug gefunden (${params.origin}→${params.destination}, ${params.departure_date}): ` +
        fallbackDeals.map((d) => `${d.airline} CHF ${d.price} (ID ${d.id})`).join(", ") +
        `. Nenne den CHF-Preis exakt so wie er hier steht — nie umrechnen, nie eine andere Währung.`;
      return { summary: fallbackSummary, deals: fallbackDeals };
    }

    console.log(`[search_duffel] inserted IDs: ${inserted.map((r: Record<string, unknown>) => r.id).join(",")}`);

    // Map inserted rows to Deal shape
    const deals: Deal[] = inserted.map((row: Record<string, unknown>) => ({
      id: row.id as number,
      title: row.title as string,
      origin_iata: row.origin_iata as string,
      destination_iata: row.destination_iata as string,
      origin: row.origin as string,
      destination: row.destination as string,
      airline: row.airline as string,
      cabin_class: row.cabin_class as string,
      price: row.price as number,
      currency: row.currency as string,
      stops: row.stops as number | null,
      flight_duration_display: row.flight_duration_display as string | null,
      baggage_included: row.baggage_included as boolean | null,
      baggage_allowance_kg: row.baggage_allowance_kg as number | null,
      baggage_pieces_included: row.baggage_pieces_included as number | null,
      aircraft: row.aircraft as string | null,
      image: null,
      tier: "free",
      travel_period_display: row.travel_period_display as string | null,
      skyscanner_url: row.skyscanner_url as string | null,
      miles: null,
      scoring: null,
    }));

    const summary =
      `${deals.length} Flug gefunden (${params.origin}→${params.destination}, ${params.departure_date}): ` +
      deals.map((d) => {
        const parts = [`${d.airline} CHF ${d.price} (ID ${d.id})`];
        if (d.stops !== null) parts.push(d.stops === 0 ? "Nonstop" : `${d.stops} Stopp`);
        if (d.flight_duration_display) parts.push(d.flight_duration_display);
        if (d.baggage_included === true && d.baggage_pieces_included) parts.push(`${d.baggage_pieces_included}× Gepäck inklusive`);
        else if (d.baggage_included === false) parts.push("kein Aufgabegepäck");
        return parts.join(" | ");
      }).join(", ") +
      `. Nenne den CHF-Preis exakt so wie er hier steht — nie umrechnen, nie eine andere Währung.`;

    return { summary, deals };
  } catch (err) {
    console.error("Duffel search error:", err);
    return { summary: "Could not fetch live prices at this time.", deals: [] };
  }
}

// Post-process GPT text: deterministically fix currency issues.
// GPT's training data says Duffel returns EUR, so it ignores all prompt
// instructions. We fix the output instead of fighting the model.
function sanitizeCurrency(text: string, deals: Deal[]): string {
  // Build a map of approximate EUR amounts → correct CHF amounts.
  // GPT may output the original EUR amount (before our conversion) or our CHF amount mislabeled as EUR.
  const eurToChf = new Map<number, number>();
  for (const d of deals) {
    // d.price is already in CHF. GPT might output it as "X EUR".
    eurToChf.set(d.price, d.price); // CHF amount mislabeled as EUR → same CHF
    // Also map the approximate original EUR amount (reverse of our conversion)
    const eurRate = getToChf().EUR ?? 0.91;
    const approxEur = Math.round(d.price / eurRate);
    eurToChf.set(approxEur, d.price);
  }

  let result = text;

  // Remove "(ca. XXX CHF)" or "(ca. CHF XXX)" parenthetical conversions
  result = result.replace(/\s*\(ca\.\s*(?:CHF\s*)?\d[\d'.]*\s*(?:CHF)?\s*\)/gi, "");

  // Replace "XXX.XX EUR" or "XXX EUR" with "CHF YYY" using closest match
  result = result.replace(/(\d[\d'.]*)\s*EUR/gi, (match, numStr) => {
    const num = Math.round(parseFloat(numStr.replace(/'/g, "")));
    // Find closest match in our map
    let bestChf = num; // fallback: use same number
    let bestDist = Infinity;
    for (const [eurVal, chfVal] of eurToChf) {
      const dist = Math.abs(eurVal - num);
      if (dist < bestDist) {
        bestDist = dist;
        bestChf = chfVal;
      }
    }
    // Only substitute if reasonably close (within 10%)
    if (bestDist <= num * 0.1) {
      return `CHF ${bestChf}`;
    }
    return `CHF ${num}`; // just swap label
  });

  // Also fix "EUR XXX" format (EUR before number)
  result = result.replace(/EUR\s*(\d[\d'.]*)/gi, (match, numStr) => {
    const num = Math.round(parseFloat(numStr.replace(/'/g, "")));
    let bestChf = num;
    let bestDist = Infinity;
    for (const [eurVal, chfVal] of eurToChf) {
      const dist = Math.abs(eurVal - num);
      if (dist < bestDist) {
        bestDist = dist;
        bestChf = chfVal;
      }
    }
    if (bestDist <= num * 0.1) {
      return `CHF ${bestChf}`;
    }
    return `CHF ${num}`;
  });

  return result;
}

const TOOLS: OpenAI.Chat.Completions.ChatCompletionTool[] = [
  {
    type: "function",
    function: {
      name: "get_deals",
      description:
        "Fetch current flight deals from the SnapFare database. ALWAYS call this first before search_duffel — even for specific route requests. Use destination_iata for exact routes, destination_text for fuzzy city/country/region searches.",
      parameters: {
        type: "object",
        properties: {
          origin_iata: {
            type: "string",
            description: "Origin airport IATA code (e.g. ZRH, GVA, BSL)",
          },
          destination_iata: {
            type: "string",
            description: "Destination airport IATA code (e.g. BKK, JFK, DXB). Use when user specifies a specific destination.",
          },
          destination_text: {
            type: "string",
            description: "Fuzzy text search on destination city or country name (e.g. 'Thailand', 'New York', 'Japan'). Use when no exact IATA code is known, or for broad region searches.",
          },
          max_price: {
            type: "number",
            description: "Maximum price in CHF",
          },
          cabin_class: {
            type: "string",
            description: "Cabin class: Economy, Business, or First",
          },
          tier: {
            type: "string",
            description: "Deal tier: free or premium",
          },
          limit: {
            type: "number",
            description: "Number of deals to return (default 5, max 10)",
          },
        },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "search_duffel",
      description:
        "Search for live flight prices via the Duffel API. Returns the cheapest option. Use when: (a) get_deals returned no deal for the user's destination, or (b) deals were shown but user wants to book/get more options for specific dates. Always ask the user for departure_date (and optionally return_date, cabin_class) before calling this tool if not already known.",
      parameters: {
        type: "object",
        properties: {
          origin: {
            type: "string",
            description: "Origin airport IATA code",
          },
          destination: {
            type: "string",
            description: "Destination airport IATA code",
          },
          departure_date: {
            type: "string",
            description: "Departure date in YYYY-MM-DD format",
          },
          return_date: {
            type: "string",
            description: "Return date in YYYY-MM-DD format (optional, for round trips)",
          },
          cabin_class: {
            type: "string",
            description: "Cabin class: economy, business, or first",
          },
        },
        required: ["origin", "destination", "departure_date"],
      },
    },
  },
];

const SYSTEM_PROMPT = `Du bist der SnapFare Agent — ein präziser Schweizer Flugdeal-Experte. Direkt, nüchtern, mit echtem Urteil für gute Deals.

CHARAKTER
- Direkt und selbstbewusst — keine Füllwörter, kein "Natürlich!", kein "Sehr gerne!", kein "Gerne helfe ich dir!"
- Ein trockener Witz ist erlaubt, Floskeln nicht
- Schweizer Nüchternheit mit echter Begeisterung wenn ein Deal wirklich gut ist

DEAL-QUALITÄT (wichtig)
- Jeder Deal hat einen Score von 0–100: höher = besserer Deal (Preis, Dauer, Direktflug gewichtet)
- Score über 60: echter Schnäppchen-Alarm — das explizit erwähnen
- Score unter 40: eher schwach — zeigen wenn nichts Besseres da ist, aber neutral kommentieren
- Sortiere Antworten immer nach Score (bester Deal zuerst)

ANTWORT-FORMAT
Wenn Deals gefunden: 1–2 Sätze. Nenne den günstigsten CHF-Preis exakt aus dem Tool-Ergebnis — nie umrechnen, nie EUR, nie USD. Wenn das Tool-Ergebnis Stops, Dauer oder Gepäckinfo enthält, erwähne diese kurz in einer Zeile (z.B. "Nonstop, 8 Std 30 Min, 1× Gepäck inklusive"). Die Karten zeigen alle Details.
Wenn keine Deals: Kurz erklären warum, dann konkret vorschlagen: anderes Budget, anderen Abflughafen (z.B. GVA statt ZRH), oder flexiblere Daten — max. 2 Sätze.

ARBEITSWEISE

SCHRITT 1 — get_deals (IMMER zuerst, für jede Anfrage)
→ Rufe get_deals auf, bevor du irgendetwas anderes tust
→ Allgemeine Anfragen ("zeig Deals", "was gibt's Günstiges"): breite Suche, keine engen Filter — zeig die besten Deals unabhängig von den Nutzer-Präferenzen
→ Spezifische Route ("nach Tokio", "ZRH→BKK"): origin_iata/destination_iata setzen
→ Region oder Land: destination_text nutzen (z.B. "Thailand", "Japan", "Karibik")
→ Nutzer-Präferenzen sind Kontext, keine Pflichtfilter — setze sie nur wenn es Sinn macht

SCHRITT 2 — Live-Preis anbieten (wenn kein Deal gefunden)
→ Wenn get_deals keinen Deal für die gewünschte Destination liefert: NICHT nur Alternativen vorschlagen
→ Stattdessen SOFORT aktiv anbieten: "Aktuell kein kuratierter Deal — ich kann dir aber einen aktuellen Live-Preis suchen. Kein Sonderangebot, aber ein echter aktueller Preis. Wann möchtest du fliegen?"
→ In EINER Frage alle fehlenden Infos erfragen: Abflugdatum, Rückflugdatum (Hin- und Rückflug?), Kabine falls nicht Economy
→ Sobald Datum bekannt: search_duffel aufrufen — kein weiteres Nachfragen

SCHRITT 2b — Nutzer will mehr (obwohl Deals vorhanden)
→ Deals wurden gezeigt aber Nutzer will konkret buchen oder genaue Preise für ein Datum
→ Gleich wie oben: fehlende Details in einer Frage erfragen, dann search_duffel aufrufen

SCHRITT 3 — Fragen zu Duffel-Ergebnissen
→ Wenn search_duffel bereits ausgeführt wurde: Fragen zu Stops, Dauer, Gepäck aus dem Tool-Ergebnis beantworten — nicht erneut suchen
→ Stops, Dauer und Gepäckinfo aus dem Ergebnis nennen wenn vorhanden

REGELN
- Immer auf Deutsch antworten
- Preise NUR in CHF — niemals EUR, USD oder andere Währungen
- Keine Auflistung von Route/Airline/Kabine/Dauer im Text — das zeigen die Karten
- Skyscanner-Links nie im Text erwähnen — sie sind in den Karten
- Meilen nur wenn Nutzer fragt oder der Wert aussergewöhnlich hoch ist
- Aktuelles Datum: ${new Date().toLocaleDateString("de-CH")}

DEAL-KARTEN: Jede Antwort mit diesem Tag abschliessen. Alle Deal-IDs aus dem Tool-Ergebnis eintragen:
[DEALS: id1,id2,id3]
Wenn keine Deals: [DEALS:]`;

const handler = async (req: Request): Promise<Response> => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    // Pre-fetch live FX rates (cached, non-blocking on failure)
    await fetchLiveRates();

    // Require authentication
    const authHeader = req.headers.get("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return new Response(
        JSON.stringify({ error: "Unauthorized" }),
        { status: 401, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    const token = authHeader.replace("Bearer ", "");
    const { data: { user }, error: authError } = await supabaseAdmin.auth.getUser(token);
    if (authError || !user) {
      return new Response(
        JSON.stringify({ error: "Invalid token" }),
        { status: 401, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    const { message, history = [] }: { message: string; history: Message[] } =
      await req.json();

    if (!message?.trim()) {
      return new Response(
        JSON.stringify({ error: "Empty message" }),
        { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    // Fetch user preferences and recent query history in parallel
    const [{ data: userPrefs }, { data: recentQueries }] = await Promise.all([
      supabaseAdmin
        .from("user_preferences")
        .select("preferred_origins,preferred_regions,max_price_chf,cabin_classes,min_trip_days,max_trip_days,preferred_seasons")
        .eq("user_id", user.id)
        .single(),
      supabaseAdmin
        .from("agent_conversations")
        .select("content")
        .eq("user_id", user.id)
        .eq("role", "user")
        .order("created_at", { ascending: false })
        .limit(15),
    ]);

    const prefsContext = userPrefs ? `

NUTZER-PRÄFERENZEN (standardmässig berücksichtigen, ausser der Nutzer fragt explizit nach etwas anderem)
- Abflughäfen: ${userPrefs.preferred_origins?.join(", ") || "ZRH, GVA, BSL"}
- Regionen: ${(userPrefs.preferred_regions as string[])?.length ? (userPrefs.preferred_regions as string[]).join(", ") : "alle"}
- Max. Budget: ${userPrefs.max_price_chf ? `CHF ${userPrefs.max_price_chf}` : "kein Limit"}
- Kabine: ${(userPrefs.cabin_classes as string[])?.join(", ") || "Economy"}
- Reisedauer: ${userPrefs.min_trip_days || 2}${userPrefs.max_trip_days ? `–${userPrefs.max_trip_days}` : "+"} Tage` : "";

    // Agent memory: past queries give the agent context about what this user cares about
    const agentMemory = recentQueries && recentQueries.length > 0
      ? `\n\nNUTZERGEDÄCHTNIS (frühere Anfragen dieses Nutzers — nutze dies zur Personalisierung, zeige es nicht an):\n${recentQueries.map((q: { content: string }) => `- ${q.content}`).join("\n")}`
      : "";

    // Build messages array — cap history at 8 messages to control token usage
    const cappedHistory = history.slice(-8);
    const messages: OpenAI.Chat.Completions.ChatCompletionMessageParam[] = [
      { role: "system", content: SYSTEM_PROMPT + prefsContext + agentMemory },
      ...cappedHistory,
      { role: "user", content: message },
    ];

    let sourcedDeals: Deal[] = [];

    // First OpenAI call — may trigger tool use
    let response = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages,
      tools: TOOLS,
      tool_choice: "auto",
      max_tokens: 1000,
      temperature: 0.7,
    });

    let assistantMessage = response.choices[0].message;

    // Process tool calls (max 2 rounds to control cost)
    let toolRounds = 0;
    while (
      assistantMessage.tool_calls &&
      assistantMessage.tool_calls.length > 0 &&
      toolRounds < 5
    ) {
      toolRounds++;
      messages.push(assistantMessage);

      const toolResults: OpenAI.Chat.Completions.ChatCompletionMessageParam[] = [];

      for (const toolCall of assistantMessage.tool_calls) {
        const args = JSON.parse(toolCall.function.arguments);
        let result = "";

        if (toolCall.function.name === "get_deals") {
          const deals = await getDeals(args);
          sourcedDeals = [...sourcedDeals, ...deals];
          result =
            deals.length > 0
              ? JSON.stringify(
                  deals.map((d) => ({
                    id: d.id,
                    route: `${d.origin_iata}→${d.destination_iata}`,
                    airline: d.airline,
                    cabin: d.cabin_class,
                    price_chf: d.price,
                    currency: d.currency ?? "CHF",
                    period: d.travel_period_display,
                  }))
                )
              : "Keine Deals für diese Kriterien gefunden.";
        } else if (toolCall.function.name === "search_duffel") {
          const duffelResult = await searchDuffel(args, user.id);
          sourcedDeals = [...sourcedDeals, ...duffelResult.deals];
          result = duffelResult.summary;
        }

        toolResults.push({
          role: "tool",
          tool_call_id: toolCall.id,
          content: result,
        });
      }

      messages.push(...toolResults);

      // Second OpenAI call with tool results
      response = await openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages,
        max_tokens: 800,
        temperature: 0.7,
      });

      assistantMessage = response.choices[0].message;
    }

    // Deduplicate sourced deals by id
    const uniqueDeals = Array.from(
      new Map(sourcedDeals.map((d) => [d.id, d])).values()
    );

    // Parse [DEALS: id1,id2,...] tag from GPT response to align cards with text
    let responseText = assistantMessage.content ?? "";
    let selectedDeals = uniqueDeals;
    const dealTagMatch = responseText.match(/\[DEALS:\s*([\d,\s-]*)\]/);
    if (dealTagMatch) {
      const rawIds = dealTagMatch[1].trim();
      if (rawIds) {
        const selectedIds = rawIds.split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => !isNaN(n));
        selectedDeals = selectedIds
          .map((id) => uniqueDeals.find((d) => d.id === id))
          .filter((d): d is Deal => d !== undefined);
      } else {
        selectedDeals = [];
      }
      responseText = responseText.replace(/\n?\[DEALS:[\d,\s-]*\]/, "").trimEnd();
    }

    // Deterministic fix: replace any EUR references with correct CHF values
    responseText = sanitizeCurrency(responseText, selectedDeals);

    return new Response(
      JSON.stringify({
        response: responseText,
        deals: selectedDeals,
      }),
      { status: 200, headers: { "Content-Type": "application/json", ...corsHeaders } }
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("deals-chat error:", message);
    return new Response(
      JSON.stringify({ error: message }),
      { status: 500, headers: { "Content-Type": "application/json", ...corsHeaders } }
    );
  }
};

serve(handler);
