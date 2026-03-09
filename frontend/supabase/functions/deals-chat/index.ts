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
  max_price?: number;
  cabin_class?: string;
  tier?: string;
  limit?: number;
}): Promise<Deal[]> {
  let query = supabaseAdmin
    .from("deals")
    .select(
      "id,title,origin_iata,destination_iata,origin,destination,airline,cabin_class,price,currency,stops,flight_duration_display,baggage_included,baggage_allowance_kg,image,tier,travel_period_display,skyscanner_url,miles,scoring"
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
    const top1: { total_amount: string; total_currency: string; slices: { segments: { operating_carrier: { iata_code: string } }[] }[] }[] = offers
      .sort((a: { total_amount: string }, b: { total_amount: string }) =>
        parseFloat(a.total_amount) - parseFloat(b.total_amount)
      )
      .slice(0, 1);

    const rates = getToChf();

    // Build rows to insert into chat_deals (user-scoped, never shown in public deals section)
    const rows = top1.map((o) => {
      const carrier = o.slices?.[0]?.segments?.[0]?.operating_carrier?.iata_code ?? "?";
      const rate = rates[o.total_currency] ?? 1.0;
      const priceChf = Math.round(parseFloat(o.total_amount) * rate);
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
      };
    });

    // Insert into chat_deals and get back the assigned IDs
    console.log(`[search_duffel] inserting ${rows.length} rows for user ${userId}`);
    const { data: inserted, error } = await supabaseAdmin
      .from("chat_deals")
      .insert(rows)
      .select("id,title,origin_iata,destination_iata,origin,destination,airline,cabin_class,price,currency,travel_period_display,skyscanner_url");

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
        stops: null,
        flight_duration_display: null,
        baggage_included: null,
        baggage_allowance_kg: null,
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

    // Map inserted rows to Deal shape (fields not in chat_deals default to null)
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
      stops: null,
      flight_duration_display: null,
      baggage_included: null,
      baggage_allowance_kg: null,
      image: null,
      tier: "free",
      travel_period_display: row.travel_period_display as string | null,
      skyscanner_url: row.skyscanner_url as string | null,
      miles: null,
      scoring: null,
    }));

    const summary =
      `${deals.length} Flug gefunden (${params.origin}→${params.destination}, ${params.departure_date}): ` +
      deals.map((d) => `${d.airline} CHF ${d.price} (ID ${d.id})`).join(", ") +
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
        "Fetch current flight deals from the SnapFare database. Use this to find deals matching user criteria.",
      parameters: {
        type: "object",
        properties: {
          origin_iata: {
            type: "string",
            description: "Origin airport IATA code (e.g. ZRH, GVA, BSL)",
          },
          destination_iata: {
            type: "string",
            description: "Destination airport IATA code (e.g. BKK, JFK, DXB)",
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
        "Search for live flight prices via the Duffel API. Use when the user asks about a specific route and date that may not be in the deals database.",
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

const SYSTEM_PROMPT = `Du bist der SnapFare Agent — ein schlagfertiger, humorvoller Schweizer Flugdeal-Experte mit Charme und Präzision. Denk an einen gut informierten Freund, der zufällig alle Flugpreise kennt und gerne damit angibt.

CHARAKTER
- Humorvoll aber professionell: ein trockener Witz ist erlaubt, Floskeln nicht
- Direkt und selbstbewusst — keine Füllwörter, kein "Natürlich!", kein "Sehr gerne!"
- Schweizer Nüchternheit trifft auf echte Begeisterung für gute Deals

ANTWORT-FORMAT:
Wenn du Deals gefunden hast: Schreibe 1-2 kurze Sätze. Nenne den günstigsten CHF-Preis exakt so wie er im Tool-Ergebnis steht (als price_chf oder "CHF X") — nie umrechnen, nie EUR, nie USD, nie eine andere Währung. Die Karten zeigen alle weiteren Details.
Wenn keine Deals gefunden: Freier Text, kurz.

ARBEITSWEISE — wichtig, immer so vorgehen:

1. ALLGEMEINE DEAL-ANFRAGEN ("zeig mir Deals", "was gibt's Günstiges"):
   → Nutze get_deals mit den Nutzer-Präferenzen als Standardparameter
   → Zeige die besten Treffer; erwähne kurz, wenn du auch ausserhalb der Präferenzen suchen kannst

2. SPEZIFISCHE FLUGANFRAGEN ("ich will nach Kenya", "Flug nach Tokio im April"):
   → Kläre zuerst fehlende Infos ab — in einer einzigen Frage, kompakt:
      • Abflughafen (falls nicht aus Präferenzen eindeutig)
      • Zielflughafen als IATA-Code (z.B. NBO für Nairobi)
      • Reisedaten (Hin- und Rückflug)
   → Sobald alle Infos da sind: search_duffel aufrufen
   → Bei flexiblen Daten (z.B. "irgendwann im Mai"): bis zu 3 verschiedene Daten mit je einem search_duffel-Aufruf durchsuchen und die Ergebnisse vergleichen
   → Die Skyscanner-Buchungslinks erscheinen automatisch in den Deal-Karten — nie manuell verlinken

3. PRÄFERENZEN sind Standardwerte, keine Einschränkungen:
   → Wenn der Nutzer explizit nach etwas anderem fragt (andere Route, höheres Budget, andere Kabine), ignoriere die Präferenzen für diese Anfrage

REGELN
- Antworte immer auf Deutsch
- Preise NUR in CHF nennen — NIEMALS EUR, NIEMALS USD, NIEMALS eine andere Währung. Die Tool-Ergebnisse liefern dir CHF-Beträge, verwende diese exakt.
- Keine detaillierte Auflistung von Route/Airline/Kabine/Dauer/Gepäck im Text — das zeigen die Karten.
- Erwähne NIEMALS Skyscanner-Links im Text — die Buchungslinks sind direkt in den Deal-Karten sichtbar
- Meilen nur erwähnen wenn der Nutzer danach fragt oder es besonders attraktiv ist
- Kein "Klicke hier", kein "Weitere Infos findest du..." — kein Link-Spam
- Aktuelles Datum: ${new Date().toLocaleDateString("de-CH")}

DEAL-KARTEN: Schliesse jede Antwort mit einem Tag ab. Setze ALLE Deal-IDs ein, die du vom Tool erhalten hast:
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

    // Fetch user preferences to personalise the system prompt
    const { data: userPrefs } = await supabaseAdmin
      .from("user_preferences")
      .select("preferred_origins,preferred_regions,max_price_chf,cabin_classes,min_trip_days,max_trip_days,preferred_seasons")
      .eq("user_id", user.id)
      .single();

    const prefsContext = userPrefs ? `

NUTZER-PRÄFERENZEN (standardmässig berücksichtigen, ausser der Nutzer fragt explizit nach etwas anderem)
- Abflughäfen: ${userPrefs.preferred_origins?.join(", ") || "ZRH, GVA, BSL"}
- Regionen: ${(userPrefs.preferred_regions as string[])?.length ? (userPrefs.preferred_regions as string[]).join(", ") : "alle"}
- Max. Budget: ${userPrefs.max_price_chf ? `CHF ${userPrefs.max_price_chf}` : "kein Limit"}
- Kabine: ${(userPrefs.cabin_classes as string[])?.join(", ") || "Economy"}
- Reisedauer: ${userPrefs.min_trip_days || 2}${userPrefs.max_trip_days ? `–${userPrefs.max_trip_days}` : "+"} Tage` : "";

    // Build messages array — cap history at 8 messages to control token usage
    const cappedHistory = history.slice(-8);
    const messages: OpenAI.Chat.Completions.ChatCompletionMessageParam[] = [
      { role: "system", content: SYSTEM_PROMPT + prefsContext },
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
      max_tokens: 800,
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
