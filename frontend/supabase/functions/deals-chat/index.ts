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

// Tool: search Duffel API for live flight prices
async function searchDuffel(params: {
  origin: string;
  destination: string;
  departure_date: string;
  return_date?: string;
  cabin_class?: string;
}): Promise<{ summary: string; deals: Deal[] }> {
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

    // Top 3 cheapest options
    const top3: { total_amount: string; total_currency: string; slices: { segments: { operating_carrier: { iata_code: string } }[] }[] }[] = offers
      .sort((a: { total_amount: string }, b: { total_amount: string }) =>
        parseFloat(a.total_amount) - parseFloat(b.total_amount)
      )
      .slice(0, 3);

    const deals: Deal[] = top3.map((o, i) => {
      const carrier = o.slices?.[0]?.segments?.[0]?.operating_carrier?.iata_code ?? "?";
      const price = parseFloat(o.total_amount);
      return {
        id: -(i + 1),
        title: `${carrier}: ${params.origin}→${params.destination}`,
        origin_iata: params.origin,
        destination_iata: params.destination,
        origin: params.origin,
        destination: params.destination,
        airline: carrier,
        cabin_class: params.cabin_class ?? "economy",
        price,
        currency: o.total_currency,
        stops: null,
        flight_duration_display: null,
        baggage_included: null,
        baggage_allowance_kg: null,
        image: null,
        tier: "free",
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
        miles: null,
        scoring: null,
      };
    });

    const summary = `Live prices (${params.origin}→${params.destination}, ${params.departure_date}): ` +
      top3.map((o, i) => `${deals[i].airline}: ${o.total_amount} ${o.total_currency}`).join(" | ");

    return { summary, deals };
  } catch (err) {
    console.error("Duffel search error:", err);
    return { summary: "Could not fetch live prices at this time.", deals: [] };
  }
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

ANTWORT-FORMAT (immer einhalten)
- Kurze Einleitung (1 Satz), dann sofort zur Sache
- Wichtige Infos als kompakte Liste: Route, Preis, Airline, Kabine, Dauer, Gepäck
- Maximal 3-4 Bullet Points pro Deal — kein Fließtext-Roman
- Gesamtlänge: so kurz wie möglich, so lang wie nötig

REGELN
- Antworte immer auf Deutsch
- Preise immer in CHF
- Erwähne NIEMALS Skyscanner-Links im Text — die Buchungslinks sind direkt in den Deal-Karten sichtbar
- Meilen nur erwähnen wenn der Nutzer danach fragt oder es besonders attraktiv ist
- Kein "Klicke hier", kein "Weitere Infos findest du..." — kein Link-Spam
- Aktuelles Datum: ${new Date().toLocaleDateString("de-CH")}`;

const handler = async (req: Request): Promise<Response> => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
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
      toolRounds < 2
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
                    price: `${d.price} ${d.currency}`,
                    duration: d.flight_duration_display,
                    baggage: d.baggage_included
                      ? `${d.baggage_allowance_kg ?? "?"} kg`
                      : "Kein Gepäck",
                    period: d.travel_period_display,
                    miles: d.miles,
                    link: d.skyscanner_url,
                  }))
                )
              : "Keine Deals für diese Kriterien gefunden.";
        } else if (toolCall.function.name === "search_duffel") {
          const duffelResult = await searchDuffel(args);
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

    return new Response(
      JSON.stringify({
        response: assistantMessage.content,
        deals: uniqueDeals,
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
