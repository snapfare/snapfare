import os
import json
from html import escape
from typing import Dict, Any, List

import pandas as pd

from services.baggage_format import format_baggage_short_de
from scoring.miles_utils import choose_best_program, filter_miles_programs_display, great_circle_miles

try:  # airportsdata is optional; if missing, we skip city lookups.
    import airportsdata  # type: ignore
    airports = airportsdata.load("IATA")
except Exception:  # pragma: no cover - defensive
    airports = {}

# Optional lookup tables; fall back to empty dicts if CSV data is missing.
try:
    df_aircraft = pd.read_csv("backend/scoring/data/aircraft_types.csv", dtype=str).fillna("")
    AIRCRAFT = df_aircraft.set_index("IATA").to_dict(orient="index")
except FileNotFoundError:
    AIRCRAFT = {}

try:
    df_airlines = pd.read_csv("backend/scoring/data/airlines.csv", dtype=str).fillna("")
    AIRLINES = df_airlines.set_index("code").to_dict(orient="index")
except FileNotFoundError:
    AIRLINES = {}

try:
    df_airports_de = pd.read_csv("backend/scoring/data/airport_names_german.csv", dtype=str).fillna("")
    # El CSV puede contener cabeceras duplicadas y códigos repetidos.
    # - Ignoramos filas tipo "code,deutscher_Name" embebidas.
    # - Preferimos la última aparición para permitir curación incremental.
    if "code" in df_airports_de.columns:
        df_airports_de["code"] = df_airports_de["code"].astype(str).str.strip().str.upper()
        df_airports_de = df_airports_de[df_airports_de["code"].str.lower() != "code"]
        df_airports_de = df_airports_de.drop_duplicates(subset="code", keep="last")
    AIRPORTS_DE = df_airports_de.set_index("code").to_dict(orient="index")
except FileNotFoundError:
    AIRPORTS_DE = {}


def aircraft_model(iata: str) -> str | None:
    rec = AIRCRAFT.get(iata.upper())
    return rec.get("Aircraft_Model") if rec else None

def airlines(code: str) -> str | None:
    rec = AIRLINES.get(code.upper())
    return rec.get("name") if rec else None

def airports_de(code: str) -> str | None:
    rec = AIRPORTS_DE.get(code.upper())
    return rec.get("deutscher_Name") if rec else None


def airport_photo_url(code: str) -> str | None:
    rec = AIRPORTS_DE.get((code or "").strip().upper())
    if not rec:
        return None
    url = (rec.get("photo_url") or rec.get("image_url") or "").strip()
    return url or None


def _unsplash_fallback_image(query: str) -> str:
    # Simple, no-key fallback. Works as a best-effort remote image.
    q = (query or "").strip().replace(" ", "+")
    if not q:
        q = "city"
    return f"https://source.unsplash.com/featured/640x400/?{q}"  # nosec - URL string


def _env_display_currency() -> str | None:
    cur = (os.getenv("DISPLAY_CURRENCY") or os.getenv("SNAPCORE_DISPLAY_CURRENCY") or "").strip().upper()
    return cur or None


def _convert_price(amount: float, from_cur: str, to_cur: str) -> float | None:
    """Convert price using simple env-configured FX rates.

    Supported:
      - EUR -> CHF via FX_EUR_TO_CHF
    """
    f = (from_cur or "").strip().upper()
    t = (to_cur or "").strip().upper()
    if not f or not t or f == t:
        return amount
    if f == "EUR" and t == "CHF":
        raw = (os.getenv("FX_EUR_TO_CHF") or "").strip()
        if not raw:
            return None
        try:
            rate = float(raw)
            if rate <= 0:
                return None
            return amount * rate
        except Exception:
            return None
    return None


def _format_price_display(price: Any, currency: str) -> str:
    """Format price line for HTML.

    If DISPLAY_CURRENCY is set and we have an FX rate, we show the converted
    currency instead of the original.
    """
    cur = (currency or "").strip().upper() or "CHF"
    if not isinstance(price, (int, float)):
        return f"Preis N/A {cur}".strip()

    target = _env_display_currency()
    if target and target != cur:
        converted = _convert_price(float(price), cur, target)
        if converted is not None:
            return f"ab {converted:.0f} {target}".strip()

    return f"ab {float(price):.0f} {cur}".strip()


def _is_business_deal(deal: Dict[str, Any]) -> bool:
    cabin = str(deal.get("cabin_class") or "").strip().upper()
    if cabin in {"BUSINESS", "J", "C"}:
        return True
    title = str(deal.get("title") or "").lower()
    return "business" in title

def offer_to_html(offer: Dict[str, Any]) -> str:
    """
    Convert a single Amadeus-style offer dict into the HTML snippet structure.
    Returns a string containing the rendered HTML.
    """

    # Safe helpers
    def _first_segment(itins):
        if not itins:
            return {}
        for itin in itins:
            segs = itin.get("segments") or []
            if segs:
                return segs[0]
        return {}

    def _last_segment(itins):
        if not itins:
            return {}
        for itin in reversed(itins):
            segs = itin.get("segments") or []
            if segs:
                return segs[-1]
        return {}

    itins = offer.get("itineraries") or []
    first_seg = _first_segment(itins)

    # Para la “ruta” usamos el primer itinerario (outbound):
    # origen = primera salida, destino = última llegada del primer itin.
    first_itin = (itins or [{}])[0] if isinstance(itins, list) else {}
    first_itin_segs = (first_itin or {}).get("segments") or []
    last_outbound_seg = (first_itin_segs[-1] if first_itin_segs else {})

    # Extract fields with fallbacks
    img = offer.get("image") or (offer.get("images") or [{}])[0].get("url") or ""
    origin_code = (first_seg.get("departure") or {}).get("iataCode") or ""
    dest_code = (last_outbound_seg.get("arrival") or {}).get("iataCode") or ""

    # Prefer destination photo when curated in airport_names_german.csv
    dest_photo = airport_photo_url(dest_code)
    if dest_photo:
        img = dest_photo
    elif not img:
        # Best-effort fallback when CSV doesn't have photos
        img = _unsplash_fallback_image(dest_code or "destination")

    origin_city = (airports.get(origin_code) or {}).get("city") or (airports.get(origin_code) or {}).get("name") or origin_code
    dest_city = (airports.get(dest_code) or {}).get("city") or (airports.get(dest_code) or {}).get("name") or dest_code

    if origin_code != "":
        origin_code_de = airports_de(origin_code)
        if origin_code_de is not None:
            origin_city = origin_code_de
    if dest_code != "":
        dest_code_de = airports_de(dest_code)
        if dest_code_de is not None:
            dest_city = dest_code_de
    def _fmt_place(name: str, code: str) -> str:
        n = (name or "").strip()
        c = (code or "").strip().upper()
        if n and c and n.upper() != c:
            return f"{n} ({c})"
        return n or c or ""

    # Try to construct a readable route; fall back to codes only
    if origin_code and dest_code:
        route = f"{_fmt_place(origin_city, origin_code)} → {_fmt_place(dest_city, dest_code)}"
    elif origin_code:
        route = origin_code
    elif dest_code:
        route = dest_code
    else:
        route = "Flight"

    # Airline / carrier
    carrier = (first_seg.get("carrierCode")) or (offer.get("validatingAirlineCodes") or [""])[0] or "Unknown"
    if carrier != "" and carrier != "Unknown":
        carrier_de = airlines(carrier)
        if carrier_de is not None:
            carrier = carrier_de

    # Baggage: try common keys, else placeholder
    includingCheckedBags = (
            (offer.get("travelerPricings") or [{}])[0]
            .get("fareDetailsBySegment", [{}])[0]
            .get("includedCheckedBags", {})
            .get('quantity')
    )
    includingCabinBags = (
            (offer.get("travelerPricings") or [{}])[0]
            .get("fareDetailsBySegment", [{}])[0]
            .get("includedCabinBags", {})
            .get('quantity')
    )

    def _qty(val: Any) -> int:
        if val is None:
            return 0
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str):
            s = val.strip()
            return int(s) if s.isdigit() else 0
        return 0

    includingCheckedBags = _qty(includingCheckedBags)
    includingCabinBags = _qty(includingCabinBags)
    baggage = "—"
    if includingCabinBags > 0 and includingCheckedBags == 0:
        baggage = f"{includingCabinBags}×8 kg"
    elif includingCabinBags == 0 and includingCheckedBags > 0:
        baggage = f"{includingCheckedBags}×23 kg"
    elif includingCabinBags > 0 and includingCheckedBags > 0:
        baggage = f"{includingCabinBags}×8 kg + {includingCheckedBags}×23 kg"


    # Aircraft: attempt to read from segment
    aircraft = (first_seg.get("aircraft") or {}).get("code") or first_seg.get("equipment") or "Unknown"
    aircraft_model_name = aircraft_model(aircraft)
    if aircraft_model_name is None:
        aircraft_model_name = aircraft

    # Miles / loyalty info (not standard in Amadeus offers) - optional
    miles = offer.get("miles") or offer.get("loyalty") or ""

    # Travel dates: show first departure datetime if available
    dates = (first_seg.get("departure") or {}).get("at") or ""

    # Price extraction
    price_obj = offer.get("price") or {}
    price_cur = str(price_obj.get("currency") or offer.get("currency") or "EUR").strip().upper() or "EUR"
    raw_total = price_obj.get("total") or price_obj.get("grandTotal") or price_obj.get("totalPrice")
    price_num: float | None = None
    if isinstance(raw_total, (int, float)):
        price_num = float(raw_total)
    elif isinstance(raw_total, str):
        try:
            price_num = float(raw_total.strip().replace(",", "."))
        except Exception:
            price_num = None
    price_text = _format_price_display(price_num, price_cur)

    # Link / CTA
    link = offer.get("deepLink") or offer.get("url") or offer.get("link") or "#"

    # Escape values for safe HTML insertion
    img_e = escape(str(img))
    route_e = escape(route)
    carrier_e = escape(str(carrier))
    baggage_e = escape(str(baggage))
    aircraft_e = escape(str(aircraft_model_name))
    miles_e = escape(str(miles))
    dates_e = escape(str(dates))
    price_e = escape(price_text)
    link_e = escape(str(link))

    # Return the HTML snippet using the provided structure
    html = (
        f"<!-- ===================== DEAL ===================== -->\n"
        f"<tr><td style=\"padding:10px 0;\">\n"
        f"  <table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\" \n"
        f"         style=\"width:100%;border-radius:18px;background:linear-gradient(135deg,#0f172a,#0b1220);"
        f"box-shadow:0 1px 0 rgba(255,255,255,0.03) inset;text-align:center;\">\n"
        f"    <tr>\n"
        f"      <td class=\"stack card-pad\" style=\"padding:20px 16px;\">\n"
        f"        <div class=\"m-bound\">\n"
        f"          <img src=\"{img_e}\"\n"
        f"               width=\"160\" height=\"100\" alt=\"{route_e}\"\n"
        f"               class=\"deal-img\"\n"
        f"               style=\"display:block;margin:0 auto;border-radius:14px;width:160px;height:100px;object-fit:cover;"
        f"border:1px solid rgba(255,255,255,0.06);\">\n"
        f"          <div style=\"margin-top:12px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:18px;line-height:24px;"
        f"color:#e5e7eb;font-weight:800;\">\n"
        f"            {route_e}\n"
        f"          </div>\n"
        f"          <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:18px;"
        f"color:#9ca3af;\">\n"
        f"            {carrier_e}\n"
        f"          </div>\n"
        f"          <div style=\"margin-top:10px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;"
        f"color:#cbd5e1;\">🧳 Gepäck: {baggage_e}</div>\n"
        f"          <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;"
        f"color:#cbd5e1;\">✈️ Flugzeug: {aircraft_e}</div>\n"
        f"          <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;"
        f"color:#cbd5e1;\">💳 Meilen: <strong>{miles_e}</strong></div>\n"
        f"          <div style=\"margin-top:8px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:12px;line-height:18px;"
        f"color:#94a3b8;\">\n"
        f"            Mögliche Reisedaten:<br>{dates_e}\n"
        f"          </div>\n"
        f"          <div class=\"price\" style=\"margin-top:14px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:22px;"
        f"line-height:26px;color:#60a5fa;font-weight:900;\">{price_e} </div>\n"
        f"          <a href=\"{link_e}\" target=\"_blank\"\n"
        f"             class=\"cta\"\n"
        f"             style=\"display:inline-block;background:#2264f5;color:#ffffff;text-decoration:none;"
        f"font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;font-weight:700;padding:10px 18px;border-radius:12px;"
        f"border:1px solid rgba(255,255,255,0.06);\">\n"
        f"            Deal ansehen\n"
        f"          </a>\n"
        f"        </div>\n"
        f"      </td>\n"
        f"    </tr>\n"
        f"  </table>\n"
        f"</td></tr>\n"
    )

    return html


def _parse_json_dict(val: Any) -> Dict[str, Any]:
    """Parse a JSON object from a Supabase JSON/JSONB field.

    Depending on the client/table schema, Supabase may return JSONB as a dict
    or as a serialized JSON string.
    """

    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return {}
        # Fast reject: we only care about JSON objects.
        if not (s.startswith("{") and s.endswith("}")):
            return {}
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

def build_full_html(offer_htmls: List[str], title: str = "Flight Deals") -> str:
    """Return a complete HTML document string containing the provided offer snippets."""
    head = """
    <meta charset="utf-8">
    <meta name="x-apple-disable-message-reformatting">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Newsletter Deals</title>
    <style>
      /* Mobile-Tweaks (Desktop ist ebenfalls zentriert) */
      @media screen and (max-width:600px){
        .container{width:100%!important}
        .stack{display:block!important;width:100%!important;text-align:center!important}
        .card-pad{padding:20px 16px!important}
        .deal-img{display:block!important;margin:0 auto!important}
        .price{display:block!important;text-align:center!important;font-size:20px!important}

        /* NEU: Einheitliche Innenbreite je Karte */
        .m-bound{width:320px!important;margin:0 auto!important;text-align:center!important}

        /* NEU: CTA konsistente Breite, aber nicht full-width */
        .cta{
          display:inline-block!important;
          width:auto!important;
          min-width:180px!important;
          max-width:240px!important;
          margin:12px auto 0!important;
          text-align:center!important;
        }
      }
      a[x-apple-data-detectors]{color:inherit!important;text-decoration:none!important}
    </style>
    """

    # Body prefix provided by user (hidden preview text + header + opening container table)
    body_prefix = (
        '<body style="margin:0;padding:0;background:#0b1120;-webkit-font-smoothing:antialiased;">'
        '<div style="display: none; max-height: 0px; overflow: hidden;">{{PreviewText}}&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;'
        '&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;'
        '&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;‌&nbsp;</div>'
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#0b1120;">'
        '  <tr>'
        '    <td align="center" style="padding:24px;">'
        '      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="640" class="container"'
        ' style="width:640px;background:#0b1120;border-collapse:separate;text-align:center;">'
        '            '
        '            <!-- Header -->'
        '            <tr>'
        '<td style="padding:12px 16px 16px 16px;text-align:center;">'
        '  <h1 style="margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:24px;line-height:30px;color:#e5e7eb;">'
        '    Ready für die besten Flugdeals? ✈️'
        '  </h1>'
        '  <p align="justify"'
        '     style="margin:12px 0 0 0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:15px;line-height:24px;color:#9ca3af;text-align:justify;text-justify:inter-word;">'
        '    Willkommen zurück – alle zwei Wochen flattern die besten Flugdeals direkt in deine Inbox. '
        '  <a href="XXX" target="_blank" '
        '     style="color:#60a5fa;text-decoration:underline;">Premium-Deals</a> '
        '  gibt es mit einer Jahresmitgliedschaft für <strong style="color:#e5e7eb;">CHF 49.–</strong>. Der Premium-Newsletter enthält Business- und Meilen-Angebote mit Fokus auf die Langstrecke und Frühzugang zur neuen Plattform, der Free-Newsletter zeigt dir natürlich weiterhin die günstigsten Economy-Deals auf Kurz- und Langstrecke. Auch wichtig für dich: wir arbeiten zurzeit daran dir personalisierte Deals zu deinen Wunschzielen anzubieten, welche sich mit nur einem Klick buchen lassen - selbstverständlich kostenlos😄 Viel Spass mit den heutigen Angeboten - für den finalen Preis gerne immer auf die Buchungsseite weiterleiten lassen!'
        '  </p>'
        '</td>'
        '            </tr>'
    )

    # The offer snippets are expected to contain `<tr>...</tr>` blocks; insert them inside the container table
    body_rows = "\n".join(offer_htmls)

    # Premium section + footer (inserted after offers, before closing tables)
    premium_and_footer = (
        "  <!-- ===================== PREMIUM SECTION ===================== -->"
        "            <tr>"
        "              <td style=\"padding:22px 16px 8px 16px;text-align:center;\">"
        "                <div style=\"font-family:Inter,Segoe UI,Arial,sans-serif;font-size:14px;line-height:20px;color:#9ca3af;\">"
        "                  🔒 Exklusive Premium-Deals"
        "                </div>"
        "              </td>"
        "            </tr>"

        "            <!-- Premium 1 -->"
        "            <tr><td style=\"padding:8px 0;\">"
        "              <table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\""
        "                     style=\"width:100%;border-radius:18px;background:linear-gradient(135deg,#0f172a,#0b1220);opacity:0.88;text-align:center;\">"
        "                <tr>"
        "                  <td class=\"stack card-pad\" style=\"padding:20px 16px;\">"
        "                    <div class=\"m-bound\">"
        "                      <div class=\"deal-img\" style=\"width:160px;height:100px;border-radius:14px;background:#111827;border:1px dashed rgba(255,255,255,0.2);text-align:center;line-height:100px;color:#6b7280;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:18px;margin:0 auto;\">🔒</div>"
        "                      <div style=\"margin-top:12px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:18px;line-height:24px;color:#9ca3af;font-weight:800;\">Ziel exklusiv – Premium</div>"
        "                      <div style=\"margin-top:10px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;color:#6b7280;\">🧳 Gepäck: —</div>"
        "                      <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;color:#6b7280;\">✈️ Flugzeug: —</div>"
        "                      <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;color:#6b7280;\">💳 Meilen: —</div>"
        "                      <div style=\"margin-top:8px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:12px;line-height:18px;color:#6b7280;\">Mögliche Reisedaten:<br>—</div>"
        "                      <div class=\"price\" style=\"margin-top:14px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:22px;line-height:26px;color:#475569;font-weight:900;\">CHF ——</div>"
        "                      <a href=\"XXX\" target=\"_blank\" class=\"cta\""
        "                         style=\"display:inline-block;background:#0b172f;color:#94a3b8;text-decoration:none;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;font-weight:700;padding:10px 18px;border-radius:12px;border:1px solid rgba(255,255,255,0.06);\">"
        "                        Jetzt Premium freischalten"
        "                      </a>"
        "                    </div>"
        "                  </td>"
        "                </tr>"
        "              </table>"
        "            </td></tr>"

        "            <!-- Feedback -->"
        "            <tr>"
        "              <td style=\"padding:20px 16px;text-align:center;\">"
        "                <p style=\"margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:14px;line-height:22px;color:#9ca3af;\">"
        "                  Hast du Anregungen, Feedback oder Wünsche zur Häufigkeit? Antworte einfach direkt auf diese E-Mail – wir freuen uns über jede Rückmeldung!"
        "                </p>"
        "              </td>"
        "            </tr>"

        "<!-- Footer -->"
        "<tr>"
        "  <td style=\"padding:22px 16px 10px 16px;text-align:center;\">"
        "    <p style=\"margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:12px;line-height:18px;color:#64748b;\">"
        "      Du erhältst diese E-Mail, weil du dich angemeldet hast."
        "      Wenn du keine Deals mehr möchtest, kannst du dich jederzeit"
        "      <a href=\"{{UnsubscribeURL}}\" target=\"_blank\" rel=\"noopener\" style=\"color:#94a3b8;text-decoration:underline;\">abmelden</a>. "
        "      Das <a href=\"XXX\" target=\"_blank\" rel=\"noopener\" style=\"color:#94a3b8;text-decoration:underline;\">Impressum</a> "
        "      und die <a href=\"XXX\" target=\"_blank\" rel=\"noopener\" style=\"color:#94a3b8;text-decoration:underline;\">Datenschutzerklärung</a> "
        "      findest du auf der Webseite."
        "    </p>"

        "    <div style=\"height:10px;line-height:10px;font-size:0;\">&nbsp;</div>"

        "    <p style=\"margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:12px;line-height:18px;color:#64748b;\">"
        "      © 2025 Alle Rechte vorbehalten."
        "    </p>"

        "    <!-- Required footer elements for EmailOctopus Starter -->"
        "    <p style=\"margin:6px 0 0 0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:11px;line-height:16px;color:#94a3b8;\">"
        "      Powered by <a href=\"{{RewardsURL}}\" target=\"_blank\" rel=\"noopener\" style=\"color:#94a3b8;text-decoration:underline;\">EmailOctopus</a> • "
        "      <span style=\"color:#94a3b8;\">{{SenderInfo}}</span>"
        "    </p>"
        "  </td>"
        "</tr>"
    )

    # Close inner and outer tables and body/html
    closing = (
        '      </table>'
        '    </td>'
        '  </tr>'
        '</table>'
        '</body>'
        '</html>'
    )

    html = (
        "<!doctype html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        f"{head}\n"
        "</head>\n"
        f"{body_prefix}\n"
        f"{body_rows}\n"
        f"{premium_and_footer}\n"
        f"{closing}\n"
    )
    return html


def deal_to_newsletter_row(deal: Dict[str, Any]) -> str:
    """Render a single scored deal as a newsletter <tr>...</tr> block.

    This mirrors the example card template used for the newsletter
    (dark gradient card with route, airline, baggage, aircraft, miles,
    dates, and CTA button) so that snippets can be inlined directly
    into the newsletter HTML structure.
    """

    title = str(deal.get("title") or "Flight deal")

    origin_city_raw = str(deal.get("origin") or "").strip()
    dest_city_raw = str(deal.get("destination") or "").strip()
    origin_iata = str(deal.get("origin_iata") or "").strip().upper()
    dest_iata = str(deal.get("destination_iata") or "").strip().upper()

    def _looks_like_iata(val: str) -> bool:
        v = (val or "").strip().upper()
        return len(v) == 3 and v.isalpha()

    def _resolve_place(city_val: str, code: str, fallback_label: str) -> str:
        city = (city_val or "").strip()
        code_u = (code or "").strip().upper()

        # Priority 1: use explicit city from deal if it's not just the code.
        if city and (not code_u or city.strip().upper() != code_u):
            resolved_city = city
        else:
            # Priority 2: if we have an IATA, try German CSV mapping.
            resolved_city = ""
            if code_u and len(code_u) == 3:
                mapped = airports_de(code_u)
                if mapped:
                    resolved_city = mapped

            # Priority 2b: fall back to airportsdata (city/name) if available.
            if not resolved_city and code_u and len(code_u) == 3:
                rec = airports.get(code_u)
                if isinstance(rec, dict):
                    resolved_city = str(rec.get("city") or rec.get("name") or "").strip()

            # Priority 3: fall back to deal city (even if it's code) or IATA.
            if not resolved_city:
                resolved_city = city if city else code_u

        # Format label. Avoid things like "SJU (SJU)".
        if code_u and resolved_city and resolved_city.upper() != code_u:
            return f"{resolved_city} ({code_u})"
        if resolved_city:
            return resolved_city
        if code_u:
            return code_u
        return fallback_label

    origin_label = _resolve_place(origin_city_raw, origin_iata, "Origin")
    dest_label = _resolve_place(dest_city_raw, dest_iata, "Destination")

    route_text = f"{origin_label} → {dest_label}"
    # Two-line version as in the example: origin on first line, dest on second
    prefix = "BUSINESS: " if _is_business_deal(deal) else ""
    route_html = f"{escape(prefix + origin_label)} →<br>{escape(dest_label)}"

    airline = str(deal.get("airline") or "").strip() or ""
    baggage_text = format_baggage_short_de(deal) or "Kein Gepäck inklusive"

    aircraft = str(deal.get("aircraft") or "").strip() or "—"
    llm_fields = _parse_json_dict(deal.get("llm_enriched_fields"))
    mpd = llm_fields.get("miles_programs_display")
    mpd_filtered = None
    if mpd not in (None, ""):
        mpd_filtered = filter_miles_programs_display(str(mpd), airline)

    miles_raw = mpd_filtered if mpd_filtered not in (None, "") else deal.get("miles")

    # If we can infer a valid program set for the airline, prefer a single best-program
    # estimate over ambiguous/invalid multi-program strings.
    dist_m = None
    if origin_iata and dest_iata:
        try:
            dist_m = great_circle_miles(origin_iata, dest_iata)
        except Exception:
            dist_m = None

    if isinstance(dist_m, int) and dist_m > 0:
        best_prog, best_est = choose_best_program(dist_m, airline)
        if best_prog and isinstance(best_est, int) and best_est > 0:
            # Use computed best text when current miles display is missing or looks ambiguous.
            miles_text = str(miles_raw).strip() if miles_raw not in (None, "") else ""
            is_ambiguous = any(sep in miles_text for sep in ("/", "|", "\n", ";"))
            is_missing = not miles_text or miles_text in {"—", "-"}
            looks_numeric_only = miles_text.isdigit()
            if is_missing or is_ambiguous or looks_numeric_only:
                miles_raw = f"{best_est:,}".replace(",", "'") + f" · {best_prog}"
    miles = "—"
    if miles_raw not in (None, ""):
        try:
            miles_int = int(float(str(miles_raw).strip()))
            miles = f"{miles_int:,}".replace(",", "'")
        except Exception:
            miles = str(miles_raw).strip() or "—"

    date_out = deal.get("date_out") or deal.get("departure_date")
    date_in = deal.get("date_in") or deal.get("return_date")
    date_range = str(deal.get("date_range") or "").strip()
    if date_out and date_in:
        dates_text = f"{date_out} – {date_in}"
    elif date_range:
        dates_text = date_range
    else:
        dates_text = "Flexible dates"

    price = deal.get("price")
    currency = str(deal.get("currency") or "CHF").strip()
    price_text = _format_price_display(price, currency)

    link = str(deal.get("booking_url") or deal.get("link") or "#").strip() or "#"

    img = str(deal.get("image") or deal.get("image_url") or "").strip()
    dest_photo = airport_photo_url(dest_iata)
    if dest_photo:
        img = dest_photo
    elif not img:
        img = _unsplash_fallback_image(dest_label or dest_iata or "destination")

    # Escape values for safe HTML insertion (except route_html which already
    # encodes its own <br> and escaped labels).
    img_e = escape(img) if img else ""
    route_alt_e = escape(route_text)
    carrier_e = escape(airline or "-")
    baggage_e = escape(baggage_text)
    aircraft_e = escape(aircraft)
    miles_e = escape(miles)
    dates_e = escape(dates_text)
    price_e = escape(price_text)
    link_e = escape(link)

    img_block = ""
    if img_e:
        img_block = (
            f"          <img src=\"{img_e}\""
            f"               width=\"160\" height=\"100\" alt=\"{route_alt_e}\""
            "               class=\"deal-img\""
            "               style=\"display:block;margin:0 auto;border-radius:14px;width:160px;height:100px;object-fit:cover;"
            "border:1px solid rgba(255,255,255,0.06);\">"
        )

    card_html = (
        "<!-- ===================== DEAL ===================== -->"
        "<tr><td style=\"padding:10px 0;\">"
        "  <table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\" "
        "         style=\"width:100%;border-radius:18px;background:linear-gradient(135deg,#0f172a,#0b1220);"
        "box-shadow:0 1px 0 rgba(255,255,255,0.03) inset;text-align:center;\">"
        "    <tr>"
        "      <td class=\"stack card-pad\" style=\"padding:20px 16px;\">"
        "        <div class=\"m-bound\">"
        f"{img_block}"
        "          <div style=\"margin-top:12px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:18px;line-height:24px;"
        "color:#e5e7eb;font-weight:800;\">"
        f"            {route_html}"
        "          </div>"
        "          <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:18px;"
        "color:#9ca3af;\">"
        f"            {carrier_e}"
        "          </div>"
        "          <div style=\"margin-top:10px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;"
        "color:#cbd5e1;\">🧳 Gepäck: "
        f"{baggage_e}</div>"
        "          <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;"
        "color:#cbd5e1;\">✈️ Flugzeug: "
        f"{aircraft_e}</div>"
        "          <div style=\"margin-top:4px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;"
        "color:#cbd5e1;\">💳 Meilen: <strong>"
        f"{miles_e}</strong></div>"
        "          <div style=\"margin-top:8px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:12px;line-height:18px;"
        "color:#94a3b8;\">"
        "            Mögliche Reisedaten:<br>"
        f"{dates_e}"
        "          </div>"
        "          <div class=\"price\" style=\"margin-top:14px;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:22px;"
        "line-height:26px;color:#60a5fa;font-weight:900;\">"
        f"{price_e} "
        "</div>"
        f"          <a href=\"{link_e}\" target=\"_blank\""
        "             class=\"cta\""
        "             style=\"display:inline-block;background:#2264f5;color:#ffffff;text-decoration:none;"
        "font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;font-weight:700;padding:10px 18px;border-radius:12px;"
        "border:1px solid rgba(255,255,255,0.06);\">"
        "            Deal ansehen"
        "          </a>"
        "        </div>"
        "      </td>"
        "    </tr>"
        "  </table>"
        "</td></tr>"
    )
    return card_html