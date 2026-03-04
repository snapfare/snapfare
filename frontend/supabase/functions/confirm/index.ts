import { serve } from "https://deno.land/std@0.190.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { Resend } from "resend";

const resend = new Resend(Deno.env.get("RESEND_API_KEY"));
const supabaseAdmin = createClient(
  Deno.env.get("SUPABASE_URL")!,
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
);

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
};

const handler = async (req: Request): Promise<Response> => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const { email, location } = await req.json();

    if (!email || !email.includes("@")) {
      return new Response(
        JSON.stringify({ success: false, error: "Invalid email" }),
        { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    const normalizedEmail = email.trim().toLowerCase();

    // Check if already unsubscribed — hard stop (business rule)
    const { data: existing } = await supabaseAdmin
      .from("subscribers")
      .select("status, tier")
      .eq("email", normalizedEmail)
      .single();

    if (existing?.status === "unsubscribed") {
      return new Response(
        JSON.stringify({ success: false, error: "Unsubscribed" }),
        { status: 409, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    if (existing?.status === "active") {
      return new Response(
        JSON.stringify({ success: true, message: "Already subscribed" }),
        { status: 200, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    // Upsert subscriber with status = 'active' (single opt-in, consistent with deployed system)
    const { error: upsertError } = await supabaseAdmin
      .from("subscribers")
      .upsert(
        {
          email: normalizedEmail,
          status: "active",
          tier: "free",
          source: location ? `web:${location}` : "web",
          updated_at: new Date().toISOString(),
        },
        { onConflict: "email" }
      );

    if (upsertError) {
      console.error("Subscriber upsert error:", upsertError);
      return new Response(
        JSON.stringify({ success: false, error: "Database error" }),
        { status: 500, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    // Send welcome email
    const emailResponse = await resend.emails.send({
      from: "SnapFare <noreply@basics-db.ch>",
      to: [normalizedEmail],
      subject: "Willkommen bei SnapFare! 🎉",
      html: buildWelcomeEmailHtml(),
    });

    console.log("Subscriber added and email sent:", normalizedEmail, emailResponse);

    return new Response(
      JSON.stringify({ success: true, data: emailResponse }),
      { status: 200, headers: { "Content-Type": "application/json", ...corsHeaders } }
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("confirm function error:", message);
    return new Response(
      JSON.stringify({ success: false, error: message }),
      { status: 500, headers: { "Content-Type": "application/json", ...corsHeaders } }
    );
  }
};

function buildWelcomeEmailHtml(): string {
  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Willkommen bei SnapFare</title>
</head>
<body style="margin:0;padding:0;background-color:#f8fafc;">
  <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 4px 6px rgba(0,0,0,0.1);">
    <div style="background:linear-gradient(135deg,#10b981 0%,#3b82f6 100%);padding:40px 30px;text-align:center;">
      <h1 style="color:#ffffff;font-size:32px;font-weight:bold;margin:0;">SnapFare</h1>
      <p style="color:#e0f2fe;font-size:16px;margin:10px 0 0 0;">Vollautomatisierte Schnäppchenjagd</p>
    </div>
    <div style="padding:40px 30px;">
      <h2 style="color:#1e293b;font-size:24px;font-weight:600;margin:0 0 20px 0;text-align:center;">Willkommen an Bord! 🚀</h2>
      <p style="color:#475569;font-size:16px;line-height:1.6;margin:0 0 20px 0;text-align:center;">
        Vielen Dank für deine Anmeldung bei SnapFare! Du erhältst ab sofort unsere besten Flugdeals ab der Schweiz.
      </p>
      <div style="background:linear-gradient(135deg,#f0fdf4 0%,#eff6ff 100%);border-radius:8px;padding:25px;margin:25px 0;">
        <h3 style="color:#047857;font-size:18px;font-weight:600;margin:0 0 15px 0;text-align:center;">Was passiert als nächstes?</h3>
        <ul style="color:#374151;font-size:14px;line-height:1.6;margin:0;padding-left:20px;">
          <li style="margin-bottom:8px;">🔍 Wir schicken dir alle zwei Wochen die besten Deals ab der Schweiz</li>
          <li style="margin-bottom:8px;">📱 Erhalte eine persönliche Einladung für dein personalisiertes Deal-Dashboard</li>
          <li style="margin-bottom:8px;">🎯 Als Premium-Nutzer bekommst du zudem Business- und Meilendeals</li>
          <li>💰 Spare ab Tag 1 hunderte Franken bei deinen Flugbuchungen</li>
        </ul>
      </div>
      <p style="color:#475569;font-size:16px;line-height:1.6;margin:25px 0 0 0;text-align:center;">
        Bis bald!<br>
        <strong style="color:#1e293b;">Das SnapFare Team</strong>
      </p>
    </div>
    <div style="background-color:#f8fafc;padding:30px;text-align:center;border-top:1px solid #e2e8f0;">
      <p style="color:#64748b;font-size:12px;margin:0 0 10px 0;">
        Du erhältst diese E-Mail, weil du dich bei SnapFare angemeldet hast.
      </p>
      <p style="color:#94a3b8;font-size:11px;margin:0;">
        © 2026 SnapFare. Alle Rechte vorbehalten.
      </p>
    </div>
  </div>
</body>
</html>`;
}

serve(handler);
