import { serve } from "https://deno.land/std@0.190.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { Resend } from "npm:resend@4.0.0";

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
      // Already subscribed — send them the welcome email anyway (they re-requested it)
      // but do NOT re-upsert (preserves their current tier, including premium)
      try {
        await resend.emails.send({
          from: "SnapFare <noreply@basics-db.ch>",
          to: [normalizedEmail],
          subject: "Willkommen bei SnapFare! 🎉",
          html: buildWelcomeEmailHtml(),
        });
        console.log("Welcome email resent to existing subscriber:", normalizedEmail);
      } catch (emailErr) {
        console.error("Email send error (existing subscriber):", emailErr);
      }
      return new Response(
        JSON.stringify({ success: true, message: "Already subscribed, email sent" }),
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
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Willkommen bei SnapFare</title>
</head>
<body style="margin:0;padding:0;background-color:#0a0f1e;">
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#0a0f1e;">
    <tr>
      <td align="center" style="padding:40px 16px;">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="520" style="width:520px;border-radius:16px;overflow:hidden;border:1px solid rgba(255,255,255,0.08);">

          <!-- Header -->
          <tr>
            <td align="center" style="background:linear-gradient(135deg,#065f46 0%,#1e40af 100%);padding:28px 32px;">
              <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:26px;font-weight:800;color:#ffffff;letter-spacing:-0.5px;">SnapFare</p>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td align="center" style="background:#0d1526;padding:36px 32px 28px 32px;">

              <p style="margin:0 0 6px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:20px;font-weight:700;color:#f8fafc;text-align:center;">Willkommen an Bord! 🚀</p>
              <p style="margin:0 0 24px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:14px;line-height:21px;color:#64748b;text-align:center;">Du bist jetzt dabei — wir schicken dir die besten Flugdeals ab der Schweiz.</p>

              <!-- What happens next -->
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                  <td style="border-radius:12px;background:#111827;border:1px solid rgba(16,185,129,0.2);padding:18px 20px;text-align:left;">
                    <p style="margin:0 0 12px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;font-weight:600;color:#e2e8f0;text-align:center;">Was passiert als nächstes?</p>
                    <p style="margin:0 0 7px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;line-height:19px;color:#94a3b8;">✈️&nbsp; Regelmässige Newsletter mit kuratierten Flugdeals ab der Schweiz.</p>
                    <p style="margin:0 0 7px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;line-height:19px;color:#94a3b8;">🎯&nbsp; Optional: Premium für Business- &amp; Meilendeals dazubuchen.</p>
                    <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;line-height:19px;color:#94a3b8;">💰&nbsp; Ziel: Ab der ersten Buchung spürbar sparen.</p>
                  </td>
                </tr>
              </table>

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td align="center" style="background:#070c18;padding:16px 32px;border-top:1px solid rgba(255,255,255,0.05);">
              <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;color:#4b5563;">© 2026 SnapFare &nbsp;·&nbsp; Du erhältst diese E-Mail weil du dich angemeldet hast.</p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>`;
}

serve(handler);
