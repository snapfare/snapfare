import { serve } from "https://deno.land/std@0.190.0/http/server.ts";
import { Resend } from "npm:resend@4.0.0";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

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
    const { email, redirectTo } = await req.json();

    if (!email || !email.includes("@")) {
      return new Response(
        JSON.stringify({ success: false, error: "Invalid email" }),
        { status: 400, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    const normalizedEmail = email.trim().toLowerCase();

    // Generate a signed recovery link via the Supabase admin API
    const { data, error: linkError } = await supabaseAdmin.auth.admin.generateLink({
      type: "recovery",
      email: normalizedEmail,
      options: {
        redirectTo: redirectTo ?? "https://snapfare-dev.netlify.app/reset-password",
      },
    });

    if (linkError || !data?.properties?.action_link) {
      console.error("generateLink error:", linkError);
      return new Response(
        JSON.stringify({ success: false, error: "Could not generate reset link" }),
        { status: 500, headers: { "Content-Type": "application/json", ...corsHeaders } }
      );
    }

    const resetUrl = data.properties.action_link;

    const emailResponse = await resend.emails.send({
      from: "SnapFare <noreply@basics-db.ch>",
      to: [normalizedEmail],
      subject: "Passwort zurücksetzen 🔑",
      html: buildResetEmailHtml(resetUrl),
    });

    console.log("Password reset email sent to:", normalizedEmail, emailResponse);

    return new Response(
      JSON.stringify({ success: true, data: emailResponse }),
      { status: 200, headers: { "Content-Type": "application/json", ...corsHeaders } }
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("send-password-reset-email error:", message);
    return new Response(
      JSON.stringify({ success: false, error: message }),
      { status: 500, headers: { "Content-Type": "application/json", ...corsHeaders } }
    );
  }
};

function buildResetEmailHtml(resetUrl: string): string {
  return `
<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Passwort zurücksetzen</title>
</head>
<body style="margin:0;padding:0;background-color:#0b1120;-webkit-font-smoothing:antialiased;">
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#0b1120;">
    <tr>
      <td align="center" style="padding:24px;">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="640" style="width:640px;border-radius:18px;background:linear-gradient(135deg,#020617,#020617);border-collapse:separate;overflow:hidden;">

          <!-- Header -->
          <tr>
            <td style="padding:28px 24px 18px 24px;text-align:center;">
              <h1 style="margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:28px;line-height:34px;color:#e5e7eb;">
                SnapFare
              </h1>
              <p style="margin:6px 0 0 0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:14px;line-height:20px;color:#9ca3af;">
                Vollautomatisierte Schnäppchenjagd – die besten Reisedeals direkt in deiner Inbox.
              </p>
            </td>
          </tr>

          <!-- Icon + Title -->
          <tr>
            <td style="padding:4px 24px 8px 24px;text-align:center;">
              <div style="margin:0 auto 12px auto;width:52px;height:52px;border-radius:14px;background:linear-gradient(135deg,#1e3a5f,#1e2d5a);border:1px solid rgba(96,165,250,0.25);display:flex;align-items:center;justify-content:center;">
                <span style="font-size:26px;line-height:52px;">🔑</span>
              </div>
              <h2 style="margin:0 0 8px 0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:20px;line-height:28px;color:#e5e7eb;font-weight:700;">
                Passwort zurücksetzen
              </h2>
              <p style="margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:14px;line-height:22px;color:#cbd5e1;text-align:center;max-width:440px;margin:0 auto;">
                Du hast eine Anfrage zum Zurücksetzen deines SnapFare-Passworts erhalten. Klicke auf den Button, um ein neues Passwort zu setzen.
              </p>
            </td>
          </tr>

          <!-- CTA -->
          <tr>
            <td style="padding:24px 24px 8px 24px;text-align:center;">
              <a href="${resetUrl}" target="_blank"
                 style="display:inline-block;background:#2264f5;color:#ffffff;text-decoration:none;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:15px;font-weight:700;padding:14px 32px;border-radius:12px;border:1px solid rgba(255,255,255,0.06);">
                Passwort jetzt zurücksetzen
              </a>
            </td>
          </tr>

          <!-- Security note -->
          <tr>
            <td style="padding:18px 24px 4px 24px;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%"
                     style="border-radius:14px;background:linear-gradient(135deg,#1c1f2e,#0b1120);border:1px solid rgba(255,255,255,0.07);">
                <tr>
                  <td style="padding:16px 18px;">
                    <p style="margin:0 0 6px 0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:13px;line-height:20px;color:#9ca3af;">
                      ⚠️ <strong style="color:#d1d5db;">Nicht du?</strong> Wenn du kein neues Passwort angefordert hast, kannst du diese E-Mail einfach ignorieren — dein Konto bleibt unverändert.
                    </p>
                    <p style="margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:12px;line-height:18px;color:#6b7280;">
                      Der Link ist 24 Stunden gültig.
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Closing -->
          <tr>
            <td style="padding:20px 24px 20px 24px;text-align:center;">
              <p style="margin:0 0 4px 0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:14px;line-height:20px;color:#cbd5e1;">
                Viel Spaß beim nächsten Deal-Hunting!
              </p>
              <p style="margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:14px;line-height:20px;color:#e5e7eb;font-weight:600;">
                Dein SnapFare Team
              </p>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:16px 24px 18px 24px;border-top:1px solid #111827;text-align:center;background:#020617;">
              <p style="margin:0 0 6px 0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:11px;line-height:16px;color:#6b7280;">
                Du erhältst diese E-Mail, weil für dein SnapFare-Konto ein Passwort-Reset angefordert wurde.
              </p>
              <p style="margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:11px;line-height:16px;color:#4b5563;">
                © 2026 SnapFare. Alle Rechte vorbehalten.
              </p>
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
