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
<body style="margin:0;padding:0;background-color:#060d1a;-webkit-font-smoothing:antialiased;">
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#060d1a;">
    <tr>
      <td align="center" style="padding:32px 16px;">
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="560" style="width:560px;max-width:560px;border-radius:20px;overflow:hidden;border:1px solid rgba(255,255,255,0.07);">

          <!-- Gradient header -->
          <tr>
            <td style="background:linear-gradient(135deg,#064e3b 0%,#1e3a5f 60%,#1e1b4b 100%);padding:30px 32px 26px 32px;text-align:center;">
              <h1 style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:28px;font-weight:800;color:#ffffff;letter-spacing:-0.3px;">SnapFare</h1>
              <p style="margin:4px 0 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;color:rgba(255,255,255,0.55);">Vollautomatisierte Schnäppchenjagd</p>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="background:#0b1220;padding:36px 32px 32px 32px;text-align:center;">

              <!-- Icon -->
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 auto 18px auto;">
                <tr>
                  <td style="width:56px;height:56px;border-radius:14px;background:linear-gradient(135deg,#064e3b,#1e3a5f);border:1px solid rgba(16,185,129,0.25);text-align:center;vertical-align:middle;">
                    <span style="font-size:26px;line-height:56px;display:block;">🔑</span>
                  </td>
                </tr>
              </table>

              <h2 style="margin:0 0 10px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:21px;font-weight:700;color:#f1f5f9;">Passwort zurücksetzen</h2>
              <p style="margin:0 auto 28px auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:14px;line-height:22px;color:#64748b;max-width:380px;">
                Klicke auf den Button, um ein neues Passwort für dein SnapFare-Konto zu setzen.
              </p>

              <!-- CTA -->
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
                <tr>
                  <td style="border-radius:12px;background:linear-gradient(135deg,#1d4ed8,#2264f5);">
                    <a href="${resetUrl}" target="_blank"
                       style="display:inline-block;padding:14px 38px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:15px;font-weight:700;color:#ffffff;text-decoration:none;">
                      Passwort zurücksetzen
                    </a>
                  </td>
                </tr>
              </table>

              <!-- Security note -->
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:28px;">
                <tr>
                  <td style="border-radius:12px;background:#0f1729;border:1px solid rgba(245,158,11,0.18);padding:14px 18px;text-align:left;">
                    <p style="margin:0 0 4px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:13px;line-height:19px;color:#94a3b8;">
                      <span style="color:#f59e0b;">⚠</span>&nbsp;<strong style="color:#cbd5e1;">Nicht du?</strong>&nbsp;Dann kannst du diese E-Mail ignorieren — dein Konto bleibt unverändert.
                    </p>
                    <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;line-height:16px;color:#334155;">Der Link ist 24 Stunden gültig.</p>
                  </td>
                </tr>
              </table>

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:#070e1c;padding:16px 32px;text-align:center;border-top:1px solid rgba(255,255,255,0.05);">
              <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;line-height:16px;color:#1e293b;">
                © 2026 SnapFare &nbsp;·&nbsp; Passwort-Reset angefordert für dein Konto
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
