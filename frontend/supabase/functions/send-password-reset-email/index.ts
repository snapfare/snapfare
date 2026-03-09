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
            <td align="center" style="background:#0d1526;padding:40px 32px 32px 32px;">

              <!-- Icon -->
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 auto 20px auto;">
                <tr>
                  <td align="center" valign="middle" style="width:54px;height:54px;border-radius:12px;background:#0a1f35;border:1px solid rgba(59,130,246,0.3);">
                    <span style="font-size:24px;">🔑</span>
                  </td>
                </tr>
              </table>

              <p style="margin:0 0 8px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:20px;font-weight:700;color:#f8fafc;text-align:center;">Passwort zurücksetzen</p>
              <p style="margin:0 0 28px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:14px;line-height:21px;color:#64748b;text-align:center;">Setze jetzt ein neues Passwort für dein Konto.</p>

              <!-- CTA -->
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 auto;">
                <tr>
                  <td align="center" style="border-radius:10px;background:#2563eb;">
                    <a href="${resetUrl}" target="_blank"
                       style="display:inline-block;padding:13px 36px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:15px;font-weight:700;color:#ffffff;text-decoration:none;letter-spacing:0.1px;">
                      Passwort zurücksetzen
                    </a>
                  </td>
                </tr>
              </table>

              <!-- Security note -->
              <p style="margin:24px 0 0 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:12px;line-height:18px;color:#475569;text-align:center;">
                Nicht angefordert? Ignoriere diese E-Mail.<br>Der Link läuft nach 24 Stunden ab.
              </p>

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td align="center" style="background:#070c18;padding:16px 32px;border-top:1px solid rgba(255,255,255,0.05);">
              <p style="margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:11px;color:#4b5563;">© 2026 SnapFare</p>
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
