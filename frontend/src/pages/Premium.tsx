import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Check, Star } from "lucide-react";
import { useState } from "react";
import { useToast } from "@/hooks/use-toast";
import twintButton from "@/assets/twint-button.svg";

const TWINT_LINK =
  "https://go.twint.ch/1/e/tw?tw=acq.gLWaSc6qS9WXTyve02qU3TYzXh6aJj-WV-OoE_J4WpK9fVqgx8XwDgLVcKKthvDk.&amount=49.00&trxInfo=BLACKFRIDAY_PREMIUM_1Y";

const Premium = () => {
  const [email, setEmail] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const { toast } = useToast();

  const handleUpgrade = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email) return;

    setIsSubmitting(true);
    try {
      const isEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
      if (!isEmail) {
        toast({
          title: "Ungültige E-Mail",
          description: "Bitte gib eine gültige E-Mail-Adresse ein.",
          variant: "destructive",
        });
        return;
      }

      const response = await fetch(
        "https://wwoowwnjrepokmjgxhlw.functions.supabase.co/create-session",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email }),
        }
      );

      if (!response.ok) {
        window.location.href = TWINT_LINK;
        return;
      }

      const data = await response.json();
      if (data?.payUrl) {
        window.location.href = data.payUrl;
        return;
      }

      window.location.href = TWINT_LINK;
    } catch {
      window.location.href = TWINT_LINK;
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">

      {/* ✅ INTRO / HERO (aus altem Code) */}
      <div className="pt-20 pb-12 text-center px-4">
        <h1 className="text-5xl md:text-6xl font-bold mb-6">
          <span className="bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
            SnapFare Premium
          </span>
        </h1>
        <p className="text-xl text-gray-300 max-w-2xl mx-auto">
          Profitiere mit SnapFare Premium von noch mehr Deals und verpasse nie
          wieder Meilendeals, Business auf der Langstrecke oder Error Fares.
        </p>
      </div>

      <main className="py-16">
        <div className="max-w-7xl mx-auto px-4">
          <div className="grid md:grid-cols-2 gap-8 max-w-5xl mx-auto">

            {/* Free Plan */}
            <div className="bg-white/5 border border-white/10 rounded-2xl p-8">
              <div className="text-center mb-8">
                <h3 className="text-2xl font-bold mb-2 text-white">Free</h3>
                <div className="text-4xl font-bold text-white mb-2">CHF 0</div>
                <p className="text-gray-300">Pro Jahr</p>
              </div>

              <ul className="space-y-4 mb-8">
                <li className="flex items-center gap-3">
                  <Check className="w-5 h-5 text-green-400" />
                  <span className="text-gray-300">Newsletter mit den besten Reisedeals</span>
                </li>
              </ul>

              <Button
                variant="secondary"
                className="w-full bg-white/10 text-gray-300 border-white/20"
                disabled
              >
                Aktueller Plan
              </Button>
            </div>

            {/* Premium Plan */}
            <div className="bg-white/5 border-2 border-green-400 rounded-2xl p-8 relative">
              <div className="absolute top-4 right-4">
                <div className="bg-gradient-to-r from-green-500 to-blue-500 text-white px-3 py-1 rounded-full text-sm font-medium flex items-center gap-1">
                  <Star className="w-4 h-4" />
                  Beliebt
                </div>
              </div>

              <div className="text-center mb-8">
                <span className="inline-block bg-green-400 text-black text-xs font-semibold px-3 py-1 rounded-full mb-2">
                  Frühling 2026
                </span>
                <h3 className="text-2xl font-bold mb-2 text-white">Premium</h3>
                <span className="text-4xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                  CHF 49
                </span>
                <p className="text-gray-300 text-sm">pro Jahr</p>
              </div>

              <ul className="space-y-4 mb-8">
                {[
                  "Newsletter mit den besten Reisedeals",
                  "Exklusive Meilendeals ab der Schweiz",
                  "Businessclass-Deals auf der Langstrecke",
                  "Frühzugang zur personalisierten Flugsuche",
                  "Frühzugang zur 1-Click-Flugbuchungsplattform",
                  "Vergünstigte Beratungen zu den besten Deals",
                  "Zugang zur exklusiven SnapFare Community"
                ].map((text) => (
                  <li key={text} className="flex items-center gap-3">
                    <Check className="w-5 h-5 text-green-400" />
                    <span className="text-gray-300">{text}</span>
                  </li>
                ))}
              </ul>

              <form onSubmit={handleUpgrade} className="space-y-4">
                <Input
                  type="email"
                  placeholder="you@example.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                  className="bg-white/10 border-white/20 text-white placeholder:text-gray-400"
                />
                <button
                  type="submit"
                  disabled={isSubmitting || !email}
                  className="w-full h-[58px] flex justify-center items-center disabled:opacity-50"
                >
                  <img src={twintButton} alt="Mit TWINT bezahlen" className="h-[58px]" />
                </button>
              </form>
            </div>
          </div>
        </div>
      </main>

      <footer className="bg-slate-900 py-8 sm:py-12 relative left-1/2 right-1/2 -mx-[50vw] w-screen">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex flex-col md:flex-row justify-between items-center gap-4">
            <div className="flex items-center gap-5 mb-4 md:mb-0">
              <span className="text-xl sm:text-2xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                SnapFare
              </span>
            </div>
      
            <div className="flex flex-col sm:flex-row items-center gap-4 text-center">
              <div className="flex gap-6 text-sm">
                <a href="/impressum" className="text-gray-400 hover:text-white transition-colors">
                  Impressum
                </a>
                <a href="/datenschutz" className="text-gray-400 hover:text-white transition-colors">
                  Datenschutzerklärung
                </a>
              </div>
      
              <p className="text-gray-400 text-xs sm:text-sm">
                © 2025 SnapFare. Alle Rechte vorbehalten.
              </p>
            </div>
          </div>
        </div>
      </footer>

    </div>
  );
};

export default Premium;
