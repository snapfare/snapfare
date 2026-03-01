import { useState, useEffect } from 'react';
import {
  Plane, Clock, Target, Bell, CreditCard, Shield, User, ArrowRight, Bug
} from 'lucide-react';
import { SiTiktok, SiYoutube } from 'react-icons/si';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent } from '@/components/ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from '@/components/ui/dialog';
import { toast } from '@/hooks/use-toast';
import { supabase } from '@/integrations/supabase/client';

type Deal = {
  id: string;
  from: string;
  to: string;
  airline: string;
  airtime: string;
  travelClass: string;
  baggage: string;
  dates: string;
  price: string;     // z.B. "CHF 430"
  image: string;
};

const Index = () => {
  
  const [email, setEmail] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [userLocation, setUserLocation] = useState<string>('');

  // Modal state + ausgewählter Deal
  const [isDealModalOpen, setIsDealModalOpen] = useState(false);
  const [selectedDeal, setSelectedDeal] = useState<Deal | null>(null);

  // Deals
  const deals: Deal[] = [
    {
      id: "IST",
      from: "Zürich",
      to: "Istanbul",
      airline: "Turkish Airlines",
      airtime: "3 Stunden",
      travelClass: "Economy",
      baggage: "Handgepäck",
      dates: "Februar - Juni 2026",
      price: "CHF 55",
      image:
        "https://plus.unsplash.com/premium_photo-1681929370651-cc8391395d81?fm=jpg&q=60&w=3000&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxzZWFyY2h8NXx8aXN0YW5idWx8ZW58MHx8MHx8fDA%3D",
    },
    {
      id: "BCN",
      from: "Basel",
      to: "Barcelona",
      airline: "Easyjet",
      airtime: "2 Stunden",
      travelClass: "Economy",
      baggage: "Handgepäck",
      dates: "März, Mai, Juni, und Juli 2026",
      price: "CHF 42",
      image:
        "https://images.unsplash.com/photo-1599484233778-5bdc6b69bbe3?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=MnwxMjA3fDB8MXxzZWFyY2h8Mnx8YmFyY2Vsb25hJTIwYmVhY2h8fDB8fHx8MTYxOTAwNjc4NA&ixlib=rb-1.2.1&q=80&w=1080",
    },
    {
      id: "CMB",
      from: "Zürich",
      to: "Sri Lanka",
      airline: "Etihad",
      airtime: "12 Stunden",
      travelClass: "Economy",
      baggage: "8 + 25 kg",
      dates: "März - Juni 2026",
      price: "CHF 450",
      image:
        "https://images.unsplash.com/photo-1743585825000-5f2a75e59f20?fm=jpg&q=60&w=3000&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D",
    },
  ];

useEffect(() => {
  // 1️⃣ Capture UTM source once
  const params = new URLSearchParams(window.location.search);
  const utmSource = params.get("utm_source");

  if (utmSource) {
    sessionStorage.setItem("utm_source", utmSource.toLowerCase());
  }

  // 2️⃣ Existing location logic (unchanged)
  const getUserLocation = async () => {
    try {
      const response = await fetch('https://ipapi.co/json/');
      const data = await response.json();
      setUserLocation(`${data.city}, ${data.country_name}`);
    } catch (error) {
      console.log('Could not get location:', error);
      setUserLocation('Unknown');
    }
  };

  getUserLocation();
}, []);

  const openDealModal = (deal: Deal) => {
    setSelectedDeal(deal);
    setIsDealModalOpen(true);
  
    // ✅ TikTok Event: Deal View
    window.ttq?.track('ViewContent', {
      content_name: `${deal.from} → ${deal.to}`,
      value: deal.price,
    });
  };

  const handleWaitlistSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email.trim()) return;

    setIsSubmitting(true);

    try {
      const dealLabel =
        selectedDeal
          ? `${selectedDeal.from} → ${selectedDeal.to} (${selectedDeal.airline}) – ${selectedDeal.price}`
          : null;

      const utm_source = sessionStorage.getItem("utm_source") ?? "direct";
      const { error } = await supabase
        .from('waitlist')
        .insert([
          {
            email: email.trim().toLowerCase(),
            location: userLocation,
            // source_deal: dealLabel, // <-- Optional: Spalte vorher mit ALTER TABLE anlegen
          }
        ]);

      if (error) {
        console.error('Waitlist error:', error);
        throw error;
      }

      // ✅ TikTok Conversion: Waitlist Signup
      window.ttq?.track('CompleteRegistration');

      try {
        const res = await fetch(
          'https://wwoowwnjrepokmjgxhlw.functions.supabase.co/confirm',
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              email: email.trim().toLowerCase(),
            }),
          }
        );
        
        
      } catch (error) {
        console.error('Confirmation email error (non-blocking):', error);
      }

      toast({
        title: 'Erfolgreich angemeldet! 🎉',
        description: 'Bitte bestätige deine Email, um alle Deals zu erhalten!'
      });

      setEmail('');
      setIsDealModalOpen(false);
    } catch (error) {
      console.error('Error joining waitlist:', error);
      toast({
        title: 'Fehler beim Anmelden',
        description: 'Bitte versuche es noch einmal. Falls das Problem bestehen bleibt, kontaktiere uns.',
        variant: 'destructive'
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const features = [
    {
      icon: <Clock className="w-8 h-8" />,
      title: 'Wunschdeal festlegen',
      description: 'Teile SnapFare mit, wonach du suchst – in 30 Sekunden erledigt.'
    },
    {
      icon: <Bell className="w-8 h-8" />,
      title: 'Benachrichtigung erhalten',
      description: 'Findet SnapFare einen passenden Flugdeal, bekommst du sofort eine E-Mail.'
    },
    {
      icon: <Target className="w-8 h-8" />,
      title: 'Buchen & Sparen',
      description: 'In der Benachrichtigung findest du direkt den Buchungslink zum Angebot.'
    }
  ];

  const socialLinks = [
    { href: 'https://www.tiktok.com/@snapfare', label: 'TikTok', Icon: SiTiktok },
    { href: 'https://www.youtube.com/@snapfare_ch', label: 'YouTube', Icon: SiYoutube },
  ];

  return (
    <div className="bg-slate-900">
      {/* Hero Section */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-r from-green-500/10 to-blue-500/10"></div>
        <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-12 sm:pt-16 lg:pt-20 pb-0">
          {/* Premium Button */}
          <div className="absolute top-4 right-4 sm:top-6 sm:right-6 lg:top-8 lg:right-8">
            <a
              href="/premium"
              className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white px-4 py-2 rounded-lg font-semibold text-sm transition-all duration-300"
            >
              Premium
            </a>
          </div>

          <div className="text-center">
            <div className="flex justify-center mb-4 sm:mb-6 lg:mb-8">
              <h1 className="text-2xl sm:text-3xl lg:text-4xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                SnapFare
              </h1>
            </div>
            <h1 className="text-3xl sm:text-4xl md:text-5xl lg:text-6xl xl:text-7xl font-bold text-white mb-4 sm:mb-6 leading-tight px-2">
              Deine <span className="bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">vollautomatisierte</span>
              <br />
              Schnäppchenjagd
            </h1>

            {/* GEÄNDERTER TEXT HIER */}
            <p className="text-base sm:text-lg md:text-xl lg:text-2xl text-gray-300 mb-6 sm:mb-8 max-w-4xl mx-auto leading-relaxed px-4">
              Melde dich jetzt an, um alle <span className="text-green-400">Flugdeals</span> der letzten Woche via Email zu erhalten!
            </p>

            {/* Waitlist Form (Top) */}
            <form onSubmit={handleWaitlistSubmit} className="max-w-md mx-auto mb-8 sm:mb-12 px-4">
              <div className="flex flex-col sm:flex-row gap-3 sm:gap-4">
                <Input
                  type="email"
                  placeholder="Deine E-Mail-Adresse"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                  className="flex-1 bg-white/10 border-white/20 text-white placeholder:text-gray-400 h-12 text-base"
                />
                <Button
                  type="submit"
                  disabled={isSubmitting}
                  className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 h-12 px-6 sm:px-8 text-sm sm:text-base whitespace-nowrap"
                >
                  {isSubmitting ? (
                    <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                  ) : (
                    <>
                      Jetzt anmelden! <ArrowRight className="ml-2 h-4 w-4" />
                    </>
                  )}
                </Button>
              </div>
              <p className="text-xs sm:text-sm text-gray-400 mt-3">Kostenlos anmelden! Kein Spam, versprochen!</p>
            </form>

            {/* Flight Deals Section */}
            <div className="max-w-5xl mx-auto px-4">
              {/* Dieser Absatz wurde ENTFERNT:
                  <p className="text-base sm:text-lg md:text-xl lg:text-2xl text-gray-300 mb-6 sm:mb-8 max-w-4xl mx-auto leading-relaxed px-4">
                    Melde dich jetzt an, um alle <span className="text-green-400">Flugdeals</span> der letzten Woche via Email zu erhalten!
                  </p>
              */}

              <div className="grid grid-cols-1 md:grid-cols-3 gap-6 sm:gap-8 mb-10 md:mb-14 lg:mb-16">
                {deals.map((deal) => (
                  <div
                    key={deal.id}
                    onClick={() => openDealModal(deal)}
                    role="button"
                    tabIndex={0}
                    aria-label={`Deal öffnen: ${deal.from} nach ${deal.to}`}
                    onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && openDealModal(deal)}
                    className="bg-white/10 border border-white/20 rounded-lg p-0 flex flex-col hover:bg-white/15 transition-all duration-300 overflow-hidden cursor-pointer focus:outline-none focus:ring-2 focus:ring-green-500"
                  >
                    <div className="relative h-40 sm:h-44 w-full">
                      <img src={deal.image} className="object-cover w-full h-full" loading="lazy" />
                      <div className="absolute inset-0 bg-gradient-to-t from-black/40 to-transparent" />
                    </div>

                    <div className="p-6 flex-1 flex flex-col justify-between">
                      <div>
                        <div className="flex items-center justify-center gap-3 mb-4">
                          <span className="text-xl font-bold text-white">{deal.from}</span>
                          <ArrowRight className="h-6 w-6 text-green-400" />
                          <span className="text-xl font-bold text-white">{deal.to}</span>
                        </div>

                        <div className="text-center mb-4">
                          <div className="text-xm font-semibold text-white">{deal.airline}</div>
                          <div className="text-xm text-gray-300">✈️Flugdauer: {deal.airtime}</div>
                          <div className="text-xm text-gray-300">💳Reiseklasse: {deal.travelClass}</div>
                          <div className="text-xm text-gray-300">🧳Gepäck: {deal.baggage}</div>
                        </div>

                        <div className="text-center mb-3">
                          <div className="text-xm font-semibold text-white">Mögliche Reisedaten:</div>
                          <div className="text-sm text-white">{deal.dates}</div>
                        </div>
                      </div>

                      <div className="text-center">
                        <div className="text-xs text-gray-400 mb-1">Preis pro Person</div>
                        <div className="text-3xl font-bold text-green-400">{deal.price}</div>
                        <div className="text-xs text-gray-400">inkl. Steuern & Gebühren</div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              {/* Deal Modal (Popup) */}
              <Dialog open={isDealModalOpen} onOpenChange={setIsDealModalOpen}>
                <DialogContent className="bg-slate-900 text-white border border-white/20">
                  <DialogHeader>
                    <DialogTitle>Hol dir den Link zu folgendem Flugdeal:</DialogTitle>
                    <DialogDescription className="text-gray-300">
                      {selectedDeal && (
                        <span className="block mt-1">
                          {selectedDeal.from} → {selectedDeal.to} ({selectedDeal.airline}) – {selectedDeal.price}
                        </span>
                      )}
                    </DialogDescription>
                  </DialogHeader>

                  <p className="text-gray-300">
                    Melde dich kurz an – wir schicken dir den Direktlink zum Deal sowie weitere Deals alle zwei Wochen.
                  </p>

                  <form onSubmit={handleWaitlistSubmit} className="mt-4 flex flex-col sm:flex-row gap-3">
                    <Input
                      type="email"
                      placeholder="Deine E-Mail-Adresse"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      required
                      className="flex-1 bg-white/10 border-white/20 text-white placeholder:text-gray-400 h-12 text-base"
                    />
                    <Button
                      type="submit"
                      disabled={isSubmitting}
                      className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 h-12 px-6 sm:px-8"
                    >
                      {isSubmitting ? (
                        <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white" />
                      ) : (
                        <>Link zusenden</>
                      )}
                    </Button>
                  </form>

                  <DialogFooter>
                    <p className="text-xs text-gray-400">Kein Spam. Abmeldung jederzeit möglich.</p>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </div>

            {/* Features Section */}
            <div className="py-16 sm:py-20 lg:py-24 bg-gradient-to-b from-slate-800 to-slate-900 relative left-1/2 right-1/2 -mx-[50vw] w-screen">
              <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="text-center mb-12 sm:mb-16 lg:mb-20">
                  <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4 sm:mb-6 px-4">
                    SnapFare findet automatisch die besten <span className="text-green-400">Angebote</span>
                  </h2>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 sm:gap-8">
                  {features.map((feature, index) => (
                    <Card key={index} className="bg-white/5 border-white/10 hover:bg-white/10 transition-all duration-300 group">
                      <CardContent className="p-6 sm:p-8">
                        <div className="text-green-400 mb-4 sm:mb-6 group-hover:scale-110 transition-transform duration-300">
                          {feature.icon}
                        </div>
                        <h3 className="text-lg sm:text-xl font-semibold text-white mb-3 sm:mb-4">{feature.title}</h3>
                        <p className="text-sm sm:text-base text-gray-300 leading-relaxed">{feature.description}</p>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </div>
            </div>

            {/* Process Section */}
            <div className="py-16 sm:py-20 lg:py-24 bg-gradient-to-b from-slate-900 to-slate-800 relative left-1/2 right-1/2 -mx-[50vw] w-screen">
              <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="text-center mb-12 sm:mb-16 lg:mb-20">
                  <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4 sm:mb-6 px-4">
                    Warum <span className="text-blue-400">SnapFare</span>?
                  </h2>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 sm:gap-8">
                  <div className="text-center">
                    <div className="w-12 h-12 sm:w-16 sm:h-16 bg-gradient-to-r from-green-500 to-green-600 rounded-full flex items-center justify-center mx-auto mb-4 sm:mb-6">
                      <span className="text-white font-bold text-lg sm:text-xl">1</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white mb-3 sm:mb-4">Spare gutes Geld</h3>
                    <p className="text-sm sm:text-base text-gray-300">Die besten Flugpreise sind oft nur kurz verfügbar. SnapFare erkennt sie sofort – bevor sie wieder verschwinden.</p>
                  </div>

                  <div className="text-center">
                    <div className="w-12 h-12 sm:w-16 sm:h-16 bg-gradient-to-r from-blue-500 to-blue-600 rounded-full flex items-center justify-center mx-auto mb-4 sm:mb-6">
                      <span className="text-white font-bold text-lg sm:text-xl">2</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white mb-3 sm:mb-4">Keine endlosen Preisvergleiche</h3>
                    <p className="text-sm sm:text-base text-gray-300">Vergiss 10 Tabs und stundenlanges Suchen. SnapFare übernimmt die Recherche für dich.</p>
                  </div>

                  <div className="text-center">
                    <div className="w-12 h-12 sm:w-16 sm:h-16 bg-gradient-to-r from-purple-500 to-purple-600 rounded-full flex items-center justify-center mx-auto mb-4 sm:mb-6">
                      <span className="text-white font-bold text-lg sm:text-xl">3</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white mb-3 sm:mb-4">Nur relevante Deals</h3>
                    <p className="text-sm sm:text-base text-gray-300">Du bekommst nur Angebote, die wirklich gut sind. Kein Newsletter-Müll.</p>
                  </div>

                  <div className="text-center">
                    <div className="w-12 h-12 sm:w-16 sm:h-16 bg-gradient-to-r from-orange-500 to-orange-600 rounded-full flex items-center justify-center mx-auto mb-4 sm:mb-6">
                      <span className="text-white font-bold text-lg sm:text-xl">4</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white mb-3 sm:mb-4">Kostenlos & jederzeit kündbar</h3>
                    <p className="text-sm sm:text-base text-gray-300">Keine Gebühren. Keine Verpflichtung. Einfach anmelden und testen.</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Final CTA */}
            <div className="py-16 sm:py-20 lg:py-24 bg-gradient-to-r from-green-500/10 to-blue-500/10 relative left-1/2 right-1/2 -mx-[50vw] w-screen">
              <div className="max-w-4xl mx-auto text-center px-4 sm:px-6 lg:px-8">
                <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4 sm:mb-6">Bereit für stressfreie Schnäppchen?</h2>
                <p className="text-lg sm:text-xl text-gray-300 mb-8 sm:mb-10">Melde dich jetzt an und sei einer der ersten, die SnapFare nutzen können.</p>

                <form onSubmit={handleWaitlistSubmit} className="max-w-md mx-auto">
                  <div className="flex flex-col sm:flex-row gap-3 sm:gap-4">
                    <Input
                      type="email"
                      placeholder="Deine E-Mail-Adresse"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      required
                      className="flex-1 bg-white/10 border-white/20 text-white placeholder:text-gray-400 h-12 text-base"
                    />
                    <Button
                      type="submit"
                      disabled={isSubmitting}
                      className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 h-12 px-6 sm:px-8 text-sm sm:text-base whitespace-nowrap"
                    >
                      {isSubmitting ? (
                        <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                      ) : (
                        <>
                          Anmelden! <Plane className="ml-2 h-4 w-4" />
                        </>
                      )}
                    </Button>
                  </div>
                </form>
              </div>
            </div>

            {/* Footer */}
            <footer className="bg-slate-900 py-8 sm:py-12 relative left-1/2 right-1/2 -mx-[50vw] w-screen">
              <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="flex flex-col md:flex-row justify-between items-center gap-4">
                  <div className="flex items-center gap-5 mb-4 md:mb-0">
                    <span className="text-xl sm:text-2xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                      SnapFare
                    </span>

                    {/* Social Icons (echte Logos via react-icons) */}
                    <div className="flex items-center gap-3">
                      {socialLinks.map(({ href, label, Icon }) => (
                        <a
                          key={label}
                          href={href}
                          target="_blank"
                          rel="noopener noreferrer"
                          aria-label={label}
                          title={label}
                          className="inline-flex h-10 w-10 items-center justify-center rounded-xl
                                     bg-white/5 border border-white/10
                                     hover:bg-white/10 hover:border-white/20
                                     transition-all hover:scale-105 hover:shadow-lg
                                     focus:outline-none focus:ring-2 focus:ring-green-500/40"
                        >
                          <Icon className="h-5 w-5 text-white" />
                          <span className="sr-only">{label}</span>
                        </a>
                      ))}
                    </div>
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

                    <p className="text-gray-400 text-xs sm:text-sm">© 2025 SnapFare. Alle Rechte vorbehalten.</p>
                  </div>
                </div>
              </div>
            </footer>
          </div>
        </div>
      </div>

    </div>
  );
};

export default Index;
