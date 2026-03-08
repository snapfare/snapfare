import React, { useState, useEffect, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import { supabase } from "@/integrations/supabase/client";
import { useNavigate, Link } from "react-router-dom";
import {
  LogOut, Settings, Plane, SlidersHorizontal, X, Loader2, ChevronDown, Sparkles,
} from "lucide-react";
import { usePremiumGate } from "@/hooks/usePremiumGate";
import { usePersonalizedDeals } from "@/hooks/usePersonalizedDeals";
import { useQueryClient } from "@tanstack/react-query";
import DealCard from "@/components/DealCard";
import DealsChatPanel from "@/components/DealsChatPanel";
import OnboardingScreen from "@/components/dashboard/OnboardingScreen";
import { REGION_LABELS } from "@/lib/regionMapping";
import type { Region } from "@/lib/regionMapping";

// ─── Preference form config ───────────────────────────────────────────────────
const ORIGINS = [
  { code: "ZRH", label: "Zürich (ZRH)" },
  { code: "GVA", label: "Genf (GVA)" },
  { code: "BSL", label: "Basel (BSL)" },
];

const CABIN_OPTIONS = [
  { value: "Economy", label: "Economy" },
  { value: "Business", label: "Business" },
  { value: "First", label: "First" },
];

const SEASON_OPTIONS = [
  { value: "summer", label: "Sommer (Jun–Aug)" },
  { value: "winter", label: "Winter (Dez–Feb)" },
  { value: "spring_fall", label: "Frühling / Herbst" },
  { value: "any", label: "Jederzeit" },
];

const REGION_OPTIONS = (Object.keys(REGION_LABELS) as Region[]).map((r) => ({
  value: r,
  label: REGION_LABELS[r],
}));

interface PrefsForm {
  preferred_origins: string[];
  preferred_regions: string[];
  max_price_chf: string;
  cabin_classes: string[];
  min_trip_days: string;
  max_trip_days: string;
  preferred_seasons: string[];
  flight_types: string[];
}

const DEFAULT_PREFS: PrefsForm = {
  preferred_origins: ["ZRH", "GVA", "BSL"],
  preferred_regions: [],
  max_price_chf: "",
  cabin_classes: ["Economy"],
  min_trip_days: "2",
  max_trip_days: "",
  preferred_seasons: ["any"],
  flight_types: ["short_haul", "long_haul"],
};

function toggleItem<T>(arr: T[], item: T): T[] {
  return arr.includes(item) ? arr.filter((x) => x !== item) : [...arr, item];
}

const DEALS_PER_PAGE = 6;

function getTimeGreeting(): string {
  const h = new Date().getHours();
  if (h >= 5 && h < 12) return "Guten Morgen";
  if (h >= 12 && h < 18) return "Guten Mittag";
  return "Guten Abend";
}

// ─── Main Dashboard ───────────────────────────────────────────────────────────
const Dashboard = () => {
  const { isLoading: authLoading, isAuthenticated, isPremium, tier, user } = usePremiumGate();
  const { deals, prefs, isLoading: dealsLoading, refetchDeals } = usePersonalizedDeals(user?.id);
  const [showPrefs, setShowPrefs] = useState(false);
  const [form, setForm] = useState<PrefsForm>(DEFAULT_PREFS);
  const [isSaving, setIsSaving] = useState(false);
  const [visibleCount, setVisibleCount] = useState(DEALS_PER_PAGE);
  const [showOnboarding, setShowOnboarding] = useState(false);
  const onboardingDismissed = useRef(false);
  const { toast } = useToast();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  // Get user display name
  const userName: string =
    (prefs as any)?.full_name ||
    user?.user_metadata?.full_name ||
    user?.email?.split("@")[0] ||
    "";

  // Populate form when prefs load
  useEffect(() => {
    if (prefs) {
      setForm({
        preferred_origins: prefs.preferred_origins ?? ["ZRH"],
        preferred_regions: prefs.preferred_regions ?? [],
        max_price_chf: prefs.max_price_chf ? String(prefs.max_price_chf) : "",
        cabin_classes: prefs.cabin_classes ?? ["Economy"],
        min_trip_days: String(prefs.min_trip_days ?? 2),
        max_trip_days: prefs.max_trip_days ? String(prefs.max_trip_days) : "",
        preferred_seasons: prefs.preferred_seasons ?? ["any"],
        flight_types: prefs.flight_types ?? ["short_haul", "long_haul"],
      });

      // Only show onboarding if not yet completed and not already dismissed this session
      if (!onboardingDismissed.current) {
        const completed = (prefs as any)?.onboarding_completed;
        if (completed === false || completed === null || completed === undefined) {
          setShowOnboarding(true);
        }
      }
    } else if (!dealsLoading && user && prefs === null && !onboardingDismissed.current) {
      // No preferences record at all → new user, show onboarding
      setShowOnboarding(true);
    }
  }, [prefs, dealsLoading, user]);

  // Redirect unauthenticated users
  useEffect(() => {
    if (!authLoading && !isAuthenticated) {
      navigate("/auth");
    }
  }, [authLoading, isAuthenticated, navigate]);

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    navigate("/");
  };

  const handleSavePreferences = async () => {
    if (!user) return;
    setIsSaving(true);

    try {
      const payload = {
        user_id: user.id,
        email: user.email ?? "",
        preferred_origins: form.preferred_origins,
        preferred_regions: form.preferred_regions,
        max_price_chf: form.max_price_chf ? parseInt(form.max_price_chf, 10) : null,
        cabin_classes: form.cabin_classes,
        min_trip_days: parseInt(form.min_trip_days, 10) || 2,
        max_trip_days: form.max_trip_days ? parseInt(form.max_trip_days, 10) : null,
        preferred_seasons: form.preferred_seasons,
        flight_types: form.flight_types,
        updated_at: new Date().toISOString(),
      };

      const { error } = await supabase
        .from("user_preferences")
        .upsert(payload, { onConflict: "user_id" });

      if (error) throw error;

      toast({ title: "Präferenzen gespeichert!", description: "Deine Deals werden aktualisiert." });
      queryClient.invalidateQueries({ queryKey: ["user-preferences", user.id] });
      queryClient.invalidateQueries({ queryKey: ["personalized-deals", user.id] });
      setShowPrefs(false);
    } catch {
      toast({ title: "Fehler", description: "Speichern fehlgeschlagen.", variant: "destructive" });
    } finally {
      setIsSaving(false);
    }
  };

  // Loading
  if (authLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-900">
        <Loader2 className="w-8 h-8 text-green-400 animate-spin" />
      </div>
    );
  }

  // Active filter chips
  const activeFilters: string[] = [];
  if (prefs?.preferred_origins?.length) activeFilters.push(prefs.preferred_origins.join(", "));
  if (prefs?.preferred_regions?.length)
    activeFilters.push(...prefs.preferred_regions.map((r) => REGION_LABELS[r as Region] ?? r));
  if (prefs?.max_price_chf) activeFilters.push(`≤ CHF ${prefs.max_price_chf}`);

  const visibleDeals = deals.slice(0, visibleCount);
  const hasMore = visibleCount < deals.length;

  return (
    <div className="min-h-screen bg-slate-900">
      {/* Onboarding overlay */}
      {showOnboarding && user && (
        <OnboardingScreen
          userId={user.id}
          userEmail={user.email ?? ""}
          userName={userName}
          onComplete={() => {
            onboardingDismissed.current = true;
            setShowOnboarding(false);
            queryClient.invalidateQueries({ queryKey: ["user-preferences", user.id] });
            queryClient.invalidateQueries({ queryKey: ["personalized-deals", user.id] });
          }}
        />
      )}

      {/* Header */}
      <header className="bg-slate-900 border-b border-white/10 sticky top-0 z-40 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <span className="font-bold text-xl bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
              SnapFare
            </span>
            {isPremium && (
              <Badge className="bg-amber-500/20 text-amber-300 border border-amber-500/30 text-[10px] ml-1 py-0">
                Premium
              </Badge>
            )}
          </Link>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowPrefs(!showPrefs)}
              className="flex items-center gap-1.5 bg-white/5 border-white/20 text-gray-300 hover:bg-white/10 hover:text-white h-8"
            >
              <SlidersHorizontal className="w-4 h-4" />
              <span className="hidden sm:inline text-xs">Präferenzen</span>
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={handleSignOut}
              className="flex items-center gap-1.5 text-gray-500 hover:text-gray-300 h-8"
            >
              <LogOut className="w-4 h-4" />
              <span className="hidden sm:inline text-xs">Abmelden</span>
            </Button>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6">
        {/* Welcome */}
        <div className="mb-6">
          <h1 className="text-xl font-bold text-white">
            {userName ? `${getTimeGreeting()}, ${userName} 👋` : getTimeGreeting()}
          </h1>
          <p className="text-sm text-gray-500 mt-0.5">Willkommen auf deinem persönlichen Dashboard – du kannst entweder durch die Liste deiner Deals scrollen oder deinem persönlichen Agent eine Suchanfrage geben!</p>

          {activeFilters.length > 0 && (
            <div className="flex flex-wrap gap-2 items-center mt-3">
              {activeFilters.map((f) => (
                <Badge
                  key={f}
                  className="text-xs bg-green-500/10 text-green-300 border border-green-500/20"
                >
                  {f}
                </Badge>
              ))}
              <button
                onClick={() => setShowPrefs(true)}
                className="text-xs text-gray-500 hover:text-gray-300 underline"
              >
                Ändern
              </button>
            </div>
          )}
        </div>

        <div className="flex gap-6 items-start">
          {/* Preferences side panel */}
          {showPrefs && (
            <aside className="w-72 flex-shrink-0 hidden lg:block">
              <div className="bg-white/5 border border-white/10 rounded-xl p-5 sticky top-24 max-h-[calc(100vh-8rem)] overflow-y-auto">
                <div className="flex items-center justify-between mb-5">
                  <h2 className="font-semibold text-white flex items-center gap-2 text-sm">
                    <Settings className="w-4 h-4 text-green-400" />
                    Präferenzen
                  </h2>
                  <button onClick={() => setShowPrefs(false)} className="text-gray-500 hover:text-gray-300">
                    <X className="w-4 h-4" />
                  </button>
                </div>

                <div className="space-y-5">
                  {/* Departure airports */}
                  <fieldset>
                    <legend className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2">
                      Abflughafen
                    </legend>
                    <div className="space-y-2">
                      {ORIGINS.map((o) => (
                        <div key={o.code} className="flex items-center gap-2">
                          <Checkbox
                            id={`origin-${o.code}`}
                            checked={form.preferred_origins.includes(o.code)}
                            onCheckedChange={() =>
                              setForm((f) => ({
                                ...f,
                                preferred_origins: toggleItem(f.preferred_origins, o.code),
                              }))
                            }
                            className="border-white/30 data-[state=checked]:bg-green-500 data-[state=checked]:border-green-500"
                          />
                          <Label htmlFor={`origin-${o.code}`} className="text-sm cursor-pointer font-normal text-gray-300">
                            {o.label}
                          </Label>
                        </div>
                      ))}
                    </div>
                  </fieldset>

                  {/* Destination regions */}
                  <fieldset>
                    <legend className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2">
                      Reiseregion
                    </legend>
                    <div className="space-y-2">
                      {REGION_OPTIONS.map((r) => (
                        <div key={r.value} className="flex items-center gap-2">
                          <Checkbox
                            id={`region-${r.value}`}
                            checked={form.preferred_regions.includes(r.value)}
                            onCheckedChange={() =>
                              setForm((f) => ({
                                ...f,
                                preferred_regions: toggleItem(f.preferred_regions, r.value),
                              }))
                            }
                            className="border-white/30 data-[state=checked]:bg-green-500 data-[state=checked]:border-green-500"
                          />
                          <Label htmlFor={`region-${r.value}`} className="text-sm cursor-pointer font-normal text-gray-300">
                            {r.label}
                          </Label>
                        </div>
                      ))}
                    </div>
                    {form.preferred_regions.length === 0 && (
                      <p className="text-[11px] text-gray-600 mt-1">Alle Regionen</p>
                    )}
                  </fieldset>

                  {/* Budget */}
                  <div>
                    <Label htmlFor="max-price" className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2 block">
                      Max. Budget CHF
                    </Label>
                    <Input
                      id="max-price"
                      type="number"
                      placeholder="Kein Limit"
                      value={form.max_price_chf}
                      onChange={(e) => setForm((f) => ({ ...f, max_price_chf: e.target.value }))}
                      className="h-8 text-sm bg-white/5 border-white/10 text-white placeholder:text-gray-600"
                    />
                  </div>

                  {/* Cabin */}
                  <fieldset>
                    <legend className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2">
                      Kabine
                    </legend>
                    <div className="space-y-2">
                      {CABIN_OPTIONS.map((c) => (
                        <div key={c.value} className="flex items-center gap-2">
                          <Checkbox
                            id={`cabin-${c.value}`}
                            checked={form.cabin_classes.includes(c.value)}
                            onCheckedChange={() =>
                              setForm((f) => ({
                                ...f,
                                cabin_classes: toggleItem(f.cabin_classes, c.value),
                              }))
                            }
                            className="border-white/30 data-[state=checked]:bg-green-500 data-[state=checked]:border-green-500"
                          />
                          <Label htmlFor={`cabin-${c.value}`} className="text-sm cursor-pointer font-normal text-gray-300">
                            {c.label}
                          </Label>
                        </div>
                      ))}
                    </div>
                  </fieldset>

                  {/* Trip duration */}
                  <div>
                    <Label className="text-[10px] font-semibold text-gray-500 uppercase tracking-wider mb-2 block">
                      Reisedauer (Tage)
                    </Label>
                    <div className="flex items-center gap-2">
                      <Input
                        type="number"
                        placeholder="Min"
                        value={form.min_trip_days}
                        onChange={(e) => setForm((f) => ({ ...f, min_trip_days: e.target.value }))}
                        className="h-8 text-sm bg-white/5 border-white/10 text-white placeholder:text-gray-600"
                      />
                      <span className="text-gray-600">–</span>
                      <Input
                        type="number"
                        placeholder="Max"
                        value={form.max_trip_days}
                        onChange={(e) => setForm((f) => ({ ...f, max_trip_days: e.target.value }))}
                        className="h-8 text-sm bg-white/5 border-white/10 text-white placeholder:text-gray-600"
                      />
                    </div>
                  </div>

                </div>

                <Button
                  onClick={handleSavePreferences}
                  disabled={isSaving}
                  className="w-full mt-5 bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white h-9 text-sm"
                >
                  {isSaving ? (
                    <><Loader2 className="w-3 h-3 mr-2 animate-spin" />Speichern...</>
                  ) : (
                    "Speichern"
                  )}
                </Button>
              </div>
            </aside>
          )}

          {/* Main content */}
          <div className="flex-1 min-w-0 space-y-6">
            {/* SnapFare Agent */}
            <div>
              <div className="flex items-center gap-2 mb-3">
                <Sparkles className="w-4 h-4 text-green-400" />
                <h2 className="text-sm font-semibold text-white">SnapFare Agent</h2>
              </div>
              <DealsChatPanel userName={userName} />
            </div>

            {/* Deals section */}
            <div>
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-sm font-semibold text-white flex items-center gap-2">
                  <Plane className="w-4 h-4 text-green-400" />
                  Deine Deals
                </h2>
                {deals.length > 0 && (
                  <span className="text-xs text-gray-600">
                    {Math.min(visibleCount, deals.length)} von {deals.length}
                  </span>
                )}
              </div>

              {dealsLoading ? (
                <div className="flex items-center justify-center py-24">
                  <Loader2 className="w-8 h-8 text-green-400 animate-spin" />
                </div>
              ) : deals.length === 0 ? (
                <div className="text-center py-24 bg-white/5 border border-white/10 rounded-xl">
                  <Plane className="w-12 h-12 text-gray-600 mx-auto mb-4" />
                  <p className="font-medium text-gray-300 mb-1">Keine Deals gefunden</p>
                  <p className="text-sm text-gray-500">
                    Passe deine Präferenzen an oder warte auf neue Deals.
                  </p>
                  <Button
                    variant="outline"
                    size="sm"
                    className="mt-4 text-sm bg-white/5 border-white/20 text-gray-300 hover:bg-white/10"
                    onClick={() => setShowPrefs(true)}
                  >
                    Präferenzen öffnen
                  </Button>
                </div>
              ) : (
                <>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                    {visibleDeals.map((deal) => (
                      <DealCard key={deal.id} deal={deal} />
                    ))}
                  </div>

                  {hasMore && (
                    <div className="text-center mt-6">
                      <Button
                        variant="outline"
                        onClick={() => setVisibleCount((c) => c + DEALS_PER_PAGE)}
                        className="bg-white/5 border-white/20 text-gray-300 hover:bg-white/10 hover:text-white gap-2"
                      >
                        <ChevronDown className="w-4 h-4" />
                        Mehr Deals laden ({deals.length - visibleCount} weitere)
                      </Button>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
