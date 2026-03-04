import React, { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import { supabase } from "@/integrations/supabase/client";
import { useNavigate, Link } from "react-router-dom";
import { LogOut, Settings, Plane, SlidersHorizontal, X, Loader2 } from "lucide-react";
import { usePremiumGate } from "@/hooks/usePremiumGate";
import { usePersonalizedDeals } from "@/hooks/usePersonalizedDeals";
import { useQueryClient } from "@tanstack/react-query";
import DealCard from "@/components/DealCard";
import DealsChatPanel from "@/components/DealsChatPanel";
import PremiumRequired from "@/pages/PremiumRequired";
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
  include_miles_deals: boolean;
  include_budget_deals: boolean;
}

const DEFAULT_PREFS: PrefsForm = {
  preferred_origins: ["ZRH"],
  preferred_regions: [],
  max_price_chf: "",
  cabin_classes: ["Economy"],
  min_trip_days: "2",
  max_trip_days: "",
  preferred_seasons: ["any"],
  flight_types: ["short_haul", "long_haul"],
  include_miles_deals: true,
  include_budget_deals: true,
};

function toggleItem<T>(arr: T[], item: T): T[] {
  return arr.includes(item) ? arr.filter((x) => x !== item) : [...arr, item];
}

// ─── Main Dashboard ───────────────────────────────────────────────────────────
const Dashboard = () => {
  const { isLoading: authLoading, isAuthenticated, isPremium, user } = usePremiumGate();
  const { deals, prefs, isLoading: dealsLoading, refetchDeals } = usePersonalizedDeals(user?.id);
  const [showPrefs, setShowPrefs] = useState(false);
  const [form, setForm] = useState<PrefsForm>(DEFAULT_PREFS);
  const [isSaving, setIsSaving] = useState(false);
  const { toast } = useToast();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

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
        include_miles_deals: prefs.include_miles_deals ?? true,
        include_budget_deals: prefs.include_budget_deals ?? true,
      });
    }
  }, [prefs]);

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
        include_miles_deals: form.include_miles_deals,
        include_budget_deals: form.include_budget_deals,
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
    } catch (err) {
      toast({ title: "Fehler", description: "Speichern fehlgeschlagen.", variant: "destructive" });
    } finally {
      setIsSaving(false);
    }
  };

  // Loading
  if (authLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
      </div>
    );
  }

  // Premium gate
  if (isAuthenticated && !isPremium) {
    return <PremiumRequired />;
  }

  // Active filter chips for display
  const activeFilters: string[] = [];
  if (prefs?.preferred_origins?.length) activeFilters.push(prefs.preferred_origins.join(", "));
  if (prefs?.preferred_regions?.length)
    activeFilters.push(...prefs.preferred_regions.map((r) => REGION_LABELS[r as Region] ?? r));
  if (prefs?.max_price_chf) activeFilters.push(`≤ CHF ${prefs.max_price_chf}`);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-40">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <div className="w-8 h-8 bg-gradient-to-r from-blue-600 to-purple-600 rounded-lg flex items-center justify-center">
              <Plane className="w-4 h-4 text-white" />
            </div>
            <span className="font-bold text-gray-900">SnapFare</span>
            <Badge className="bg-amber-100 text-amber-700 border-0 text-[10px] ml-1 py-0">
              Premium
            </Badge>
          </Link>

          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowPrefs(!showPrefs)}
              className="flex items-center gap-1.5 text-gray-700 h-8"
            >
              <SlidersHorizontal className="w-4 h-4" />
              <span className="hidden sm:inline text-xs">Präferenzen</span>
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={handleSignOut}
              className="flex items-center gap-1.5 text-gray-500 h-8"
            >
              <LogOut className="w-4 h-4" />
              <span className="hidden sm:inline text-xs">Abmelden</span>
            </Button>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6">
        {/* Welcome + filters */}
        <div className="mb-5">
          <h1 className="text-xl font-bold text-gray-900">
            Hallo {user?.email?.split("@")[0]} 👋
          </h1>
          <p className="text-sm text-gray-500 mb-3">Deine personalisierten Flugdeals</p>

          {activeFilters.length > 0 ? (
            <div className="flex flex-wrap gap-2 items-center">
              {activeFilters.map((f) => (
                <Badge
                  key={f}
                  className="text-xs bg-blue-50 text-blue-700 border-blue-100 border"
                >
                  {f}
                </Badge>
              ))}
              <button
                onClick={() => setShowPrefs(true)}
                className="text-xs text-gray-400 hover:text-blue-600 underline"
              >
                Ändern
              </button>
            </div>
          ) : (
            <p className="text-xs text-gray-400">
              Keine Filter aktiv — zeige Top-Deals nach Score.{" "}
              <button onClick={() => setShowPrefs(true)} className="text-blue-500 underline">
                Präferenzen setzen
              </button>
            </p>
          )}
        </div>

        <div className="flex gap-6 items-start">
          {/* Preferences side panel */}
          {showPrefs && (
            <aside className="w-72 flex-shrink-0 hidden lg:block">
              <div className="bg-white rounded-2xl border border-gray-100 shadow-sm p-5 sticky top-24 max-h-[calc(100vh-8rem)] overflow-y-auto">
                <div className="flex items-center justify-between mb-5">
                  <h2 className="font-semibold text-gray-800 flex items-center gap-2 text-sm">
                    <Settings className="w-4 h-4 text-blue-600" />
                    Präferenzen
                  </h2>
                  <button onClick={() => setShowPrefs(false)} className="text-gray-400 hover:text-gray-600">
                    <X className="w-4 h-4" />
                  </button>
                </div>

                <div className="space-y-5">
                  {/* Departure airports */}
                  <fieldset>
                    <legend className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-2">
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
                          />
                          <Label htmlFor={`origin-${o.code}`} className="text-sm cursor-pointer font-normal">
                            {o.label}
                          </Label>
                        </div>
                      ))}
                    </div>
                  </fieldset>

                  {/* Destination regions */}
                  <fieldset>
                    <legend className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-2">
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
                          />
                          <Label htmlFor={`region-${r.value}`} className="text-sm cursor-pointer font-normal">
                            {r.label}
                          </Label>
                        </div>
                      ))}
                    </div>
                    {form.preferred_regions.length === 0 && (
                      <p className="text-[11px] text-gray-400 mt-1">Alle Regionen</p>
                    )}
                  </fieldset>

                  {/* Budget */}
                  <div>
                    <Label htmlFor="max-price" className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-2 block">
                      Max. Budget CHF
                    </Label>
                    <Input
                      id="max-price"
                      type="number"
                      placeholder="Kein Limit"
                      value={form.max_price_chf}
                      onChange={(e) => setForm((f) => ({ ...f, max_price_chf: e.target.value }))}
                      className="h-8 text-sm"
                    />
                  </div>

                  {/* Cabin */}
                  <fieldset>
                    <legend className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-2">
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
                          />
                          <Label htmlFor={`cabin-${c.value}`} className="text-sm cursor-pointer font-normal">
                            {c.label}
                          </Label>
                        </div>
                      ))}
                    </div>
                  </fieldset>

                  {/* Trip duration */}
                  <div>
                    <Label className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-2 block">
                      Reisedauer (Tage)
                    </Label>
                    <div className="flex items-center gap-2">
                      <Input
                        type="number"
                        placeholder="Min"
                        value={form.min_trip_days}
                        onChange={(e) => setForm((f) => ({ ...f, min_trip_days: e.target.value }))}
                        className="h-8 text-sm"
                      />
                      <span className="text-gray-300">–</span>
                      <Input
                        type="number"
                        placeholder="Max"
                        value={form.max_trip_days}
                        onChange={(e) => setForm((f) => ({ ...f, max_trip_days: e.target.value }))}
                        className="h-8 text-sm"
                      />
                    </div>
                  </div>

                  {/* Content toggles */}
                  <fieldset>
                    <legend className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-2">
                      Deal-Typen
                    </legend>
                    <div className="space-y-2">
                      <div className="flex items-center gap-2">
                        <Checkbox
                          id="include-miles"
                          checked={form.include_miles_deals}
                          onCheckedChange={(c) =>
                            setForm((f) => ({ ...f, include_miles_deals: c as boolean }))
                          }
                        />
                        <Label htmlFor="include-miles" className="text-sm cursor-pointer font-normal">
                          Business / Meilen
                        </Label>
                      </div>
                      <div className="flex items-center gap-2">
                        <Checkbox
                          id="include-budget"
                          checked={form.include_budget_deals}
                          onCheckedChange={(c) =>
                            setForm((f) => ({ ...f, include_budget_deals: c as boolean }))
                          }
                        />
                        <Label htmlFor="include-budget" className="text-sm cursor-pointer font-normal">
                          Budget / Economy
                        </Label>
                      </div>
                    </div>
                  </fieldset>
                </div>

                <Button
                  onClick={handleSavePreferences}
                  disabled={isSaving}
                  className="w-full mt-5 bg-gradient-to-r from-blue-600 to-purple-600 text-white h-9 text-sm"
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

          {/* Deals grid */}
          <div className="flex-1 min-w-0">
            {dealsLoading ? (
              <div className="flex items-center justify-center py-24">
                <Loader2 className="w-8 h-8 text-blue-600 animate-spin" />
              </div>
            ) : deals.length === 0 ? (
              <div className="text-center py-24 text-gray-500">
                <Plane className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="font-medium mb-1">Keine Deals gefunden</p>
                <p className="text-sm text-gray-400">
                  Passe deine Präferenzen an oder warte auf neue Deals.
                </p>
                <Button
                  variant="outline"
                  size="sm"
                  className="mt-4 text-sm"
                  onClick={() => setShowPrefs(true)}
                >
                  Präferenzen öffnen
                </Button>
              </div>
            ) : (
              <>
                <p className="text-xs text-gray-400 mb-4">
                  {deals.length} Deal{deals.length !== 1 ? "s" : ""} gefunden
                </p>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                  {deals.map((deal) => (
                    <DealCard key={deal.id} deal={deal} />
                  ))}
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      {/* AI Chat Panel (floating) */}
      <DealsChatPanel />
    </div>
  );
};

export default Dashboard;
