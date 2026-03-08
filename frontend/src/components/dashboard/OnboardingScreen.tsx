import React, { useState } from 'react';
import { Plane, ChevronRight, Check, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { useToast } from '@/hooks/use-toast';

const BUDGET_OPTIONS = [
  { label: 'Budgetreise', desc: 'bis CHF 500', value: 500 },
  { label: 'Mittelklasse', desc: 'bis CHF 1200', value: 1200 },
  { label: 'Komfort', desc: 'bis CHF 2500', value: 2500 },
  { label: 'Premium', desc: 'kein Limit', value: null },
];

const CLASS_OPTIONS = [
  { value: ['Economy'], label: 'Economy', emoji: '✈️', desc: 'Günstigste Option' },
  { value: ['Business'], label: 'Business', emoji: '🌟', desc: 'Mehr Komfort' },
  { value: ['Economy', 'Business'], label: 'Beide', emoji: '🎯', desc: 'Alles anzeigen' },
];

// Continent → region mapping (matches regionMapping.ts Region type)
const CONTINENT_OPTIONS = [
  { label: 'Europa', emoji: '🏰', region: 'europe' },
  { label: 'Asien / Pazifik', emoji: '🏯', region: 'asia_pacific' },
  { label: 'Amerika', emoji: '🗽', region: 'americas' },
  { label: 'Afrika', emoji: '🦁', region: 'africa' },
  { label: 'Naher Osten', emoji: '🕌', region: 'middle_east' },
  { label: 'Ozeanien', emoji: '🦘', region: 'oceania' },
];

type OnboardingScreenProps = {
  userId: string;
  userEmail: string;
  userName: string;
  onComplete: () => void;
};

const OnboardingScreen: React.FC<OnboardingScreenProps> = ({ userId, userEmail, userName, onComplete }) => {
  const { toast } = useToast();
  const [step, setStep] = useState(1);
  const [isSaving, setIsSaving] = useState(false);

  const [budget, setBudget] = useState<number | null | undefined>(undefined); // undefined = not chosen yet
  const [cabinClasses, setCabinClasses] = useState<string[]>(['Economy']);
  const [selectedRegions, setSelectedRegions] = useState<string[]>([]);

  const toggleRegion = (region: string) => {
    setSelectedRegions(prev =>
      prev.includes(region) ? prev.filter(r => r !== region) : [...prev, region]
    );
  };

  const handleComplete = async () => {
    setIsSaving(true);

    try {
      const { error } = await supabase
        .from('user_preferences')
        .upsert({
          user_id: userId,
          email: userEmail,
          full_name: userName || null,
          preferred_origins: ['ZRH', 'GVA', 'BSL'],
          max_price_chf: budget ?? null,
          cabin_classes: cabinClasses,
          preferred_regions: selectedRegions,
          onboarding_completed: true,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_id' });

      if (error) throw error;

      onComplete();
    } catch (err) {
      console.error("[OnboardingScreen] upsert error:", err);
      toast({ title: "Fehler", description: "Konnte nicht gespeichert werden. Versuche es erneut.", variant: "destructive" });
    } finally {
      setIsSaving(false);
    }
  };

  const displayName = userName || 'dort';

  return (
    <div className="fixed inset-0 bg-slate-900/95 backdrop-blur-sm z-50 flex items-center justify-center px-4">
      <div className="w-full max-w-lg">
        {/* Logo */}
        <div className="text-center mb-8">
          <div className="flex items-center justify-center gap-2 mb-1">
            <Plane className="w-5 h-5 text-green-400" />
            <span className="text-2xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
              SnapFare
            </span>
          </div>
        </div>

        {/* Progress bar */}
        <div className="flex items-center gap-2 mb-6">
          {[1, 2, 3].map((s) => (
            <div
              key={s}
              className={`h-1.5 flex-1 rounded-full transition-all duration-300 ${
                s <= step
                  ? 'bg-gradient-to-r from-green-500 to-blue-500'
                  : 'bg-white/10'
              }`}
            />
          ))}
        </div>

        <div className="bg-white/5 border border-white/10 rounded-2xl p-6">
          {/* Step 1: Budget */}
          {step === 1 && (
            <div className="space-y-5">
              <div>
                <h2 className="text-2xl font-bold text-white">
                  Willkommen, {displayName}! 👋
                </h2>
                <p className="text-gray-400 mt-1 text-sm">
                  Lass uns kurz dein Profil einrichten, damit du nur relevante Deals siehst.
                </p>
              </div>

              <div className="space-y-2">
                <p className="text-gray-200 font-medium text-sm">Was ist dein ungefähres Budget?</p>
                <div className="grid grid-cols-2 gap-3">
                  {BUDGET_OPTIONS.map((opt) => (
                    <button
                      key={String(opt.value)}
                      onClick={() => setBudget(opt.value)}
                      className={`p-3 rounded-xl border text-left transition-all duration-200 ${
                        budget === opt.value && budget !== undefined
                          ? 'border-green-400 bg-green-400/10 text-white'
                          : 'border-white/10 bg-white/5 text-gray-300 hover:border-white/20 hover:bg-white/8'
                      }`}
                    >
                      <p className="font-medium text-sm">{opt.label}</p>
                      <p className="text-xs text-gray-500 mt-0.5">{opt.desc}</p>
                      {budget === opt.value && budget !== undefined && (
                        <Check className="w-3.5 h-3.5 text-green-400 mt-1" />
                      )}
                    </button>
                  ))}
                </div>
              </div>

              <Button
                onClick={() => setStep(2)}
                disabled={budget === undefined}
                className="w-full bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold h-11"
              >
                Weiter
                <ChevronRight className="w-4 h-4 ml-1" />
              </Button>
            </div>
          )}

          {/* Step 2: Cabin class */}
          {step === 2 && (
            <div className="space-y-5">
              <div>
                <h2 className="text-xl font-bold text-white">Welche Klasse bevorzugst du?</h2>
                <p className="text-gray-400 mt-1 text-sm">Wir filtern deine Deals entsprechend.</p>
              </div>

              <div className="grid grid-cols-3 gap-3">
                {CLASS_OPTIONS.map((opt) => {
                  const isSelected = JSON.stringify(cabinClasses.sort()) === JSON.stringify(opt.value.slice().sort());
                  return (
                    <button
                      key={opt.label}
                      onClick={() => setCabinClasses(opt.value)}
                      className={`p-4 rounded-xl border text-center transition-all duration-200 ${
                        isSelected
                          ? 'border-green-400 bg-green-400/10'
                          : 'border-white/10 bg-white/5 hover:border-white/20'
                      }`}
                    >
                      <div className="text-2xl mb-1">{opt.emoji}</div>
                      <p className={`text-sm font-medium ${isSelected ? 'text-white' : 'text-gray-300'}`}>
                        {opt.label}
                      </p>
                      <p className={`text-[10px] mt-0.5 ${isSelected ? 'text-green-300' : 'text-gray-500'}`}>
                        {opt.desc}
                      </p>
                    </button>
                  );
                })}
              </div>

              <div className="flex gap-3">
                <Button
                  variant="outline"
                  onClick={() => setStep(1)}
                  className="flex-1 bg-white/5 border-white/20 text-gray-300 hover:bg-white/10"
                >
                  Zurück
                </Button>
                <Button
                  onClick={() => setStep(3)}
                  className="flex-1 bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold h-11"
                >
                  Weiter
                  <ChevronRight className="w-4 h-4 ml-1" />
                </Button>
              </div>
            </div>
          )}

          {/* Step 3: Regions */}
          {step === 3 && (
            <div className="space-y-5">
              <div>
                <h2 className="text-xl font-bold text-white">Wohin möchtest du reisen?</h2>
                <p className="text-gray-400 mt-1 text-sm">
                  Wähle alle Regionen aus, die dich interessieren. Du kannst das später anpassen.
                </p>
              </div>

              <div className="grid grid-cols-3 gap-3">
                {CONTINENT_OPTIONS.map((c) => (
                  <button
                    key={c.region}
                    onClick={() => toggleRegion(c.region)}
                    className={`p-3 rounded-xl border text-center transition-all duration-200 ${
                      selectedRegions.includes(c.region)
                        ? 'border-green-400 bg-green-400/10'
                        : 'border-white/10 bg-white/5 hover:border-white/20'
                    }`}
                  >
                    <div className="text-xl mb-1">{c.emoji}</div>
                    <p className={`text-xs font-medium ${selectedRegions.includes(c.region) ? 'text-white' : 'text-gray-400'}`}>
                      {c.label}
                    </p>
                  </button>
                ))}
              </div>

              <div className="flex gap-3">
                <Button
                  variant="outline"
                  onClick={() => setStep(2)}
                  className="flex-1 bg-white/5 border-white/20 text-gray-300 hover:bg-white/10"
                >
                  Zurück
                </Button>
                <Button
                  onClick={handleComplete}
                  disabled={isSaving}
                  className="flex-1 bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold h-11"
                >
                  {isSaving ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Speichern...
                    </>
                  ) : (
                    <>
                      <Check className="w-4 h-4 mr-1" />
                      Fertig!
                    </>
                  )}
                </Button>
              </div>
            </div>
          )}
        </div>

        {/* Skip */}
        {step < 3 && (
          <div className="text-center mt-4">
            <button
              onClick={handleComplete}
              disabled={isSaving}
              className="text-gray-600 hover:text-gray-400 text-xs transition-colors"
            >
              Überspringen
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default OnboardingScreen;
