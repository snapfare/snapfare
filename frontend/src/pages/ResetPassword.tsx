import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/hooks/use-toast";
import { supabase } from "@/integrations/supabase/client";
import { useNavigate, Link } from 'react-router-dom';
import { Loader2, Plane, Check, X, AlertTriangle } from 'lucide-react';
import { getPasswordStrength } from "@/lib/utils";

type Status = 'loading' | 'ready' | 'error' | 'done';

const ResetPassword = () => {
  const [status, setStatus] = useState<Status>('loading');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { toast } = useToast();
  const navigate = useNavigate();

  useEffect(() => {
    let mounted = true;

    // onAuthStateChange fires when Supabase processes the #access_token hash
    const { data: { subscription } } = supabase.auth.onAuthStateChange((event, session) => {
      if (!mounted) return;
      if (event === 'PASSWORD_RECOVERY') {
        setStatus('ready');
      } else if (event === 'SIGNED_IN' && session) {
        // Some Supabase versions emit SIGNED_IN instead of PASSWORD_RECOVERY
        setStatus('ready');
      }
    });

    // Also check if a session is already set (hash processed before listener registered)
    supabase.auth.getSession().then(({ data: { session } }) => {
      if (!mounted) return;
      if (session) {
        setStatus('ready');
      } else {
        // Give onAuthStateChange time to fire, then show error
        setTimeout(() => {
          if (mounted) setStatus(s => s === 'loading' ? 'error' : s);
        }, 6000);
      }
    });

    return () => {
      mounted = false;
      subscription.unsubscribe();
    };
  }, []);

  const handleUpdatePassword = async (e: React.FormEvent) => {
    e.preventDefault();

    if (password !== confirmPassword) {
      toast({ title: "Fehler", description: "Die Passwörter stimmen nicht überein.", variant: "destructive" });
      return;
    }
    const s = getPasswordStrength(password);
    if (!s.length || !s.number || !s.special) {
      toast({ title: "Passwort zu schwach", description: "Mindestens 8 Zeichen, eine Zahl und ein Sonderzeichen erforderlich.", variant: "destructive" });
      return;
    }

    setIsLoading(true);
    try {
      const { error } = await supabase.auth.updateUser({ password });
      if (error) {
        toast({ title: "Fehler", description: error.message, variant: "destructive" });
      } else {
        setStatus('done');
        await supabase.auth.signOut();
        setTimeout(() => navigate('/auth'), 2500);
      }
    } catch {
      toast({ title: "Fehler", description: "Ein unerwarteter Fehler ist aufgetreten.", variant: "destructive" });
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-900 flex flex-col">
      <div className="flex-1 flex items-center justify-center px-4 py-8">
        <div className="w-full max-w-md">

          {/* Logo */}
          <div className="text-center mb-8">
            <div className="flex items-center justify-center gap-2 mb-2">
              <Plane className="w-5 h-5 text-green-400" />
              <span className="text-3xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                SnapFare
              </span>
            </div>
          </div>

          <div className="bg-white/5 border border-white/10 rounded-2xl p-6 backdrop-blur-sm">

            {/* Loading */}
            {status === 'loading' && (
              <div className="text-center py-8 space-y-4">
                <Loader2 className="w-8 h-8 text-green-400 animate-spin mx-auto" />
                <p className="text-sm text-gray-400">Link wird überprüft…</p>
              </div>
            )}

            {/* Error — token expired or invalid */}
            {status === 'error' && (
              <div className="text-center py-6 space-y-4">
                <div className="w-12 h-12 bg-red-500/20 rounded-full flex items-center justify-center mx-auto border border-red-500/30">
                  <AlertTriangle className="w-6 h-6 text-red-400" />
                </div>
                <div>
                  <p className="text-white font-semibold mb-1">Link abgelaufen</p>
                  <p className="text-sm text-gray-400">
                    Der Link ist ungültig oder abgelaufen. Fordere einen neuen an.
                  </p>
                </div>
                <Link
                  to="/auth"
                  className="inline-block w-full text-center bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold h-11 leading-[44px] rounded-lg transition-all"
                >
                  Neuen Link anfordern
                </Link>
              </div>
            )}

            {/* Success */}
            {status === 'done' && (
              <div className="text-center py-6 space-y-4">
                <div className="w-12 h-12 bg-green-500/20 rounded-full flex items-center justify-center mx-auto border border-green-500/30">
                  <Check className="w-6 h-6 text-green-400" />
                </div>
                <div>
                  <p className="text-white font-semibold mb-1">Passwort gespeichert!</p>
                  <p className="text-sm text-gray-400">Du wirst zur Anmeldung weitergeleitet…</p>
                </div>
              </div>
            )}

            {/* Form */}
            {status === 'ready' && (
              <>
                <div className="text-center mb-6">
                  <div className="w-12 h-12 bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-3 border border-green-500/30">
                    <span className="text-xl">🔒</span>
                  </div>
                  <h2 className="text-white font-semibold text-lg">Neues Passwort setzen</h2>
                  <p className="text-sm text-gray-400 mt-1">Wähle ein sicheres Passwort für dein Konto.</p>
                </div>

                <form onSubmit={handleUpdatePassword} className="space-y-4">
                  <div className="space-y-1.5">
                    <Label htmlFor="new-password" className="text-gray-300 text-sm">Neues Passwort</Label>
                    <Input
                      id="new-password"
                      type="password"
                      placeholder="Mindestens 8 Zeichen"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      required
                      className="bg-white/10 border-white/20 text-white placeholder:text-gray-500 focus:border-green-400/50 focus:ring-green-400/20"
                    />
                    {password.length > 0 && (() => {
                      const s = getPasswordStrength(password);
                      return (
                        <div className="flex gap-3 pt-1">
                          {[
                            { ok: s.length, label: "8+ Zeichen" },
                            { ok: s.number, label: "Zahl" },
                            { ok: s.special, label: "Sonderzeichen" },
                          ].map(({ ok, label }) => (
                            <span key={label} className={`flex items-center gap-1 text-xs ${ok ? "text-green-400" : "text-gray-500"}`}>
                              {ok ? <Check className="w-3 h-3" /> : <X className="w-3 h-3" />}
                              {label}
                            </span>
                          ))}
                        </div>
                      );
                    })()}
                  </div>

                  <div className="space-y-1.5">
                    <Label htmlFor="confirm-password" className="text-gray-300 text-sm">Passwort bestätigen</Label>
                    <Input
                      id="confirm-password"
                      type="password"
                      placeholder="Passwort wiederholen"
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                      required
                      className="bg-white/10 border-white/20 text-white placeholder:text-gray-500 focus:border-green-400/50 focus:ring-green-400/20"
                    />
                  </div>

                  <Button
                    type="submit"
                    className="w-full bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold h-11"
                    disabled={isLoading}
                  >
                    {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Passwort speichern"}
                  </Button>
                </form>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ResetPassword;
