import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useToast } from "@/hooks/use-toast";
import { supabase } from "@/integrations/supabase/client";
import { useNavigate, Link } from 'react-router-dom';
import { ArrowLeft, Loader2, Plane, Check, X } from 'lucide-react';

function getPasswordStrength(pw: string) {
  return {
    length: pw.length >= 8,
    number: /\d/.test(pw),
    special: /[^A-Za-z0-9]/.test(pw),
  };
}

type AuthTab = "login" | "register" | "reset" | "update-password";

function translateAuthError(message: string): string {
  if (message.includes("Invalid login credentials")) {
    return "E-Mail oder Passwort ist falsch.";
  }
  if (message.includes("Email not confirmed")) {
    return "E-Mail-Adresse noch nicht bestätigt. Bitte überprüfe deinen Posteingang.";
  }
  if (message.includes("already registered") || message.includes("User already registered")) {
    return "Diese E-Mail ist bereits registriert. Melde dich einfach an.";
  }
  if (message.includes("Password should be at least")) {
    return "Das Passwort muss mindestens 8 Zeichen, eine Zahl und ein Sonderzeichen enthalten.";
  }
  if (message.includes("rate limit") || message.includes("too many requests")) {
    return "Zu viele Anfragen. Bitte warte kurz und versuche es erneut.";
  }
  return message;
}

const Auth = () => {
  const [tab, setTab] = useState<AuthTab>("login");
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [resetSent, setResetSent] = useState(false);
  const { toast } = useToast();
  const navigate = useNavigate();

  useEffect(() => {
    // If ?tab=update-password is in the URL (recovery link redirect), show that tab immediately
    const urlTab = new URLSearchParams(window.location.search).get("tab");
    if (urlTab === "update-password") {
      setTab("update-password");
    }

    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      (event, session) => {
        if (event === "PASSWORD_RECOVERY") {
          // Recovery link clicked — show set-new-password form, do NOT redirect
          setTab("update-password");
        } else if (session?.user && event !== "PASSWORD_RECOVERY") {
          navigate('/dashboard');
        }
      }
    );

    supabase.auth.getSession().then(({ data: { session } }) => {
      if (session?.user && urlTab !== "update-password") {
        navigate('/dashboard');
      }
    });

    return () => subscription.unsubscribe();
  }, [navigate]);

  const handleSignIn = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    try {
      const { error } = await supabase.auth.signInWithPassword({ email, password });
      if (error) {
        toast({
          title: "Anmeldung fehlgeschlagen",
          description: translateAuthError(error.message),
          variant: "destructive",
        });
      }
    } catch {
      toast({ title: "Fehler", description: "Ein unerwarteter Fehler ist aufgetreten.", variant: "destructive" });
    } finally {
      setIsLoading(false);
    }
  };

  const handleSignUp = async (e: React.FormEvent) => {
    e.preventDefault();

    if (password !== confirmPassword) {
      toast({ title: "Fehler", description: "Die Passwörter stimmen nicht überein.", variant: "destructive" });
      return;
    }
    const pwStrength = getPasswordStrength(password);
    if (!pwStrength.length || !pwStrength.number || !pwStrength.special) {
      toast({ title: "Passwort zu schwach", description: "Mindestens 8 Zeichen, eine Zahl und ein Sonderzeichen erforderlich.", variant: "destructive" });
      return;
    }

    setIsLoading(true);
    try {
      const { data, error } = await supabase.auth.signUp({
        email,
        password,
        options: {
          emailRedirectTo: `${window.location.origin}/dashboard`,
          data: { full_name: name.trim() || null },
        },
      });
      if (error) {
        toast({ title: "Registrierung fehlgeschlagen", description: translateAuthError(error.message), variant: "destructive" });
      } else if (data.user?.identities?.length === 0) {
        toast({
          title: "E-Mail bereits registriert",
          description: "Melde dich mit deinem bestehenden Konto an oder setze dein Passwort zurück.",
        });
        setTab("login");
      } else {
        toast({
          title: "Fast geschafft!",
          description: "Bitte bestätige deine E-Mail-Adresse über den Link, den wir dir zugesendet haben.",
        });
      }
    } catch {
      toast({ title: "Fehler", description: "Ein unerwarteter Fehler ist aufgetreten.", variant: "destructive" });
    } finally {
      setIsLoading(false);
    }
  };

  const handleUpdatePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (password !== confirmPassword) {
      toast({ title: "Fehler", description: "Die Passwörter stimmen nicht überein.", variant: "destructive" });
      return;
    }
    const pwStrength = getPasswordStrength(password);
    if (!pwStrength.length || !pwStrength.number || !pwStrength.special) {
      toast({ title: "Passwort zu schwach", description: "Mindestens 8 Zeichen, eine Zahl und ein Sonderzeichen erforderlich.", variant: "destructive" });
      return;
    }
    setIsLoading(true);
    try {
      const { error } = await supabase.auth.updateUser({ password });
      if (error) {
        toast({ title: "Fehler", description: translateAuthError(error.message), variant: "destructive" });
      } else {
        toast({ title: "Passwort gesetzt!", description: "Du wirst jetzt zu deinem Dashboard weitergeleitet." });
        await supabase.auth.signOut();
        navigate('/auth');
      }
    } catch {
      toast({ title: "Fehler", description: "Ein unerwarteter Fehler ist aufgetreten.", variant: "destructive" });
    } finally {
      setIsLoading(false);
    }
  };

  const handleResetPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    try {
      const { error } = await supabase.auth.resetPasswordForEmail(email, {
        redirectTo: `${window.location.origin}/auth?tab=update-password`,
      });
      if (error) {
        toast({ title: "Fehler", description: translateAuthError(error.message), variant: "destructive" });
      } else {
        setResetSent(true);
      }
    } catch {
      toast({ title: "Fehler", description: "Ein unerwarteter Fehler ist aufgetreten.", variant: "destructive" });
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-900 flex flex-col">
      {/* Back link */}
      <div className="px-6 pt-6">
        <Link to="/" className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-300 transition-colors">
          <ArrowLeft className="w-4 h-4" />
          Zurück zur Startseite
        </Link>
      </div>

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
            <p className="text-gray-400 text-sm">
              Melde dich an, um auf dein persönliches Deal-Dashboard zuzugreifen.
            </p>
          </div>

          <div className="bg-white/5 border border-white/10 rounded-2xl p-6 backdrop-blur-sm">
            <Tabs value={tab} onValueChange={(v) => { setTab(v as AuthTab); setResetSent(false); }} className="w-full">
              <TabsList className="grid w-full grid-cols-2 bg-white/5 border border-white/10 mb-6 p-1">
                <TabsTrigger
                  value="login"
                  className="data-[state=active]:bg-slate-700 data-[state=active]:text-white data-[state=active]:shadow-sm text-gray-400 hover:text-gray-200"
                >
                  Anmelden
                </TabsTrigger>
                <TabsTrigger
                  value="register"
                  className="data-[state=active]:bg-slate-700 data-[state=active]:text-white data-[state=active]:shadow-sm text-gray-400 hover:text-gray-200"
                >
                  Registrieren
                </TabsTrigger>
              </TabsList>

              {/* Login tab */}
              <TabsContent value="login" className="space-y-4">
                <form onSubmit={handleSignIn} className="space-y-4">
                  <div className="space-y-1.5">
                    <Label htmlFor="login-email" className="text-gray-300 text-sm">E-Mail</Label>
                    <Input
                      id="login-email"
                      type="email"
                      placeholder="deine@email.ch"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      required
                      className="bg-white/10 border-white/20 text-white placeholder:text-gray-500 focus:border-green-400/50 focus:ring-green-400/20"
                    />
                  </div>
                  <div className="space-y-1.5">
                    <Label htmlFor="login-password" className="text-gray-300 text-sm">Passwort</Label>
                    <Input
                      id="login-password"
                      type="password"
                      placeholder="Dein Passwort"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      required
                      className="bg-white/10 border-white/20 text-white placeholder:text-gray-500 focus:border-green-400/50 focus:ring-green-400/20"
                    />
                  </div>
                  <Button
                    type="submit"
                    className="w-full bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold h-11"
                    disabled={isLoading}
                  >
                    {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Anmelden"}
                  </Button>
                </form>
                <button
                  type="button"
                  onClick={() => setTab("reset")}
                  className="w-full text-center text-sm text-gray-500 hover:text-gray-300 transition-colors mt-2"
                >
                  Passwort vergessen?
                </button>
              </TabsContent>

              {/* Register tab */}
              <TabsContent value="register" className="space-y-4">
                <div className="bg-green-500/10 border border-green-500/20 rounded-lg px-4 py-3 text-sm text-green-300">
                  Bereits SnapFare-Abonnent? Registriere dich mit derselben E-Mail-Adresse.
                </div>

                <form onSubmit={handleSignUp} className="space-y-4">
                  <div className="space-y-1.5">
                    <Label htmlFor="register-name" className="text-gray-300 text-sm">Wie heisst du?</Label>
                    <Input
                      id="register-name"
                      type="text"
                      placeholder="Dein Name"
                      value={name}
                      onChange={(e) => setName(e.target.value)}
                      className="bg-white/10 border-white/20 text-white placeholder:text-gray-500 focus:border-green-400/50 focus:ring-green-400/20"
                    />
                  </div>
                  <div className="space-y-1.5">
                    <Label htmlFor="register-email" className="text-gray-300 text-sm">E-Mail</Label>
                    <Input
                      id="register-email"
                      type="email"
                      placeholder="deine@email.ch"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      required
                      className="bg-white/10 border-white/20 text-white placeholder:text-gray-500 focus:border-green-400/50 focus:ring-green-400/20"
                    />
                  </div>
                  <div className="space-y-1.5">
                    <Label htmlFor="register-password" className="text-gray-300 text-sm">Passwort</Label>
                    <Input
                      id="register-password"
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
                    {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Konto erstellen"}
                  </Button>
                </form>
              </TabsContent>

              {/* Set new password tab (reached via recovery email link) */}
              <TabsContent value="update-password" className="space-y-4">
                <div className="text-center pb-2">
                  <div className="w-12 h-12 bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-3 border border-green-500/30">
                    <span className="text-xl">🔒</span>
                  </div>
                  <p className="text-sm text-gray-400">Wähle ein neues Passwort für dein Konto.</p>
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
                    <Label htmlFor="confirm-new-password" className="text-gray-300 text-sm">Passwort bestätigen</Label>
                    <Input
                      id="confirm-new-password"
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
              </TabsContent>

              {/* Password reset tab */}
              <TabsContent value="reset" className="space-y-4">
                {resetSent ? (
                  <div className="text-center py-6 space-y-3">
                    <div className="w-12 h-12 bg-green-500/20 rounded-full flex items-center justify-center mx-auto border border-green-500/30">
                      <svg className="w-6 h-6 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                      </svg>
                    </div>
                    <p className="text-sm text-gray-400">
                      Wir haben dir einen Link zum Zurücksetzen deines Passworts zugesendet. Bitte überprüfe deine E-Mails.
                    </p>
                    <button
                      onClick={() => { setTab("login"); setResetSent(false); }}
                      className="text-sm text-green-400 hover:text-green-300 transition-colors"
                    >
                      Zurück zur Anmeldung
                    </button>
                  </div>
                ) : (
                  <>
                    <p className="text-sm text-gray-400">
                      Gib deine E-Mail-Adresse ein. Wir senden dir einen Link zum Zurücksetzen deines Passworts.
                    </p>
                    <form onSubmit={handleResetPassword} className="space-y-4">
                      <div className="space-y-1.5">
                        <Label htmlFor="reset-email" className="text-gray-300 text-sm">E-Mail</Label>
                        <Input
                          id="reset-email"
                          type="email"
                          placeholder="deine@email.ch"
                          value={email}
                          onChange={(e) => setEmail(e.target.value)}
                          required
                          className="bg-white/10 border-white/20 text-white placeholder:text-gray-500 focus:border-green-400/50 focus:ring-green-400/20"
                        />
                      </div>
                      <Button
                        type="submit"
                        className="w-full bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold h-11"
                        disabled={isLoading}
                      >
                        {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Link zusenden"}
                      </Button>
                    </form>
                    <button
                      type="button"
                      onClick={() => setTab("login")}
                      className="w-full text-center text-sm text-gray-500 hover:text-gray-300 transition-colors"
                    >
                      Zurück zur Anmeldung
                    </button>
                  </>
                )}
              </TabsContent>
            </Tabs>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Auth;
