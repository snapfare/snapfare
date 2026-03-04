import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import type { User } from "@supabase/supabase-js";

interface PremiumGateResult {
  isLoading: boolean;
  isAuthenticated: boolean;
  isPremium: boolean;
  tier: string;
  user: User | null;
}

async function checkPremium(token: string): Promise<{ isPremium: boolean; tier: string }> {
  const response = await fetch(
    `${import.meta.env.VITE_SUPABASE_URL}/functions/v1/check-premium`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    return { isPremium: false, tier: "free" };
  }

  const data = await response.json();
  return { isPremium: data.isPremium ?? false, tier: data.tier ?? "free" };
}

export function usePremiumGate(): PremiumGateResult {
  const [user, setUser] = useState<User | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [authLoading, setAuthLoading] = useState(true);

  useEffect(() => {
    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      (_event, session) => {
        setUser(session?.user ?? null);
        setToken(session?.access_token ?? null);
        setAuthLoading(false);
      }
    );

    supabase.auth.getSession().then(({ data: { session } }) => {
      setUser(session?.user ?? null);
      setToken(session?.access_token ?? null);
      setAuthLoading(false);
    });

    return () => subscription.unsubscribe();
  }, []);

  const { data: premiumData, isLoading: premiumLoading } = useQuery({
    queryKey: ["premium-check", user?.id],
    queryFn: () => checkPremium(token!),
    enabled: !!token && !!user,
    staleTime: 2 * 60 * 1000, // 2 minutes
    retry: 1,
  });

  const isLoading = authLoading || (!!user && premiumLoading);

  return {
    isLoading,
    isAuthenticated: !!user,
    isPremium: premiumData?.isPremium ?? false,
    tier: premiumData?.tier ?? "free",
    user,
  };
}
