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

async function fetchSubscriberTier(email: string): Promise<{ isPremium: boolean; tier: string }> {
  const { data, error } = await supabase
    .from("subscribers")
    .select("tier, status")
    .eq("email", email.toLowerCase())
    .single();

  if (error || !data) {
    return { isPremium: false, tier: "free" };
  }

  if (data.status === "unsubscribed") {
    return { isPremium: false, tier: "free" };
  }

  return {
    isPremium: data.tier === "premium",
    tier: data.tier ?? "free",
  };
}

export function usePremiumGate(): PremiumGateResult {
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);

  useEffect(() => {
    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      (_event, session) => {
        setUser(session?.user ?? null);
        setAuthLoading(false);
      }
    );

    supabase.auth.getSession().then(({ data: { session } }) => {
      setUser(session?.user ?? null);
      setAuthLoading(false);
    });

    return () => subscription.unsubscribe();
  }, []);

  const { data: premiumData, isLoading: premiumLoading } = useQuery({
    queryKey: ["subscriber-tier", user?.email],
    queryFn: () => fetchSubscriberTier(user!.email!),
    enabled: !!user?.email,
    staleTime: 2 * 60 * 1000,
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
