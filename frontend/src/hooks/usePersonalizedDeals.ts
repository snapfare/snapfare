import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { matchesRegion } from "@/lib/regionMapping";
import type { Tables } from "@/integrations/supabase/types";

export type Deal = Tables<"deals">;

interface UserPrefs {
  preferred_origins: string[];
  preferred_regions: string[];
  max_price_chf: number | null;
  cabin_classes: string[];
  min_trip_days: number;
  max_trip_days: number | null;
  preferred_seasons: string[];
  flight_types: string[];
}

async function fetchPersonalizedDeals(
  prefs: UserPrefs | null,
  userId: string
): Promise<Deal[]> {
  let query = supabase
    .from("deals")
    .select("*")
    .order("scoring", { ascending: false })
    .limit(50);

  if (prefs && (prefs.preferred_origins?.length ?? 0) > 0) {
    query = query.in("origin_iata", prefs.preferred_origins);
  }

  if (prefs?.max_price_chf) {
    query = query.lte("price", prefs.max_price_chf);
  }

  if (prefs && (prefs.cabin_classes?.length ?? 0) > 0) {
    query = query.in("cabin_class", prefs.cabin_classes);
  }

  const { data, error } = await query;
  if (error) throw error;

  let deals = (data as Deal[]) ?? [];

  // Client-side region filter (destination_iata → region mapping)
  if (prefs && (prefs.preferred_regions?.length ?? 0) > 0) {
    deals = deals.filter((d) =>
      matchesRegion(d.destination_iata, prefs.preferred_regions)
    );
  }

  // Quality filter: only show deals with score > 30, always include travel-dealz (curated)
  deals = deals.filter(
    (d) => (parseFloat(d.scoring ?? "0") > 30) || d.source === "travel-dealz"
  );

  return deals.slice(0, 20);
}

async function fetchUserPrefs(userId: string): Promise<UserPrefs | null> {
  const { data, error } = await supabase
    .from("user_preferences")
    .select("*")
    .eq("user_id", userId)
    .single();

  if (error || !data) return null;
  return data as UserPrefs;
}

export function usePersonalizedDeals(userId: string | undefined) {
  const { data: prefs, isLoading: prefsLoading } = useQuery({
    queryKey: ["user-preferences", userId],
    queryFn: () => fetchUserPrefs(userId!),
    enabled: !!userId,
    staleTime: 5 * 60 * 1000,
  });

  const { data: deals, isLoading: dealsLoading, refetch } = useQuery({
    queryKey: ["personalized-deals", userId, prefs],
    queryFn: () => fetchPersonalizedDeals(prefs ?? null, userId!),
    enabled: !!userId && !prefsLoading,
    staleTime: 5 * 60 * 1000,
  });

  return {
    deals: deals ?? [],
    prefs: prefs ?? null,
    isLoading: prefsLoading || dealsLoading,
    refetchDeals: refetch,
  };
}
