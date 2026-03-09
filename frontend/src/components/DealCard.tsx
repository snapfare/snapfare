import React from "react";
import { ExternalLink, Clock, Luggage, Plane } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { Deal } from "@/hooks/usePersonalizedDeals";

interface DealCardProps {
  deal: Deal;
  compact?: boolean;
}

const CABIN_COLORS: Record<string, string> = {
  Economy: "bg-green-500/20 text-green-300 border-green-500/30",
  Business: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  First: "bg-purple-500/20 text-purple-300 border-purple-500/30",
  "Premium Economy": "bg-teal-500/20 text-teal-300 border-teal-500/30",
};

function normalizeCabin(cabin: string | null | undefined): string {
  if (!cabin) return "Economy";
  const map: Record<string, string> = {
    economy: "Economy",
    business: "Business",
    first: "First",
    premium_economy: "Premium Economy",
    premiumeconomy: "Premium Economy",
    "premium economy": "Premium Economy",
  };
  return map[cabin.toLowerCase().replace(/\s+/g, "_")] ?? cabin;
}

function formatDuration(minutes: number | null): string {
  if (!minutes) return "";
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

function formatBaggage(deal: Deal): string {
  if (!deal.baggage_included) return "Kein Gepäck";
  const parts: string[] = [];
  if (deal.baggage_allowance_kg) parts.push(`${deal.baggage_allowance_kg} kg`);
  if (deal.baggage_pieces_included && deal.baggage_pieces_included > 1)
    parts.push(`${deal.baggage_pieces_included} Koffer`);
  return parts.join(" · ") || "Gepäck inklusive";
}

const DealCard: React.FC<DealCardProps> = ({ deal, compact = false }) => {
  const cabinLabel = normalizeCabin(deal.cabin_class);
  const cabinBadgeClass = CABIN_COLORS[cabinLabel] ?? "bg-white/10 text-gray-300 border-white/20";

  const ctaUrl = deal.skyscanner_url ?? deal.link ?? "#";

  return (
    <div className="bg-white/5 border border-white/10 rounded-xl overflow-hidden flex flex-col hover:border-white/20 transition-all duration-200 hover:bg-white/8">
      {/* Image */}
      {deal.image && !compact && (
        <div className="relative h-36 overflow-hidden">
          <img
            src={deal.image}
            alt={`${deal.origin} → ${deal.destination}`}
            className="w-full h-full object-cover"
            loading="lazy"
          />
          {/* Dark gradient overlay */}
          <div className="absolute inset-0 bg-gradient-to-t from-slate-900/80 via-transparent to-transparent" />
          {/* Tier badge */}
          {deal.tier === "premium" && (
            <div className="absolute top-2 right-2 bg-amber-500/90 text-white text-[10px] font-bold px-2 py-0.5 rounded-full backdrop-blur-sm">
              Premium
            </div>
          )}
          {/* Route overlay on image */}
          <div className="absolute bottom-2 left-3 flex items-center gap-1.5">
            <span className="font-bold text-white text-sm drop-shadow">
              {deal.origin_iata ?? deal.origin}
            </span>
            <Plane className="w-3 h-3 text-white/70" />
            <span className="font-bold text-white text-sm drop-shadow">
              {deal.destination_iata ?? deal.destination}
            </span>
          </div>
        </div>
      )}

      <div className="p-4 flex flex-col flex-1">
        {/* Route (when no image, or compact) */}
        {(!deal.image || compact) && (
          <div className="flex items-center gap-1.5 mb-2">
            <span className="font-bold text-white text-sm">
              {deal.origin_iata ?? deal.origin}
            </span>
            <Plane className="w-3 h-3 text-gray-400 mx-0.5" />
            <span className="font-bold text-white text-sm">
              {deal.destination_iata ?? deal.destination}
            </span>
          </div>
        )}

        {/* City names */}
        {!compact && (
          <p className="text-xs text-gray-500 mb-2">
            {deal.origin} → {deal.destination}
          </p>
        )}

        {/* Airline + cabin */}
        <div className="flex items-center gap-2 mb-3">
          <span className="text-xs text-gray-400 truncate">{deal.airline}</span>
          <Badge className={`text-[10px] px-1.5 py-0 border ${cabinBadgeClass} shrink-0`}>
            {cabinLabel}
          </Badge>
        </div>

        {/* Details row */}
        {!compact && (
          <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-500 mb-3">
            {deal.flight_duration_minutes && (
              <span className="flex items-center gap-1">
                <Clock className="w-3 h-3" />
                {deal.flight_duration_display ?? formatDuration(deal.flight_duration_minutes)}
              </span>
            )}
            {deal.stops !== null && (
              <span className="text-gray-500">
                {deal.stops === 0 ? "Nonstop" : `${deal.stops} Stopp`}
              </span>
            )}
            {deal.baggage_included !== null && (
              <span className="flex items-center gap-1">
                <Luggage className="w-3 h-3" />
                {formatBaggage(deal)}
              </span>
            )}
          </div>
        )}

        {/* Travel period */}
        {deal.travel_period_display && !compact && (
          <p className="text-[11px] text-gray-500 mb-3">{deal.travel_period_display}</p>
        )}

        {/* Miles */}
        {deal.miles && !compact && (
          <p className="text-[11px] text-blue-400 mb-3">✈ {deal.miles}</p>
        )}

        {/* Price + CTA */}
        <div className="mt-auto flex items-center justify-between pt-3 border-t border-white/5">
          <div>
            <span className="text-xl font-black text-white">
              {deal.price ? `CHF ${Math.round(deal.price)}` : "–"}
            </span>
            <span className="text-xs text-gray-500 ml-1">/ Person</span>
          </div>

          <a href={ctaUrl} target="_blank" rel="noopener noreferrer">
            <Button
              size="sm"
              className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white text-xs px-3 h-8 gap-1 border-0"
            >
              Buchen
              <ExternalLink className="w-3 h-3" />
            </Button>
          </a>
        </div>
      </div>
    </div>
  );
};

export default DealCard;
