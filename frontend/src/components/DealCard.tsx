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
  Economy: "bg-blue-100 text-blue-700",
  Business: "bg-amber-100 text-amber-700",
  First: "bg-purple-100 text-purple-700",
  "Premium Economy": "bg-teal-100 text-teal-700",
};

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
  const cabinBadgeClass =
    CABIN_COLORS[deal.cabin_class ?? "Economy"] ?? "bg-gray-100 text-gray-700";

  const ctaUrl = deal.skyscanner_url ?? deal.link ?? "#";

  return (
    <div className="bg-white rounded-2xl border border-gray-100 shadow-sm hover:shadow-md transition-shadow overflow-hidden flex flex-col">
      {/* Image */}
      {deal.image && (
        <div className="relative h-36 overflow-hidden">
          <img
            src={deal.image}
            alt={`${deal.origin} → ${deal.destination}`}
            className="w-full h-full object-cover"
            loading="lazy"
          />
          {/* Tier badge */}
          {deal.tier === "premium" && (
            <div className="absolute top-2 right-2 bg-amber-500 text-white text-xs font-bold px-2 py-0.5 rounded-full">
              Premium
            </div>
          )}
        </div>
      )}

      <div className="p-4 flex flex-col flex-1">
        {/* Route */}
        <div className="flex items-center gap-1 mb-1">
          <span className="font-bold text-gray-900 text-sm">
            {deal.origin_iata ?? deal.origin}
          </span>
          <Plane className="w-3 h-3 text-gray-400 mx-1" />
          <span className="font-bold text-gray-900 text-sm">
            {deal.destination_iata ?? deal.destination}
          </span>
        </div>

        {/* City names */}
        {!compact && (
          <p className="text-xs text-gray-500 mb-2">
            {deal.origin} → {deal.destination}
          </p>
        )}

        {/* Airline + cabin */}
        <div className="flex items-center gap-2 mb-3">
          <span className="text-xs text-gray-600 truncate">{deal.airline}</span>
          <Badge className={`text-[10px] px-1.5 py-0 ${cabinBadgeClass} border-0`}>
            {deal.cabin_class}
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
              <span>
                {deal.stops === 0 ? "Nonstop" : `${deal.stops} Stopp`}
              </span>
            )}
            <span className="flex items-center gap-1">
              <Luggage className="w-3 h-3" />
              {formatBaggage(deal)}
            </span>
          </div>
        )}

        {/* Travel period */}
        {deal.travel_period_display && !compact && (
          <p className="text-[11px] text-gray-400 mb-3">{deal.travel_period_display}</p>
        )}

        {/* Miles */}
        {deal.miles && !compact && (
          <p className="text-[11px] text-indigo-600 mb-3">✈ {deal.miles}</p>
        )}

        {/* Price + CTA */}
        <div className="mt-auto flex items-center justify-between pt-2 border-t border-gray-50">
          <div>
            <span className="text-xl font-black text-blue-600">
              {deal.price ? `CHF ${Math.round(deal.price)}` : "–"}
            </span>
            <span className="text-xs text-gray-400 ml-1">/ Person</span>
          </div>

          <a href={ctaUrl} target="_blank" rel="noopener noreferrer">
            <Button
              size="sm"
              className="bg-blue-600 hover:bg-blue-700 text-white text-xs px-3 h-8 gap-1"
            >
              Ansehen
              <ExternalLink className="w-3 h-3" />
            </Button>
          </a>
        </div>
      </div>
    </div>
  );
};

export default DealCard;
