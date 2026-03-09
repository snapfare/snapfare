-- Enrich chat_deals with additional Duffel flight data fields.
-- These allow the agent to present baggage, stops, duration, and aircraft info
-- for live-searched flights (same fields as the public deals table).

ALTER TABLE chat_deals
  ADD COLUMN IF NOT EXISTS stops                    integer,
  ADD COLUMN IF NOT EXISTS flight_duration_display  text,
  ADD COLUMN IF NOT EXISTS baggage_included         boolean,
  ADD COLUMN IF NOT EXISTS baggage_allowance_kg     numeric,
  ADD COLUMN IF NOT EXISTS miles                    text,
  ADD COLUMN IF NOT EXISTS scoring                  text,
  ADD COLUMN IF NOT EXISTS aircraft                 text,
  ADD COLUMN IF NOT EXISTS baggage_pieces_included  integer;
