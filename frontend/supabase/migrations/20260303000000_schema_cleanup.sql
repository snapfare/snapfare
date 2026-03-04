-- Migration: Schema cleanup after backend overhaul (2026-03-03)
-- Removes redundant columns, adds missing columns, fixes stale data.

-- ============================================================
-- 1. ADD new columns
-- ============================================================

ALTER TABLE deals ADD COLUMN IF NOT EXISTS stops integer;

-- ============================================================
-- 2. DROP redundant / replaced columns
-- ============================================================

-- cabin_baggage: replaced by structured baggage_* fields; display computed at render time
ALTER TABLE deals DROP COLUMN IF EXISTS cabin_baggage;

-- image_url: redundant — the canonical column is "image"
ALTER TABLE deals DROP COLUMN IF EXISTS image_url;

-- date_range: redundant — derive from date_out / date_in at render time
ALTER TABLE deals DROP COLUMN IF EXISTS date_range;

-- one_way: irrelevant — pipeline only persists return flights
ALTER TABLE deals DROP COLUMN IF EXISTS one_way;

-- flight: redundant — route is in title / origin / destination
ALTER TABLE deals DROP COLUMN IF EXISTS flight;

-- baggage_allowance_display: display string computed at render time from structured fields
ALTER TABLE deals DROP COLUMN IF EXISTS baggage_allowance_display;

-- departure_date / return_date: duplicate of date_out / date_in
ALTER TABLE deals DROP COLUMN IF EXISTS departure_date;
ALTER TABLE deals DROP COLUMN IF EXISTS return_date;

-- ============================================================
-- 3. DATA FIXES
-- ============================================================

-- TravelDealz booking_url should be NULL (article URL lives in link)
UPDATE deals SET booking_url = NULL WHERE source = 'travel-dealz';

-- Clear non-Unsplash images for TravelDealz (pipeline will repopulate on next run)
UPDATE deals SET image = NULL
WHERE source = 'travel-dealz'
  AND image IS NOT NULL
  AND image NOT LIKE '%unsplash%';

-- Set default cabin_class for Duffel deals that are missing it
UPDATE deals SET cabin_class = 'Economy'
WHERE source = 'duffel'
  AND (cabin_class IS NULL OR cabin_class = '');

-- ============================================================
-- 4. INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS deals_stops_idx ON deals(stops);
CREATE INDEX IF NOT EXISTS deals_source_date_out_idx ON deals(source, date_out);
