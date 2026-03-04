-- Standardize the deals table for the Duffel migration and new pipeline fields.
-- Run this in Supabase SQL Editor for project wwoowwnjrepokmjgxhlw.

-- ─── Add new columns (idempotent: uses IF NOT EXISTS) ────────────────────────

alter table public.deals add column if not exists image_url text null;
alter table public.deals add column if not exists skyscanner_url text null;
alter table public.deals add column if not exists travel_period_display text null;
alter table public.deals add column if not exists tier text null default 'free';
alter table public.deals add column if not exists departure_date text null;
alter table public.deals add column if not exists return_date text null;

-- ─── Populate departure_date / return_date from date_out / date_in ───────────
update public.deals
set departure_date = date_out::text
where departure_date is null and date_out is not null;

update public.deals
set return_date = date_in::text
where return_date is null and date_in is not null;

-- ─── Standardize cabin_class values ─────────────────────────────────────────
-- Collapse single-letter booking class codes into canonical display strings.
update public.deals set cabin_class = 'Economy'
where cabin_class in ('Y', 'M', 'H', 'K', 'L', 'Q', 'T', 'V', 'X', 'B', 'E', 'N', 'O', 'S', 'economy');

update public.deals set cabin_class = 'Premium Economy'
where cabin_class in ('W', 'P', 'premium economy', 'premium_economy', 'PREMIUM ECONOMY');

update public.deals set cabin_class = 'Business'
where cabin_class in ('C', 'J', 'D', 'Z', 'R', 'business', 'BUSINESS');

update public.deals set cabin_class = 'First'
where cabin_class in ('F', 'first', 'FIRST');

-- Default unclassified rows to Economy
update public.deals set cabin_class = 'Economy'
where cabin_class is null or cabin_class = '';

-- ─── Standardize source values ───────────────────────────────────────────────
-- Rename legacy 'amadeus' source rows to 'duffel'.
update public.deals set source = 'duffel'
where source = 'amadeus';

-- ─── Assign tier based on cabin_class ────────────────────────────────────────
update public.deals set tier = 'premium'
where cabin_class in ('Business', 'First', 'Premium Economy') and tier != 'premium';

update public.deals set tier = 'free'
where cabin_class = 'Economy' and (tier is null or tier = '');

-- ─── Populate image_url from legacy image column ─────────────────────────────
update public.deals set image_url = image
where image_url is null and image is not null and image != '';

-- ─── Index for common query patterns ─────────────────────────────────────────
create index if not exists deals_source_idx on public.deals (source);
create index if not exists deals_tier_idx on public.deals (tier);
create index if not exists deals_origin_iata_idx on public.deals (origin_iata);
create index if not exists deals_destination_iata_idx on public.deals (destination_iata);
create index if not exists deals_cabin_class_idx on public.deals (cabin_class);
