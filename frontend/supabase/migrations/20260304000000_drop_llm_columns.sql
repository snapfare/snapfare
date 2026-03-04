-- Drop LLM enrichment tracking columns (all enrichment is now deterministic)
ALTER TABLE public.deals DROP COLUMN IF EXISTS llm_enriched;
ALTER TABLE public.deals DROP COLUMN IF EXISTS llm_enriched_fields;
ALTER TABLE public.deals DROP COLUMN IF EXISTS llm_enrichment_version;
