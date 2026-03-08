-- ============================================================
-- SnapFare Dashboard Schema
-- Run in: Supabase SQL Editor (project nvcxaddealeuvttvzrsa)
-- ============================================================

-- 1. Add full_name and onboarding_completed to user_preferences
ALTER TABLE public.user_preferences
  ADD COLUMN IF NOT EXISTS full_name text,
  ADD COLUMN IF NOT EXISTS onboarding_completed boolean DEFAULT false;

-- 2. agent_conversations table
CREATE TABLE IF NOT EXISTS public.agent_conversations (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  user_id uuid REFERENCES auth.users(id) ON DELETE CASCADE,
  session_id uuid NOT NULL,
  role text NOT NULL CHECK (role IN ('user', 'assistant')),
  content text NOT NULL,
  message_index integer NOT NULL,
  created_at timestamptz DEFAULT now()
);

ALTER TABLE public.agent_conversations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can manage own conversations"
  ON public.agent_conversations FOR ALL
  USING (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

CREATE INDEX IF NOT EXISTS idx_agent_conversations_session
  ON public.agent_conversations(user_id, session_id, message_index);
