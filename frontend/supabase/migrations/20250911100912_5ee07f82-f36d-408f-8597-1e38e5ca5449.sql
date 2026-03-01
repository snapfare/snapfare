-- Lock down payments table completely (only edge functions with service role can access)
ALTER TABLE public.payments ENABLE ROW LEVEL SECURITY;

-- Deny all client actions on payments table
CREATE POLICY payments_no_select_anon ON public.payments FOR SELECT TO anon USING (false);
CREATE POLICY payments_no_select_auth ON public.payments FOR SELECT TO authenticated USING (false);
CREATE POLICY payments_no_insert_anon ON public.payments FOR INSERT TO anon WITH CHECK (false);
CREATE POLICY payments_no_insert_auth ON public.payments FOR INSERT TO authenticated WITH CHECK (false);
CREATE POLICY payments_no_update_auth ON public.payments FOR UPDATE TO authenticated USING (false) WITH CHECK (false);
CREATE POLICY payments_no_delete_auth ON public.payments FOR DELETE TO authenticated USING (false);

-- Secure the waitlist table - insert only for anonymous users
-- Remove existing policies first
DROP POLICY IF EXISTS "Anyone can join waitlist" ON public.waitlist;
DROP POLICY IF EXISTS "Authenticated users can view waitlist" ON public.waitlist;

-- Add secure policies for waitlist
CREATE POLICY waitlist_insert_anon ON public.waitlist FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY waitlist_no_select_anon ON public.waitlist FOR SELECT TO anon USING (false);
CREATE POLICY waitlist_no_select_auth ON public.waitlist FOR SELECT TO authenticated USING (false);
CREATE POLICY waitlist_no_update_auth ON public.waitlist FOR UPDATE TO authenticated USING (false) WITH CHECK (false);
CREATE POLICY waitlist_no_delete_auth ON public.waitlist FOR DELETE TO authenticated USING (false);

-- Secure the subscribers table - insert only for anonymous users
-- Remove existing policy first
DROP POLICY IF EXISTS "subscribers_anon_insert_email" ON public.subscribers;

-- Add secure policies for subscribers
CREATE POLICY subscribers_insert_anon ON public.subscribers FOR INSERT TO anon WITH CHECK (true);
CREATE POLICY subscribers_no_select_anon ON public.subscribers FOR SELECT TO anon USING (false);
CREATE POLICY subscribers_no_select_auth ON public.subscribers FOR SELECT TO authenticated USING (false);
CREATE POLICY subscribers_no_update_auth ON public.subscribers FOR UPDATE TO authenticated USING (false) WITH CHECK (false);
CREATE POLICY subscribers_no_delete_auth ON public.subscribers FOR DELETE TO authenticated USING (false);