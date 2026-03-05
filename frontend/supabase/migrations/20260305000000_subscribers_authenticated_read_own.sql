-- Allow authenticated users to read their own subscriber row so the frontend
-- can check premium status directly without calling an edge function.
CREATE POLICY "subscribers_authenticated_read_own"
  ON subscribers
  FOR SELECT
  TO authenticated
  USING (lower(email) = lower((auth.jwt() ->> 'email')));
