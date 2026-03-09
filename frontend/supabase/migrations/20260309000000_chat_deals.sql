-- chat_deals: stores live Duffel flight results per user chat session.
-- Completely separate from the public `deals` table — never shown in the deals section.
-- Each row belongs to one user and expires after 24 hours.

CREATE TABLE IF NOT EXISTS chat_deals (
  id         bigserial PRIMARY KEY,
  user_id    uuid        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  title      text,
  price      numeric     NOT NULL,
  currency   text        NOT NULL DEFAULT 'CHF',
  origin_iata          text,
  destination_iata     text,
  origin               text,
  destination          text,
  airline              text,
  cabin_class          text,
  skyscanner_url       text,
  travel_period_display text,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL DEFAULT (now() + interval '24 hours')
);

-- Index for cleanup job
CREATE INDEX IF NOT EXISTS chat_deals_expires_at_idx ON chat_deals (expires_at);

-- RLS: users can only read their own rows; service role bypasses for INSERT
ALTER TABLE chat_deals ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own chat_deals"
  ON chat_deals FOR SELECT
  USING (auth.uid() = user_id);
