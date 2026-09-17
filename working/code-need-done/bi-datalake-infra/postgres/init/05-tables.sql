\c analytics;

CREATE TABLE events (
  id BIGSERIAL PRIMARY KEY,
  name TEXT,
  created_at TIMESTAMP DEFAULT now()
);
