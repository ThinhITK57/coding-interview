-- postgresql.public.events definition
DROP TABLE IF EXISTS postgresql.public.events;

CREATE TABLE postgresql.public.events (
   id bigint NOT NULL,
   name varchar,
   created_at timestamp(6)
);

INSERT INTO postgresql.public.events (id, name, created_at) VALUES
(CAST(1 AS bigint), 'User Signup', CAST('2026-03-01 10:15:30' AS timestamp)),
(CAST(2 AS bigint), 'User Login', CAST('2026-03-01 11:00:00' AS timestamp));