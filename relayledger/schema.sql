CREATE TABLE IF NOT EXISTS deliveries (
 id uuid PRIMARY KEY,
 idempotency_key text NOT NULL UNIQUE,
 payload jsonb NOT NULL,
 state text NOT NULL DEFAULT 'ready' CHECK (state IN ('ready','leased','delivered','dead')),
 available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz,
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 20),
 fence bigint NOT NULL DEFAULT 0,
 last_outcome text,
 CHECK (attempts <= max_attempts),
 CHECK ((state IN ('delivered','dead')) = (finished_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS deliveries_active_due ON deliveries (available_at,id)
 WHERE state IN ('ready','leased');
CREATE TABLE IF NOT EXISTS delivery_attempts (
 delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
 fence bigint NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz,
 outcome text,
 PRIMARY KEY (delivery_id,fence),
 CHECK ((finished_at IS NULL) = (outcome IS NULL))
);
