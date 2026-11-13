CREATE TABLE IF NOT EXISTS jobs (
 id uuid PRIMARY KEY, job_type text NOT NULL, payload jsonb NOT NULL,
 idempotency_key text NOT NULL UNIQUE, request_hash text NOT NULL,
 status text NOT NULL CHECK (status IN ('QUEUED','RUNNING','RETRY_SCHEDULED','SUCCEEDED','DEAD')),
 generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0), fence_epoch bigint NOT NULL DEFAULT 0 CHECK (fence_epoch >= 0),
 attempts_started integer NOT NULL DEFAULT 0 CHECK (attempts_started >= 0), attempts_in_epoch integer NOT NULL DEFAULT 0 CHECK (attempts_in_epoch >= 0), max_attempts integer NOT NULL CHECK (max_attempts > 0),
 available_at timestamptz NOT NULL DEFAULT now(), lease_owner text, lease_expires_at timestamptz,
 last_dispatched_at timestamptz, last_error_code text, last_error_message text,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz
);
CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs(status,available_at,created_at) WHERE status='QUEUED';
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs(lease_expires_at) WHERE status='RUNNING';
CREATE INDEX IF NOT EXISTS jobs_retry_idx ON jobs(available_at) WHERE status='RETRY_SCHEDULED';
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status,created_at DESC);
CREATE TABLE IF NOT EXISTS job_attempts (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id), generation bigint NOT NULL, fence_epoch bigint NOT NULL,
 worker_id text NOT NULL, attempt_number integer NOT NULL, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 outcome text NOT NULL CHECK(outcome IN ('RUNNING','SUCCEEDED','FAILED_RETRYABLE','FAILED_PERMANENT','ABANDONED','FENCED_OUT')),
 error_code text,error_message text
);
CREATE INDEX IF NOT EXISTS attempts_job_idx ON job_attempts(job_id,started_at);
CREATE TABLE IF NOT EXISTS job_outbox (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id), job_generation bigint NOT NULL,
 event_type text NOT NULL, payload jsonb NOT NULL, status text NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','PUBLISHING','PUBLISHED')),
 publish_attempts integer NOT NULL DEFAULT 0, locked_by text, locked_until timestamptz, published_at timestamptz,
 last_error text, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON job_outbox(created_at) WHERE status <> 'PUBLISHED';
CREATE TABLE IF NOT EXISTS job_events (
 id bigserial PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id), event_type text NOT NULL, worker_id text,
 generation bigint, fence_epoch bigint, metadata jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_job_idx ON job_events(job_id,id);
CREATE TABLE IF NOT EXISTS orders (id uuid PRIMARY KEY, customer_id text NOT NULL, status text NOT NULL DEFAULT 'READY', created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS order_fulfillments (id uuid PRIMARY KEY, order_id uuid NOT NULL UNIQUE REFERENCES orders(id), job_id uuid NOT NULL REFERENCES jobs(id), created_at timestamptz NOT NULL DEFAULT now());

