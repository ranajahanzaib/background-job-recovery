CREATE TABLE IF NOT EXISTS demo_runs (
 id uuid PRIMARY KEY,
 expected_jobs integer NOT NULL CHECK (expected_jobs > 0),
 created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_repair_at timestamptz;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS demo_run_id uuid REFERENCES demo_runs(id);
CREATE TABLE IF NOT EXISTS job_results (
 job_id uuid PRIMARY KEY REFERENCES jobs(id),
 result jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
