CREATE INDEX IF NOT EXISTS jobs_dispatch_repair_idx ON jobs(updated_at) WHERE status='QUEUED';
CREATE INDEX IF NOT EXISTS attempts_running_idx ON job_attempts(job_id,generation) WHERE outcome='RUNNING';
CREATE INDEX IF NOT EXISTS job_events_type_idx ON job_events(event_type,created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS fulfillments_job_idx ON order_fulfillments(job_id);
ALTER TABLE jobs ADD CONSTRAINT jobs_lease_state_chk CHECK (
 (status='RUNNING' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) OR
 (status<>'RUNNING' AND lease_owner IS NULL AND lease_expires_at IS NULL)
);
