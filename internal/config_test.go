package internal

import "testing"

func cleanConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"NATS_URL", "WORKER_ID", "HTTP_ADDR", "ADMIN_TOKEN", "JOB_LEASE_DURATION", "JOB_HEARTBEAT_INTERVAL", "NATS_ACK_WAIT", "POLL_INTERVAL", "WORKER_SHUTDOWN_TIMEOUT", "DISPATCH_REPAIR_INTERVAL", "DB_QUERY_TIMEOUT", "JOB_DEFAULT_MAX_ATTEMPTS", "WORKER_CONCURRENCY", "DB_MAX_CONNECTIONS", "DEMO_MODE_ENABLED"} {
		t.Setenv(key, "")
	}
	t.Setenv("DATABASE_URL", "postgresql:///job_recovery?host=/tmp")
}

func TestConfigDefaultsAreValidated(t *testing.T) {
	cleanConfigEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkerConcurrency != 4 || c.MaxAttempts != 5 || c.DemoModeEnabled {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestConfigRequiresDatabaseURL(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected missing DATABASE_URL to fail")
	}
}

func TestConfigRejectsHeartbeatTooCloseToLease(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("JOB_LEASE_DURATION", "12s")
	t.Setenv("JOB_HEARTBEAT_INTERVAL", "6s")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid heartbeat/lease relationship to fail")
	}
}

func TestConfigRequiresTokenForPublicBind(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("HTTP_ADDR", ":8080")
	if _, err := Load(); err == nil {
		t.Fatal("expected a public bind without ADMIN_TOKEN to fail")
	}
}

func TestConfigRejectsMalformedDuration(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("DB_QUERY_TIMEOUT", "eventually")
	if _, err := Load(); err == nil {
		t.Fatal("expected malformed duration to fail")
	}
}
