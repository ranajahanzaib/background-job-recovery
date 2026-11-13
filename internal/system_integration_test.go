package internal

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type integrationMessage struct{ data []byte }

func (m integrationMessage) Data() []byte                   { return m.data }
func (integrationMessage) Ack() error                       { return nil }
func (integrationMessage) NakWithDelay(time.Duration) error { return nil }
func (integrationMessage) InProgress() error                { return nil }
func (integrationMessage) Term() error                      { return nil }

func integrationSystem(t *testing.T) *System {
	t.Helper()
	if os.Getenv("REQUIRE_INTEGRATION") != "1" {
		t.Skip("set REQUIRE_INTEGRATION=1 with PostgreSQL and NATS to run integration tests")
	}
	dbURL, natsURL := os.Getenv("DATABASE_URL"), os.Getenv("NATS_URL")
	if dbURL == "" || natsURL == "" {
		t.Fatal("REQUIRE_INTEGRATION requires DATABASE_URL and NATS_URL")
	}
	c := Config{DatabaseURL: dbURL, NATSURL: natsURL, WorkerID: "integration-" + uuid.NewString(), HTTPAddr: "127.0.0.1:0", Lease: 15 * time.Second, Heartbeat: time.Second, AckWait: 10 * time.Second, Poll: 50 * time.Millisecond, ShutdownTimeout: 2 * time.Second, DispatchRepairInterval: time.Minute, QueryTimeout: 5 * time.Second, MaxAttempts: 5, WorkerConcurrency: 2, DBMaxConnections: 8, DemoModeEnabled: true}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err = s.ConnectNATS(); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err = s.EnsureStream(); err != nil {
		s.Close()
		t.Fatal(err)
	}
	return s
}

func cleanupJob(t *testing.T, s *System, jobID, orderID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, q := range []string{"DELETE FROM order_fulfillments WHERE job_id=$1", "DELETE FROM job_results WHERE job_id=$1", "DELETE FROM job_events WHERE job_id=$1", "DELETE FROM job_attempts WHERE job_id=$1", "DELETE FROM job_outbox WHERE job_id=$1", "DELETE FROM jobs WHERE id=$1", "DELETE FROM orders WHERE id=$1"} {
		if _, err := s.DB.ExecContext(ctx, q, jobID); err != nil {
			t.Errorf("cleanup %q: %v", q, err)
		}
	}
}

func TestIntegrationIdempotentEnqueuePublishAndConcurrentDuplicateDispatch(t *testing.T) {
	s := integrationSystem(t)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	orderID, key := uuid.NewString(), "integration-"+uuid.NewString()
	if _, err := s.DB.ExecContext(ctx, "INSERT INTO orders(id,customer_id) VALUES($1,$2)", orderID, "integration-customer"); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]string{"order_id": orderID, "customer_id": "integration-customer"})
	job, err := s.Enqueue(ctx, "fulfill_order", key, payload, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupJob(t, s, job.ID, orderID) })
	repeated, err := s.Enqueue(ctx, "fulfill_order", key, payload, 4)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.ID != job.ID {
		t.Fatalf("idempotent submission returned %s, want %s", repeated.ID, job.ID)
	}
	conflicting, _ := json.Marshal(map[string]string{"order_id": orderID, "customer_id": "different"})
	if _, err = s.Enqueue(ctx, "fulfill_order", key, conflicting, 4); err == nil {
		t.Fatal("conflicting idempotency reuse succeeded")
	}
	var outboxStatus string
	for range 100 {
		if err = s.Publish(ctx); err != nil {
			t.Fatal(err)
		}
		if err = s.DB.QueryRowContext(ctx, "SELECT status FROM job_outbox WHERE job_id=$1 AND job_generation=1", job.ID).Scan(&outboxStatus); err != nil {
			t.Fatal(err)
		}
		if outboxStatus == "PUBLISHED" {
			break
		}
	}
	if outboxStatus != "PUBLISHED" {
		t.Fatalf("outbox status=%s, want PUBLISHED", outboxStatus)
	}
	var dispatch []byte
	if err = s.DB.QueryRowContext(ctx, "SELECT payload FROM job_outbox WHERE job_id=$1 AND job_generation=1", job.ID).Scan(&dispatch); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; errs <- s.process(ctx, integrationMessage{data: dispatch}) }()
	}
	close(start)
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Errorf("duplicate dispatch processing failed: %v", e)
		}
	}
	var status string
	var attempts, effects int
	if err = s.DB.QueryRowContext(ctx, "SELECT status,attempts_started FROM jobs WHERE id=$1", job.ID).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if err = s.DB.QueryRowContext(ctx, "SELECT count(*) FROM order_fulfillments WHERE order_id=$1", orderID).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" || attempts != 1 || effects != 1 {
		t.Fatalf("status=%s attempts=%d effects=%d; want SUCCEEDED, 1, 1", status, attempts, effects)
	}
}

func TestIntegrationStaleFenceCannotCommitFulfillment(t *testing.T) {
	s := integrationSystem(t)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	orderID, key := uuid.NewString(), "stale-"+uuid.NewString()
	if _, err := s.DB.ExecContext(ctx, "INSERT INTO orders(id,customer_id) VALUES($1,$2)", orderID, "customer"); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]string{"order_id": orderID, "customer_id": "customer"})
	job, err := s.Enqueue(ctx, "fulfill_order", key, payload, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupJob(t, s, job.ID, orderID) })
	attemptID := uuid.NewString()
	if _, err = s.DB.ExecContext(ctx, "UPDATE jobs SET status='RUNNING',lease_owner=$2,fence_epoch=5,attempts_started=1,attempts_in_epoch=1,lease_expires_at=now()+interval '1 minute' WHERE id=$1", job.ID, s.C.WorkerID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.ExecContext(ctx, "INSERT INTO job_attempts(id,job_id,generation,fence_epoch,worker_id,attempt_number,outcome) VALUES($1,$2,1,5,$3,1,'RUNNING')", attemptID, job.ID, s.C.WorkerID); err != nil {
		t.Fatal(err)
	}
	err = s.complete(ctx, Job{ID: job.ID, Type: "fulfill_order", Generation: 1}, attemptID, 4, orderID)
	if err == nil {
		t.Fatal("stale fencing token completed the job")
	}
	var effects int
	if err = s.DB.QueryRowContext(ctx, "SELECT count(*) FROM order_fulfillments WHERE order_id=$1", orderID).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if effects != 0 {
		t.Fatalf("stale worker committed %d fulfillment effects", effects)
	}
	var status string
	if err = s.DB.QueryRowContext(ctx, "SELECT status FROM jobs WHERE id=$1", job.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "RUNNING" {
		t.Fatalf("stale completion changed job status to %s", status)
	}
}
