package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ranajahanzaib/background-job-recovery/migrations"
)

type System struct {
	DB  *sql.DB
	JS  jetstream.JetStream
	C   Config
	Log *slog.Logger
	NC  *nats.Conn
}

func Open(ctx context.Context, c Config) (*System, error) {
	db, e := sql.Open("pgx", c.DatabaseURL)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(c.DBMaxConnections)
	db.SetMaxIdleConns(min(5, c.DBMaxConnections))
	db.SetConnMaxLifetime(time.Hour)
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, e
	}
	return &System{DB: db, C: c, Log: slog.New(slog.NewJSONHandler(os.Stdout, nil))}, nil
}
func (s *System) ConnectNATS() error {
	if s.JS != nil {
		return nil
	}
	nc, e := nats.Connect(s.C.NATSURL, nats.Name("background-job-recovery"))
	if e != nil {
		return e
	}
	js, e := jetstream.New(nc)
	if e != nil {
		nc.Close()
		return e
	}
	s.NC = nc
	s.JS = js
	return nil
}
func (s *System) Close() {
	if s.NC != nil {
		s.NC.Close()
	}
	if s.DB != nil {
		_ = s.DB.Close()
	}
}
func (s *System) EnsureStream() error {
	if s.JS == nil {
		return fmt.Errorf("NATS is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, e := s.JS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "JOBS", Subjects: []string{"jobs.ready"}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy, MaxAge: 7 * 24 * time.Hour, MaxBytes: 1 << 30}); e != nil {
		return e
	}
	return s.ensureConsumer()
}
func (s *System) ensureConsumer() error {
	if s.JS == nil {
		return fmt.Errorf("NATS is not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.JS.CreateOrUpdateConsumer(ctx, "JOBS", jetstream.ConsumerConfig{Durable: "job-workers", AckPolicy: jetstream.AckExplicitPolicy, AckWait: s.C.AckWait, MaxAckPending: 128, MaxDeliver: -1})
	return err
}
func (s *System) Migrate(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.C.QueryTimeout)
	defer cancel()
	if _, e := s.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations(version text PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now())`); e != nil {
		return e
	}
	entries, e := migrations.FS.ReadDir(".")
	if e != nil {
		return e
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		var exists bool
		if e = s.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, entry.Name()).Scan(&exists); e != nil {
			return e
		}
		if exists {
			continue
		}
		body, e := migrations.FS.ReadFile(entry.Name())
		if e != nil {
			return e
		}
		tx, e := s.DB.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		for _, statement := range strings.Split(string(body), ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, e = tx.ExecContext(ctx, statement); e != nil {
				tx.Rollback()
				return fmt.Errorf("migration %s: %w", entry.Name(), e)
			}
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, entry.Name()); e != nil {
			tx.Rollback()
			return e
		}
		if e = tx.Commit(); e != nil {
			return e
		}
	}
	return nil
}

type Dispatch struct {
	EventID    string `json:"event_id"`
	JobID      string `json:"job_id"`
	Generation int64  `json:"job_generation"`
	Version    int    `json:"message_version"`
}
type Job struct {
	ID            string          `json:"id"`
	Type          string          `json:"job_type"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"`
	Generation    int64           `json:"generation"`
	Attempts      int             `json:"attempts_started"`
	EpochAttempts int             `json:"attempts_in_epoch"`
	MaxAttempts   int             `json:"max_attempts"`
	Error         string          `json:"last_error_message,omitempty"`
}

var ErrPermanentFailure = errors.New("permanent job failure")

func (s *System) Enqueue(ctx context.Context, typ, key string, payload json.RawMessage, max int) (Job, error) {
	ctx, cancel := context.WithTimeout(ctx, s.C.QueryTimeout)
	defer cancel()
	if typ != "fulfill_order" && typ != "controlled_failure" && typ != "generate_report" {
		return Job{}, fmt.Errorf("unsupported job type")
	}
	if typ == "controlled_failure" && !s.C.DemoModeEnabled {
		return Job{}, fmt.Errorf("controlled_failure is disabled outside demo mode")
	}
	if key == "" || len(key) > 200 || len(payload) > 65536 || max < 1 || max > 100 {
		return Job{}, fmt.Errorf("invalid key, payload, or attempt limit")
	}
	canonical, e := canonicalJSON(payload)
	if e != nil {
		return Job{}, e
	}
	if typ == "fulfill_order" {
		var request struct {
			OrderID    string `json:"order_id"`
			CustomerID string `json:"customer_id"`
		}
		if err := json.Unmarshal(canonical, &request); err != nil || request.CustomerID == "" {
			return Job{}, fmt.Errorf("fulfill_order requires order_id and customer_id")
		}
		if _, err := uuid.Parse(request.OrderID); err != nil {
			return Job{}, fmt.Errorf("order_id must be a UUID")
		}
	}
	if typ == "controlled_failure" {
		var request struct {
			DelayMS   int `json:"delay_ms"`
			FailCount int `json:"fail_count"`
		}
		if json.Unmarshal(canonical, &request) != nil || request.DelayMS < 0 || request.DelayMS > 60000 || request.FailCount < 0 || request.FailCount > 100 {
			return Job{}, fmt.Errorf("controlled_failure delay_ms and fail_count are out of bounds")
		}
	}
	fingerprint, e := json.Marshal(struct {
		Version     int             `json:"version"`
		Type        string          `json:"job_type"`
		Payload     json.RawMessage `json:"payload"`
		MaxAttempts int             `json:"max_attempts"`
	}{1, typ, canonical, max})
	if e != nil {
		return Job{}, e
	}
	sum := sha256.Sum256(fingerprint)
	hash := hex.EncodeToString(sum[:])
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return Job{}, e
	}
	defer tx.Rollback()
	var j Job
	j = Job{ID: uuid.NewString(), Type: typ, Payload: canonical, Status: "QUEUED", Generation: 1, MaxAttempts: max}

	var inserted string
	e = tx.QueryRowContext(ctx, `INSERT INTO jobs(id,job_type,payload,idempotency_key,request_hash,status,max_attempts) VALUES($1,$2,$3,$4,$5,'QUEUED',$6) ON CONFLICT(idempotency_key) DO NOTHING RETURNING id::text`, j.ID, typ, canonical, key, hash, max).Scan(&inserted)
	if errors.Is(e, sql.ErrNoRows) {
		var oldHash string
		e = tx.QueryRowContext(ctx, `SELECT id::text,job_type,payload,status,generation,attempts_started,attempts_in_epoch,max_attempts,coalesce(last_error_message,''),request_hash FROM jobs WHERE idempotency_key=$1 FOR UPDATE`, key).Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Generation, &j.Attempts, &j.EpochAttempts, &j.MaxAttempts, &j.Error, &oldHash)
		if e != nil {
			return Job{}, e
		}
		if oldHash != hash {
			return Job{}, errors.New("idempotency conflict")
		}
		return j, tx.Commit()
	}
	if e != nil {
		return Job{}, e
	}
	if typ == "fulfill_order" {
		var request struct {
			OrderID    string `json:"order_id"`
			CustomerID string `json:"customer_id"`
		}
		if e = json.Unmarshal(canonical, &request); e != nil {
			return Job{}, e
		}
		var exists bool
		if e = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM orders WHERE id=$1 AND customer_id=$2)`, request.OrderID, request.CustomerID).Scan(&exists); e != nil {
			return Job{}, e
		}
		if !exists {
			return Job{}, fmt.Errorf("order does not exist or customer does not match")
		}
	}
	if e = s.addOutbox(ctx, tx, j); e != nil {
		return Job{}, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_CREATED',1,'{}')`, j.ID); e != nil {
		return Job{}, e
	}
	return j, tx.Commit()
}
func (s *System) addOutbox(ctx context.Context, tx *sql.Tx, j Job) error {
	return s.addOutboxEvent(ctx, tx, j, "JOB_DISPATCH")
}
func (s *System) addOutboxEvent(ctx context.Context, tx *sql.Tx, j Job, eventType string) error {
	id := uuid.NewString()
	b, _ := json.Marshal(Dispatch{EventID: id, JobID: j.ID, Generation: j.Generation, Version: 1})
	_, e := tx.ExecContext(ctx, `INSERT INTO job_outbox(id,job_id,job_generation,event_type,payload,status) VALUES($1,$2,$3,$4,$5,'PENDING')`, id, j.ID, j.Generation, eventType, b)
	return e
}
func (s *System) Publish(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, max(s.C.QueryTimeout, 10*time.Second))
	defer cancel()
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	rows, e := tx.QueryContext(ctx, `SELECT id::text,payload FROM job_outbox WHERE (status='PENDING' AND (locked_until IS NULL OR locked_until<now())) OR (status='PUBLISHING' AND locked_until<now()) ORDER BY created_at LIMIT 32 FOR UPDATE SKIP LOCKED`)
	if e != nil {
		tx.Rollback()
		return e
	}
	type item struct {
		id string
		b  []byte
	}
	items := []item{}
	for rows.Next() {
		var x item
		if e = rows.Scan(&x.id, &x.b); e != nil {
			break
		}
		items = append(items, x)
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		tx.Rollback()
		return e
	}
	rows.Close()
	if e != nil {
		tx.Rollback()
		return e
	}
	for _, x := range items {
		_, e = tx.ExecContext(ctx, `UPDATE job_outbox SET status='PUBLISHING',locked_by=$2,locked_until=now()+interval '30 seconds',publish_attempts=publish_attempts+1 WHERE id=$1`, x.id, s.C.WorkerID)
		if e != nil {
			tx.Rollback()
			return e
		}
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	for _, x := range items {
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, e = s.JS.Publish(ctx2, "jobs.ready", x.b, jetstream.WithMsgID(x.id))
		cancel()
		if e != nil {
			_, _ = s.DB.ExecContext(ctx, `UPDATE job_outbox SET status='PENDING',locked_by=NULL,locked_until=now()+make_interval(secs => LEAST(60,power(2,LEAST(publish_attempts,6))::int)),last_error=$2 WHERE id=$1`, x.id, e.Error())
			continue
		}
		var sent Dispatch
		if e = json.Unmarshal(x.b, &sent); e != nil {
			return e
		}
		markTx, markErr := s.DB.BeginTx(ctx, nil)
		if markErr != nil {
			return markErr
		}
		if _, markErr = markTx.ExecContext(ctx, `UPDATE job_outbox SET status='PUBLISHED',published_at=now(),locked_by=NULL,locked_until=NULL,last_error=NULL WHERE id=$1`, x.id); markErr != nil {
			markTx.Rollback()
			return markErr
		}
		if _, markErr = markTx.ExecContext(ctx, `UPDATE jobs SET last_dispatched_at=now() WHERE id=$1 AND generation=$2`, sent.JobID, sent.Generation); markErr != nil {
			markTx.Rollback()
			return markErr
		}
		if _, markErr = markTx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_DISPATCHED',$2,jsonb_build_object('event_id',$3::text))`, sent.JobID, sent.Generation, sent.EventID); markErr != nil {
			markTx.Rollback()
			return markErr
		}
		if markErr = markTx.Commit(); markErr != nil {
			return markErr
		}
	}
	return nil
}
func (s *System) RunPublisher(ctx context.Context) error {
	t := time.NewTicker(s.C.Poll)
	defer t.Stop()
	for {
		if err := s.Publish(ctx); err != nil {
			s.Log.Error("outbox publish failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
func (s *System) RunScheduler(ctx context.Context) error {
	t := time.NewTicker(s.C.Poll)
	defer t.Stop()
	for {
		if e := s.Recover(ctx); e != nil {
			s.Log.Error("recovery scan failed", "error", e)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
func (s *System) Recover(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.C.QueryTimeout)
	defer cancel()
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	rows, e := tx.QueryContext(ctx, `SELECT id::text,status,generation,attempts_in_epoch,max_attempts FROM jobs WHERE (status='RUNNING' AND lease_expires_at<now()) OR (status='RETRY_SCHEDULED' AND available_at<=now()) ORDER BY updated_at LIMIT 64 FOR UPDATE SKIP LOCKED`)
	if e != nil {
		return e
	}
	type rec struct {
		id, status    string
		gen           int64
		attempts, max int
	}
	var due []rec
	for rows.Next() {
		var r rec
		if e = rows.Scan(&r.id, &r.status, &r.gen, &r.attempts, &r.max); e != nil {
			rows.Close()
			return e
		}
		due = append(due, r)
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return e
	}
	rows.Close()
	for _, r := range due {
		if r.status == "RUNNING" {
			if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_LEASE_EXPIRED',$2,'{}')`, r.id, r.gen); e != nil {
				return e
			}
			if _, e = tx.ExecContext(ctx, `UPDATE job_attempts SET outcome='ABANDONED',finished_at=now(),error_code='LEASE_EXPIRED',error_message='worker lease expired' WHERE job_id=$1 AND generation=$2 AND outcome='RUNNING'`, r.id, r.gen); e != nil {
				return e
			}
			if r.attempts >= r.max {
				if _, e = tx.ExecContext(ctx, `UPDATE jobs SET status='DEAD',lease_owner=NULL,lease_expires_at=NULL,last_error_code='ATTEMPTS_EXHAUSTED',last_error_message='worker lease expired',updated_at=now() WHERE id=$1`, r.id); e != nil {
					return e
				}
				_, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_DEAD',$2,'{"reason":"lease expired"}')`, r.id, r.gen)
			} else {
				if _, e = tx.ExecContext(ctx, `UPDATE jobs SET status='QUEUED',generation=generation+1,fence_epoch=fence_epoch+1,lease_owner=NULL,lease_expires_at=NULL,available_at=now(),updated_at=now() WHERE id=$1`, r.id); e != nil {
					return e
				}
				j := Job{ID: r.id, Generation: r.gen + 1}
				if e = s.addOutbox(ctx, tx, j); e != nil {
					return e
				}
				_, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_RECOVERED',$2,'{}')`, r.id, r.gen+1)
			}
		} else {
			if _, e = tx.ExecContext(ctx, `UPDATE jobs SET status='QUEUED',generation=generation+1,updated_at=now() WHERE id=$1`, r.id); e != nil {
				return e
			}
			j := Job{ID: r.id, Generation: r.gen + 1}
			if e = s.addOutbox(ctx, tx, j); e != nil {
				return e
			}
			_, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_RETRY_RELEASED',$2,'{}')`, r.id, r.gen+1)
		}
		if e != nil {
			return e
		}
	}
	repairRows, e := tx.QueryContext(ctx, `SELECT id::text,generation FROM jobs WHERE status='QUEUED' AND GREATEST(COALESCE(last_dispatched_at,created_at),COALESCE(last_repair_at,created_at),updated_at) < now()-$1::interval ORDER BY updated_at LIMIT 32 FOR UPDATE SKIP LOCKED`, fmt.Sprintf("%f seconds", s.C.DispatchRepairInterval.Seconds()))
	if e != nil {
		return e
	}
	type repair struct {
		id  string
		gen int64
	}
	var repairs []repair
	for repairRows.Next() {
		var r repair
		if e = repairRows.Scan(&r.id, &r.gen); e != nil {
			repairRows.Close()
			return e
		}
		repairs = append(repairs, r)
	}
	if e = repairRows.Err(); e != nil {
		repairRows.Close()
		return e
	}
	repairRows.Close()
	for _, r := range repairs {
		if _, e = tx.ExecContext(ctx, `UPDATE jobs SET last_repair_at=now() WHERE id=$1`, r.id); e != nil {
			return e
		}
		if e = s.addOutboxEvent(ctx, tx, Job{ID: r.id, Generation: r.gen}, "JOB_DISPATCH_REPAIR"); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_DISPATCH_REPAIRED',$2,'{}')`, r.id, r.gen); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *System) RunWorker(ctx context.Context) error {
	if e := s.ensureConsumer(); e != nil {
		return e
	}
	consumer, e := s.JS.Consumer(ctx, "JOBS", "job-workers")
	if e != nil {
		return e
	}
	sem := make(chan struct{}, s.C.WorkerConcurrency)
	var wg sync.WaitGroup
	var activeMu sync.Mutex
	active := map[int]context.CancelFunc{}
	var nextActiveID int
	for ctx.Err() == nil {
		free := cap(sem) - len(sem)
		if free < 1 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		batch, fetchErr := consumer.Fetch(free, jetstream.FetchMaxWait(time.Second))
		if fetchErr != nil && !errors.Is(fetchErr, jetstream.ErrNoMessages) {
			if ctx.Err() != nil {
				break
			}
			s.Log.Error("fetch failed", "error", fetchErr)
			continue
		}
		if fetchErr == nil {
			for msg := range batch.Messages() {
				if ctx.Err() != nil {
					_ = msg.NakWithDelay(time.Second)
					break
				}
				sem <- struct{}{}
				jobCtx, cancel := context.WithCancel(context.Background())
				activeMu.Lock()
				nextActiveID++
				activeID := nextActiveID
				active[activeID] = cancel
				activeMu.Unlock()
				wg.Add(1)
				go func(m jetstream.Msg, activeID int, cancel context.CancelFunc, jobCtx context.Context) {
					defer wg.Done()
					defer func() { <-sem; activeMu.Lock(); delete(active, activeID); activeMu.Unlock(); cancel() }()
					if err := s.process(jobCtx, m); err != nil {
						s.Log.Error("job processing failed", "error", err)
						_ = m.NakWithDelay(time.Second)
					} else {
						_ = m.Ack()
					}
				}(msg, activeID, cancel, jobCtx)
			}
			if batchErr := batch.Error(); batchErr != nil && !errors.Is(batchErr, jetstream.ErrNoMessages) {
				s.Log.Error("fetch batch failed", "error", batchErr)
			}
		}
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(s.C.ShutdownTimeout):
	}
	activeMu.Lock()
	for _, cancel := range active {
		cancel()
	}
	activeMu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("worker shutdown timed out with active handlers")
	}
	return nil
}

type jobMessage interface {
	Data() []byte
	Ack() error
	NakWithDelay(time.Duration) error
	InProgress() error
	Term() error
}

func (s *System) process(ctx context.Context, m jobMessage) error {
	var d Dispatch
	if e := json.Unmarshal(m.Data(), &d); e != nil || d.Version != 1 || d.EventID == "" || d.JobID == "" || d.Generation < 1 {
		return m.Term()
	}
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	var j Job
	e = tx.QueryRowContext(ctx, `UPDATE jobs SET status='RUNNING',lease_owner=$3,fence_epoch=fence_epoch+1,attempts_started=attempts_started+1,attempts_in_epoch=attempts_in_epoch+1,lease_expires_at=now()+$4::interval,updated_at=now() WHERE id=$1 AND generation=$2 AND status='QUEUED' AND available_at<=now() RETURNING id::text,job_type,payload,status,generation,attempts_started,attempts_in_epoch,max_attempts`, d.JobID, d.Generation, s.C.WorkerID, fmt.Sprintf("%f seconds", s.C.Lease.Seconds())).Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Generation, &j.Attempts, &j.EpochAttempts, &j.MaxAttempts)
	if errors.Is(e, sql.ErrNoRows) {
		tx.Rollback()
		return nil
	}
	if e != nil {
		tx.Rollback()
		return e
	}
	var fence int64
	if e = tx.QueryRowContext(ctx, `SELECT fence_epoch FROM jobs WHERE id=$1`, j.ID).Scan(&fence); e != nil {
		tx.Rollback()
		return e
	}
	attempt := uuid.NewString()
	_, e = tx.ExecContext(ctx, `INSERT INTO job_attempts(id,job_id,generation,fence_epoch,worker_id,attempt_number,outcome) VALUES($1,$2,$3,$4,$5,$6,'RUNNING')`, attempt, j.ID, j.Generation, fence, s.C.WorkerID, j.Attempts)
	if e == nil {
		_, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,worker_id,generation,fence_epoch,metadata) VALUES($1,'JOB_CLAIMED',$2,$3,$4,'{}')`, j.ID, s.C.WorkerID, j.Generation, fence)
	}
	if e != nil {
		tx.Rollback()
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	s.Log.Info("job claimed", "job_id", j.ID, "worker_id", s.C.WorkerID, "attempt_id", attempt, "generation", j.Generation, "fence_epoch", fence, "event_id", d.EventID)
	workCtx, cancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	heartbeatStop := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(s.C.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatStop:
				return
			case <-workCtx.Done():
				return
			case <-ticker.C:
				hbCtx, hbCancel := context.WithTimeout(workCtx, 2*time.Second)
				res, hbErr := s.DB.ExecContext(hbCtx, `UPDATE jobs SET lease_expires_at=now()+$5::interval,updated_at=now() WHERE id=$1 AND status='RUNNING' AND generation=$2 AND lease_owner=$3 AND fence_epoch=$4 AND lease_expires_at>now()`, j.ID, j.Generation, s.C.WorkerID, fence, fmt.Sprintf("%f seconds", s.C.Lease.Seconds()))
				if hbErr == nil {
					var n int64
					n, hbErr = res.RowsAffected()
					if n != 1 && hbErr == nil {
						hbErr = errors.New("lease ownership lost")
					}
				}
				if hbErr == nil {
					hbErr = m.InProgress()
				}
				hbCancel()
				if hbErr != nil {
					s.Log.Warn("lease heartbeat failed; cancelling handler", "job_id", j.ID, "error", hbErr)
					cancel()
					return
				}
			}
		}
	}()
	stopHeartbeat := func() { close(heartbeatStop); <-heartbeatDone; cancel() }
	defer stopHeartbeat()
	var p struct {
		OrderID    string `json:"order_id"`
		CustomerID string `json:"customer_id"`
		DelayMS    int    `json:"delay_ms"`
		FailCount  int    `json:"fail_count"`
		Permanent  bool   `json:"permanent"`
	}
	if e = json.Unmarshal(j.Payload, &p); e != nil {
		return e
	}
	if j.Type == "controlled_failure" && !s.C.DemoModeEnabled {
		return s.fail(workCtx, j, attempt, fence, errors.New("controlled failures disabled on this worker"), true)
	}
	if p.DelayMS > 0 {
		timer := time.NewTimer(time.Duration(p.DelayMS) * time.Millisecond)
		select {
		case <-workCtx.Done():
			timer.Stop()
			return workCtx.Err()
		case <-timer.C:
		}
	}
	if j.Type == "controlled_failure" && p.Permanent {
		return s.fail(workCtx, j, attempt, fence, errors.New("controlled permanent failure"), true)
	}
	if j.Type == "controlled_failure" && j.EpochAttempts <= p.FailCount {
		return s.fail(workCtx, j, attempt, fence, errors.New("controlled retryable failure"), false)
	}
	if err := s.complete(workCtx, j, attempt, fence, p.OrderID); err != nil {
		s.Log.Warn("job completion failed", "job_id", j.ID, "worker_id", s.C.WorkerID, "attempt_id", attempt, "generation", j.Generation, "fence_epoch", fence, "error", err)
		if errors.Is(err, ErrPermanentFailure) {
			return s.fail(workCtx, j, attempt, fence, err, true)
		}
		return err
	}
	s.Log.Info("job completed", "job_id", j.ID, "worker_id", s.C.WorkerID, "attempt_id", attempt, "generation", j.Generation, "fence_epoch", fence)
	return nil
}
func (s *System) fail(ctx context.Context, j Job, attempt string, fence int64, why error, permanent bool) error {
	ctx, cancel := context.WithTimeout(ctx, s.C.QueryTimeout)
	defer cancel()
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var owned string
	e = tx.QueryRowContext(ctx, `SELECT id::text FROM jobs WHERE id=$1 AND status='RUNNING' AND generation=$2 AND fence_epoch=$3 AND lease_owner=$4 AND lease_expires_at>now() FOR UPDATE`, j.ID, j.Generation, fence, s.C.WorkerID).Scan(&owned)
	if errors.Is(e, sql.ErrNoRows) {
		if _, e = tx.ExecContext(ctx, `UPDATE job_attempts SET outcome='FENCED_OUT',finished_at=now(),error_code='FENCED_OUT',error_message='attempt no longer owns the active lease' WHERE id=$1 AND outcome='RUNNING'`, attempt); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,worker_id,generation,fence_epoch,metadata) VALUES($1,'JOB_FENCED_OUT',$2,$3,$4,jsonb_build_object('attempt_id',$5::text))`, j.ID, s.C.WorkerID, j.Generation, fence, attempt); e != nil {
			return e
		}
		return tx.Commit()
	}
	if e != nil {
		return e
	}
	status := "RETRY_SCHEDULED"
	outcome := "FAILED_RETRYABLE"
	if permanent || j.EpochAttempts >= j.MaxAttempts {
		status = "DEAD"
		if permanent {
			outcome = "FAILED_PERMANENT"
		}
	}
	if _, e = tx.ExecContext(ctx, `UPDATE job_attempts SET outcome=$2,finished_at=now(),error_code='HANDLER_ERROR',error_message=$3 WHERE id=$1 AND outcome='RUNNING'`, attempt, outcome, why.Error()); e != nil {
		return e
	}
	res, e := tx.ExecContext(ctx, `UPDATE jobs SET status=$2,lease_owner=NULL,lease_expires_at=NULL,last_error_code='HANDLER_ERROR',last_error_message=$3,available_at=now()+($4 * interval '1 second'),updated_at=now() WHERE id=$1 AND status='RUNNING' AND generation=$5 AND fence_epoch=$6 AND lease_owner=$7`, j.ID, status, why.Error(), retryDelay(j.EpochAttempts).Seconds(), j.Generation, fence, s.C.WorkerID)
	if e != nil {
		return e
	}
	n, e := res.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return fmt.Errorf("job ownership changed during failure transition")
	}
	event := "JOB_RETRY_SCHEDULED"
	if status == "DEAD" {
		event = "JOB_DEAD"
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,worker_id,generation,fence_epoch,metadata) VALUES($1,$2,$3,$4,$5,jsonb_build_object('error',$6::text))`, j.ID, event, s.C.WorkerID, j.Generation, fence, why.Error()); e != nil {
		return e
	}
	return tx.Commit()
}
func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		return time.Second
	}
	exponent := min(attempt-1, 6)
	return time.Duration(min(60, 1<<exponent)) * time.Second
}
func (s *System) complete(ctx context.Context, j Job, attempt string, fence int64, orderID string) error {
	ctx, cancel := context.WithTimeout(ctx, s.C.QueryTimeout)
	defer cancel()
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var got string
	e = tx.QueryRowContext(ctx, `SELECT id::text FROM jobs WHERE id=$1 AND status='RUNNING' AND generation=$2 AND fence_epoch=$3 AND lease_owner=$4 AND lease_expires_at>now() FOR UPDATE`, j.ID, j.Generation, fence, s.C.WorkerID).Scan(&got)
	if e != nil {
		return e
	}
	if j.Type == "generate_report" {
		var total, fulfilled int
		if e = tx.QueryRowContext(ctx, `SELECT count(*),(SELECT count(*) FROM order_fulfillments) FROM orders`).Scan(&total, &fulfilled); e != nil {
			return e
		}
		result, _ := json.Marshal(map[string]int{"orders": total, "fulfilled": fulfilled, "pending": total - fulfilled})
		var prior []byte
		e = tx.QueryRowContext(ctx, `SELECT result FROM job_results WHERE job_id=$1`, j.ID).Scan(&prior)
		if e == nil {
			if !jsonEqual(prior, result) {
				return fmt.Errorf("%w: existing report result differs from recomputed result", ErrPermanentFailure)
			}
		} else if errors.Is(e, sql.ErrNoRows) {
			if _, e = tx.ExecContext(ctx, `INSERT INTO job_results(job_id,result) VALUES($1,$2)`, j.ID, result); e != nil {
				return e
			}
		} else {
			return e
		}
	}
	if j.Type == "fulfill_order" {
		var existing string
		e = tx.QueryRowContext(ctx, `SELECT job_id::text FROM order_fulfillments WHERE order_id=$1`, orderID).Scan(&existing)
		if e == nil && existing != j.ID {
			return fmt.Errorf("%w: order already fulfilled by another job", ErrPermanentFailure)
		}
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if errors.Is(e, sql.ErrNoRows) {
			if _, e = tx.ExecContext(ctx, `INSERT INTO order_fulfillments(id,order_id,job_id) VALUES($1,$2,$3)`, uuid.NewString(), orderID, j.ID); e != nil {
				return e
			}
		}
	}
	jobResult, e := tx.ExecContext(ctx, `UPDATE jobs SET status='SUCCEEDED',lease_owner=NULL,lease_expires_at=NULL,completed_at=now(),updated_at=now() WHERE id=$1 AND status='RUNNING' AND generation=$2 AND lease_owner=$3 AND fence_epoch=$4`, j.ID, j.Generation, s.C.WorkerID, fence)
	if e != nil {
		return e
	}
	jobRows, e := jobResult.RowsAffected()
	if e != nil {
		return e
	}
	if jobRows != 1 {
		return fmt.Errorf("job completion lost ownership")
	}
	attemptResult, e := tx.ExecContext(ctx, `UPDATE job_attempts SET outcome='SUCCEEDED',finished_at=now() WHERE id=$1 AND outcome='RUNNING'`, attempt)
	if e != nil {
		return e
	}
	attemptRows, e := attemptResult.RowsAffected()
	if e != nil {
		return e
	}
	if attemptRows != 1 {
		return fmt.Errorf("claimed attempt was not active")
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO job_events(job_id,event_type,worker_id,generation,fence_epoch,metadata) VALUES($1,'JOB_COMPLETED',$2,$3,$4,'{}')`, j.ID, s.C.WorkerID, j.Generation, fence)
	if e != nil {
		return e
	}
	return tx.Commit()
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	ax, _ := json.Marshal(x)
	by, _ := json.Marshal(y)
	return string(ax) == string(by)
}

func canonicalJSON(raw []byte) ([]byte, error) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("payload must be valid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("payload must contain one JSON value")
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, fmt.Errorf("payload must be a JSON object")
	}
	return json.Marshal(value)
}

// Redrive starts a new generation for a dead job while preserving prior attempts.
func (s *System) Redrive(ctx context.Context, jobID, actor string) (Job, error) {
	ctx, cancel := context.WithTimeout(ctx, s.C.QueryTimeout)
	defer cancel()
	if _, err := uuid.Parse(jobID); err != nil {
		return Job{}, fmt.Errorf("invalid job id")
	}
	if len(actor) > 120 {
		return Job{}, fmt.Errorf("actor is too long")
	}
	if actor == "" {
		actor = "admin"
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()
	var j Job
	if err = tx.QueryRowContext(ctx, "SELECT id::text,job_type,payload,status,generation,attempts_started,attempts_in_epoch,max_attempts,coalesce(last_error_message,'') FROM jobs WHERE id=$1 FOR UPDATE", jobID).Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Generation, &j.Attempts, &j.EpochAttempts, &j.MaxAttempts, &j.Error); err != nil {
		return Job{}, err
	}
	if j.Status != "DEAD" {
		return Job{}, fmt.Errorf("only DEAD jobs can be redriven")
	}
	if j.Type == "fulfill_order" {
		var p map[string]string
		if err = json.Unmarshal(j.Payload, &p); err != nil {
			return Job{}, err
		}
		var existing string
		err = tx.QueryRowContext(ctx, "SELECT job_id::text FROM order_fulfillments WHERE order_id=$1", p["order_id"]).Scan(&existing)
		if err == nil {
			return Job{}, fmt.Errorf("order already has a committed fulfillment")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Job{}, err
		}
	}
	res, err := tx.ExecContext(ctx, "UPDATE jobs SET status='QUEUED',generation=generation+1,fence_epoch=fence_epoch+1,attempts_in_epoch=0,available_at=now(),lease_owner=NULL,lease_expires_at=NULL,last_error_code=NULL,last_error_message=NULL,completed_at=NULL,updated_at=now() WHERE id=$1 AND status='DEAD'", jobID)
	if err != nil {
		return Job{}, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		if err != nil {
			return Job{}, err
		}
		return Job{}, fmt.Errorf("job state changed during redrive")
	}
	j.Status = "QUEUED"
	j.Generation++
	j.EpochAttempts = 0
	if err = s.addOutbox(ctx, tx, j); err != nil {
		return Job{}, err
	}
	meta, _ := json.Marshal(map[string]string{"actor": actor})
	if _, err = tx.ExecContext(ctx, "INSERT INTO job_events(job_id,event_type,generation,metadata) VALUES($1,'JOB_REDRIVEN',$2,$3)", jobID, j.Generation, meta); err != nil {
		return Job{}, err
	}
	return j, tx.Commit()
}
