package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/ranajahanzaib/background-job-recovery/internal"
	"github.com/ranajahanzaib/background-job-recovery/internal/dashboard"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: job-recovery <migrate|publisher|scheduler|worker|serve|enqueue|status|list|inspect|attempts|events|retry|verify|demo>")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c, e := internal.Load()
	if e != nil {
		return e
	}
	ctxDB, dbCancel := context.WithTimeout(ctx, 10*time.Second)
	defer dbCancel()
	s, e := internal.Open(ctxDB, c)
	if e != nil {
		return e
	}
	defer s.Close()
	switch os.Args[1] {
	case "migrate":
		return s.Migrate(ctx)
	case "publisher":
		if e = s.ConnectNATS(); e != nil {
			return e
		}
		if e = s.EnsureStream(); e != nil {
			return e
		}
		return s.RunPublisher(ctx)
	case "scheduler":
		return s.RunScheduler(ctx)
	case "worker":
		if e = s.ConnectNATS(); e != nil {
			return e
		}
		if e = s.EnsureStream(); e != nil {
			return e
		}
		return s.RunWorker(ctx)
	case "serve":
		return serve(ctx, s)
	case "enqueue":
		return enqueue(ctx, s, os.Args[2:])
	case "retry":
		return retry(ctx, s, os.Args[2:])
	case "verify":
		return verifyCLI(ctx, s, os.Args[2:])
	case "status":
		return statusCLI(ctx, s)
	case "list":
		return listCLI(ctx, s, os.Args[2:])
	case "inspect":
		return inspectCLI(ctx, s, os.Args[2:])
	case "attempts":
		return historyCLI(ctx, s, os.Args[2:], "attempts")
	case "events":
		return historyCLI(ctx, s, os.Args[2:], "events")
	case "demo":
		return demo(ctx, s, os.Args[2:])
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}
func serve(ctx context.Context, s *internal.System) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if e := s.DB.PingContext(r.Context()); e != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("POST /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		if s.C.AdminToken != "" && r.Header.Get("Authorization") != "Bearer "+s.C.AdminToken {
			http.Error(w, "unauthorized", 401)
			return
		}
		var req struct {
			JobType        string          `json:"job_type"`
			IdempotencyKey string          `json:"idempotency_key"`
			Payload        json.RawMessage `json:"payload"`
			MaxAttempts    int             `json:"max_attempts"`
		}
		if decodeRequest(w, r, &req, 70<<10) != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		if req.MaxAttempts == 0 {
			req.MaxAttempts = s.C.MaxAttempts
		}
		j, e := s.Enqueue(r.Context(), req.JobType, req.IdempotencyKey, req.Payload, req.MaxAttempts)
		if e != nil {
			status := 400
			if strings.Contains(e.Error(), "conflict") {
				status = 409
			}
			http.Error(w, e.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(j)
	})
	mux.HandleFunc("GET /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 100 {
				limit = n
			}
		}
		offset := 0
		if v := r.URL.Query().Get("offset"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				offset = n
			}
		}
		status := r.URL.Query().Get("status")
		if status != "" && status != "QUEUED" && status != "RUNNING" && status != "RETRY_SCHEDULED" && status != "SUCCEEDED" && status != "DEAD" {
			http.Error(w, "invalid status", 400)
			return
		}
		rows, e := s.DB.QueryContext(r.Context(), `SELECT id::text,job_type,payload,status,generation,attempts_started,attempts_in_epoch,max_attempts,coalesce(last_error_message,'') FROM jobs WHERE ($1='' OR status=$1) ORDER BY created_at DESC LIMIT $2 OFFSET $3`, status, limit, offset)
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer rows.Close()
		out := []internal.Job{}
		for rows.Next() {
			var j internal.Job
			if e = rows.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Generation, &j.Attempts, &j.EpochAttempts, &j.MaxAttempts, &j.Error); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			out = append(out, j)
		}
		if e = rows.Err(); e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("POST /api/demo/seed", func(w http.ResponseWriter, r *http.Request) {
		if !s.C.DemoModeEnabled {
			http.Error(w, "demo mode disabled", 404)
			return
		}
		if s.C.AdminToken != "" && r.Header.Get("Authorization") != "Bearer "+s.C.AdminToken {
			http.Error(w, "unauthorized", 401)
			return
		}
		var req struct {
			Count int `json:"count"`
		}
		if decodeRequest(w, r, &req, 4096) != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		if req.Count == 0 {
			req.Count = 100
		}
		if req.Count < 1 || req.Count > 1000 {
			http.Error(w, "count must be 1..1000", 400)
			return
		}
		runID := uuid.NewString()
		if _, e := s.DB.ExecContext(r.Context(), `INSERT INTO demo_runs(id,expected_jobs) VALUES($1,$2)`, runID, req.Count); e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		for i := 0; i < req.Count; i++ {
			id := uuid.NewString()
			customer := fmt.Sprintf("demo-customer-%d", i)
			if _, e := s.DB.ExecContext(r.Context(), `INSERT INTO orders(id,customer_id) VALUES($1,$2)`, id, customer); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			payload, _ := json.Marshal(map[string]any{"order_id": id, "customer_id": customer, "delay_ms": 700})
			job, e := s.Enqueue(r.Context(), "fulfill_order", "demo-"+id, payload, s.C.MaxAttempts)
			if e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			if _, e = s.DB.ExecContext(r.Context(), `UPDATE jobs SET demo_run_id=$1 WHERE id=$2`, runID, job.ID); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"run_id": runID, "seeded": req.Count})
	})
	mux.HandleFunc("GET /api/demo/verify", func(w http.ResponseWriter, r *http.Request) {
		result, e := verifyDemo(r.Context(), s, r.URL.Query().Get("run_id"))
		if e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if result["verified"] == false {
			w.WriteHeader(http.StatusConflict)
		}
		_ = json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("GET /api/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		var j internal.Job
		e := s.DB.QueryRowContext(r.Context(), "SELECT id::text,job_type,payload,status,generation,attempts_started,attempts_in_epoch,max_attempts,coalesce(last_error_message,'') FROM jobs WHERE id=$1", r.PathValue("id")).Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Generation, &j.Attempts, &j.EpochAttempts, &j.MaxAttempts, &j.Error)
		if e != nil {
			http.Error(w, "job not found", 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(j)
	})
	mux.HandleFunc("GET /api/jobs/{id}/attempts", func(w http.ResponseWriter, r *http.Request) {
		rows, e := s.DB.QueryContext(r.Context(), "SELECT id::text,generation,fence_epoch,worker_id,attempt_number,started_at,finished_at,outcome,error_code,error_message FROM job_attempts WHERE job_id=$1 ORDER BY started_at", r.PathValue("id"))
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var id, worker, outcome string
			var gen, fence int64
			var no int
			var started time.Time
			var finished sql.NullTime
			var code, msg sql.NullString
			if e = rows.Scan(&id, &gen, &fence, &worker, &no, &started, &finished, &outcome, &code, &msg); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			out = append(out, map[string]any{"id": id, "generation": gen, "fence_epoch": fence, "worker_id": worker, "attempt_number": no, "started_at": started, "finished_at": finished, "outcome": outcome, "error_code": code, "error_message": msg})
		}
		if e = rows.Err(); e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("GET /api/jobs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		rows, e := s.DB.QueryContext(r.Context(), "SELECT id,event_type,worker_id,generation,fence_epoch,metadata,created_at FROM job_events WHERE job_id=$1 ORDER BY id", r.PathValue("id"))
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var id int64
			var typ string
			var worker sql.NullString
			var gen, fence sql.NullInt64
			var metadata []byte
			var created time.Time
			if e = rows.Scan(&id, &typ, &worker, &gen, &fence, &metadata, &created); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			out = append(out, map[string]any{"id": id, "event_type": typ, "worker_id": worker, "generation": gen, "fence_epoch": fence, "metadata": json.RawMessage(metadata), "created_at": created})
		}
		if e = rows.Err(); e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("POST /api/jobs/{id}/redrive", func(w http.ResponseWriter, r *http.Request) {
		if s.C.AdminToken != "" && r.Header.Get("Authorization") != "Bearer "+s.C.AdminToken {
			http.Error(w, "unauthorized", 401)
			return
		}
		j, e := s.Redrive(r.Context(), r.PathValue("id"), r.Header.Get("X-Actor"))
		if e != nil {
			http.Error(w, e.Error(), 409)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(j)
	})
	mux.HandleFunc("GET /api/workers", func(w http.ResponseWriter, r *http.Request) {
		rows, e := s.DB.QueryContext(r.Context(), "SELECT lease_owner,count(*) FROM jobs WHERE status='RUNNING' GROUP BY lease_owner ORDER BY lease_owner")
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var id string
			var n int
			if e = rows.Scan(&id, &n); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			out = append(out, map[string]any{"worker_id": id, "active_jobs": n})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		rows, e := s.DB.QueryContext(r.Context(), "SELECT id,job_id::text,event_type,worker_id,generation,fence_epoch,metadata,created_at FROM job_events ORDER BY id DESC LIMIT 100")
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var id int64
			var jobID, typ string
			var worker sql.NullString
			var gen, fence sql.NullInt64
			var metadata []byte
			var created time.Time
			if e = rows.Scan(&id, &jobID, &typ, &worker, &gen, &fence, &metadata, &created); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			out = append(out, map[string]any{"id": id, "job_id": jobID, "event_type": typ, "worker_id": worker, "generation": gen, "fence_epoch": fence, "metadata": json.RawMessage(metadata), "created_at": created})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		rows, e := s.DB.QueryContext(r.Context(), "SELECT status,count(*) FROM jobs GROUP BY status")
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer rows.Close()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for rows.Next() {
			var state string
			var n int64
			if e = rows.Scan(&state, &n); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			fmt.Fprintf(w, "job_recovery_jobs_by_status{status=%q} %d\n", state, n)
		}
		if e = rows.Err(); e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		rows.Close()
		metrics := []struct{ name, query string }{
			{"job_recovery_jobs_created_total", "SELECT count(*) FROM job_events WHERE event_type='JOB_CREATED'"},
			{"job_recovery_jobs_completed_total", "SELECT count(*) FROM job_events WHERE event_type='JOB_COMPLETED'"},
			{"job_recovery_jobs_recovered_total", "SELECT count(*) FROM job_events WHERE event_type='JOB_RECOVERED'"},
			{"job_recovery_lease_expirations_total", "SELECT count(*) FROM job_events WHERE event_type='JOB_LEASE_EXPIRED'"},
			{"job_recovery_fencing_rejections_total", "SELECT count(*) FROM job_events WHERE event_type='JOB_FENCED_OUT'"},
			{"job_recovery_dispatch_repairs_total", "SELECT count(*) FROM job_events WHERE event_type='JOB_DISPATCH_REPAIRED'"},
			{"job_recovery_outbox_pending", "SELECT count(*) FROM job_outbox WHERE status<>'PUBLISHED'"},
			{"job_recovery_outbox_publish_failures_total", "SELECT count(*) FROM job_outbox WHERE last_error IS NOT NULL"},
			{"job_recovery_worker_active_jobs", "SELECT count(*) FROM jobs WHERE status='RUNNING'"},
		}
		for _, m := range metrics {
			var n int64
			if e = s.DB.QueryRowContext(r.Context(), m.query).Scan(&n); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			fmt.Fprintf(w, "%s %d\n", m.name, n)
		}
	})
	mux.HandleFunc("GET /api/stats", func(w http.ResponseWriter, r *http.Request) {
		rows, e := s.DB.QueryContext(r.Context(), `SELECT status,count(*) FROM jobs GROUP BY status`)
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer rows.Close()
		v := map[string]int{}
		for rows.Next() {
			var k string
			var n int
			if e = rows.Scan(&k, &n); e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			v[k] = n
		}
		if e = rows.Err(); e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboard.HTML)
	})
	limited := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if _, err := uuid.Parse(requestID); err != nil {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		reqCtx, cancel := context.WithTimeout(r.Context(), s.C.QueryTimeout)
		defer cancel()
		started := time.Now()
		mux.ServeHTTP(w, r.WithContext(reqCtx))
		s.Log.Info("http request", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
	srv := &http.Server{Addr: s.C.HTTPAddr, Handler: limited, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	return srv.ListenAndServe()
}
func enqueue(ctx context.Context, s *internal.System, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	typ := fs.String("type", "generate_report", "job type")
	key := fs.String("key", uuid.NewString(), "idempotency key")
	payload := fs.String("payload", `{}`, "JSON payload")
	if e := fs.Parse(args); e != nil {
		return e
	}
	j, e := s.Enqueue(ctx, *typ, *key, json.RawMessage(*payload), s.C.MaxAttempts)
	if e == nil {
		_ = json.NewEncoder(os.Stdout).Encode(j)
	}
	return e
}

func statusCLI(ctx context.Context, s *internal.System) error {
	rows, e := s.DB.QueryContext(ctx, "SELECT status,count(*) FROM jobs GROUP BY status ORDER BY status")
	if e != nil {
		return e
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		if e = rows.Scan(&state, &n); e != nil {
			return e
		}
		out[state] = n
	}
	if e = rows.Err(); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
func listCLI(ctx context.Context, s *internal.System, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	status := fs.String("status", "", "status filter")
	limit := fs.Int("limit", 50, "maximum rows")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *limit < 1 || *limit > 100 {
		return errors.New("limit must be 1..100")
	}
	if *status != "" && *status != "QUEUED" && *status != "RUNNING" && *status != "RETRY_SCHEDULED" && *status != "SUCCEEDED" && *status != "DEAD" {
		return errors.New("invalid status")
	}
	rows, e := s.DB.QueryContext(ctx, "SELECT id::text,job_type,payload,status,generation,attempts_started,attempts_in_epoch,max_attempts,coalesce(last_error_message,'') FROM jobs WHERE ($1='' OR status=$1) ORDER BY created_at DESC LIMIT $2", *status, *limit)
	if e != nil {
		return e
	}
	defer rows.Close()
	out := []internal.Job{}
	for rows.Next() {
		var j internal.Job
		if e = rows.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Generation, &j.Attempts, &j.EpochAttempts, &j.MaxAttempts, &j.Error); e != nil {
			return e
		}
		out = append(out, j)
	}
	if e = rows.Err(); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
func inspectCLI(ctx context.Context, s *internal.System, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	id := fs.String("id", "", "job UUID")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *id == "" {
		return errors.New("inspect requires --id")
	}
	var j internal.Job
	e := s.DB.QueryRowContext(ctx, "SELECT id::text,job_type,payload,status,generation,attempts_started,attempts_in_epoch,max_attempts,coalesce(last_error_message,'') FROM jobs WHERE id=$1", *id).Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Generation, &j.Attempts, &j.EpochAttempts, &j.MaxAttempts, &j.Error)
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(j)
}
func historyCLI(ctx context.Context, s *internal.System, args []string, kind string) error {
	fs := flag.NewFlagSet(kind, flag.ContinueOnError)
	id := fs.String("id", "", "job UUID")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *id == "" {
		return fmt.Errorf("%s requires --id", kind)
	}
	query := ""
	if kind == "attempts" {
		query = "SELECT id::text,generation,fence_epoch,worker_id,attempt_number,started_at,finished_at,outcome,error_code,error_message FROM job_attempts WHERE job_id=$1 ORDER BY started_at"
	} else {
		query = "SELECT id,event_type,worker_id,generation,fence_epoch,metadata,created_at FROM job_events WHERE job_id=$1 ORDER BY id"
	}
	rows, e := s.DB.QueryContext(ctx, query, *id)
	if e != nil {
		return e
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		if kind == "attempts" {
			var aid, worker, outcome string
			var gen, fence int64
			var no int
			var started time.Time
			var finished sql.NullTime
			var code, msg sql.NullString
			if e = rows.Scan(&aid, &gen, &fence, &worker, &no, &started, &finished, &outcome, &code, &msg); e != nil {
				return e
			}
			out = append(out, map[string]any{"id": aid, "generation": gen, "fence_epoch": fence, "worker_id": worker, "attempt_number": no, "started_at": started, "finished_at": finished, "outcome": outcome, "error_code": code, "error_message": msg})
		} else {
			var eid int64
			var typ string
			var worker sql.NullString
			var gen, fence sql.NullInt64
			var metadata []byte
			var created time.Time
			if e = rows.Scan(&eid, &typ, &worker, &gen, &fence, &metadata, &created); e != nil {
				return e
			}
			out = append(out, map[string]any{"id": eid, "event_type": typ, "worker_id": worker, "generation": gen, "fence_epoch": fence, "metadata": json.RawMessage(metadata), "created_at": created})
		}
	}
	if e = rows.Err(); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
func verifyDemo(ctx context.Context, s *internal.System, runID string) (map[string]any, error) {
	if _, e := uuid.Parse(runID); e != nil {
		return nil, errors.New("valid --run-id is required")
	}
	var expected, orders, total, succeeded, dead, unfinished, effects, distinctOrders, duplicates, missing, orphaned, recovered, abandoned, retries, fenced int
	var lastFailure sql.NullString
	q := `WITH run_jobs AS (SELECT * FROM jobs WHERE demo_run_id=$1),
	       run_orders AS (SELECT DISTINCT (payload->>'order_id')::uuid AS id FROM run_jobs WHERE payload ? 'order_id'),
	       run_effects AS (SELECT f.* FROM order_fulfillments f JOIN run_jobs j ON j.id=f.job_id)
	SELECT d.expected_jobs,
	 (SELECT count(*) FROM run_orders ro JOIN orders o ON o.id=ro.id),
	 (SELECT count(*) FROM run_jobs),
	 (SELECT count(*) FROM run_jobs WHERE status='SUCCEEDED'),
	 (SELECT count(*) FROM run_jobs WHERE status='DEAD'),
	 (SELECT count(*) FROM run_jobs WHERE status IN ('QUEUED','RUNNING','RETRY_SCHEDULED')),
	 (SELECT count(*) FROM run_effects),
	 (SELECT count(DISTINCT order_id) FROM run_effects),
	 (SELECT count(*) FROM (SELECT order_id FROM run_effects GROUP BY order_id HAVING count(*)>1) dup),
	 (SELECT count(*) FROM run_orders ro JOIN orders o ON o.id=ro.id WHERE NOT EXISTS (SELECT 1 FROM run_effects f WHERE f.order_id=ro.id)),
	 (SELECT count(*) FROM run_effects f WHERE NOT EXISTS (SELECT 1 FROM run_orders ro WHERE ro.id=f.order_id)),
	 (SELECT count(*) FROM job_events e JOIN run_jobs j ON j.id=e.job_id WHERE e.event_type='JOB_RECOVERED'),
	 (SELECT count(*) FROM job_attempts a JOIN run_jobs j ON j.id=a.job_id WHERE a.outcome='ABANDONED'),
	 (SELECT count(*) FROM job_attempts a JOIN run_jobs j ON j.id=a.job_id WHERE a.outcome='FAILED_RETRYABLE'),
	 (SELECT count(*) FROM job_events e JOIN run_jobs j ON j.id=e.job_id WHERE e.event_type='JOB_FENCED_OUT'),
	 (SELECT error_message FROM run_jobs WHERE last_error_message IS NOT NULL ORDER BY updated_at DESC LIMIT 1)
	FROM demo_runs d WHERE d.id=$1`
	e := s.DB.QueryRowContext(ctx, q, runID).Scan(&expected, &orders, &total, &succeeded, &dead, &unfinished, &effects, &distinctOrders, &duplicates, &missing, &orphaned, &recovered, &abandoned, &retries, &fenced, &lastFailure)
	if e != nil {
		return nil, e
	}
	verified := expected > 0 && expected == orders && expected == total && expected == succeeded && dead == 0 && unfinished == 0 && effects == expected && distinctOrders == expected && duplicates == 0 && missing == 0 && orphaned == 0
	return map[string]any{"run_id": runID, "expected_orders": expected, "orders_found": orders, "expected_jobs": expected, "total_jobs": total, "succeeded_jobs": succeeded, "dead_jobs": dead, "unfinished_jobs": unfinished, "fulfillment_records": effects, "distinct_fulfilled_orders": distinctOrders, "duplicate_fulfillments": duplicates, "missing_fulfillments": missing, "orphan_fulfillments": orphaned, "recovery_events": recovered, "abandoned_attempts": abandoned, "retry_attempts": retries, "stale_ownership_rejections": fenced, "last_failure": lastFailure.String, "verified": verified}, nil
}
func demoStatusCLI(ctx context.Context, s *internal.System, args []string) error {
	fs := flag.NewFlagSet("demo status", flag.ContinueOnError)
	id := fs.String("run-id", "", "demo run UUID")
	if e := fs.Parse(args); e != nil {
		return e
	}
	result, e := verifyDemo(ctx, s, *id)
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func verifyCLI(ctx context.Context, s *internal.System, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	id := fs.String("run-id", "", "demo run UUID")
	if e := fs.Parse(args); e != nil {
		return e
	}
	result, e := verifyDemo(ctx, s, *id)
	if e != nil {
		return e
	}
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if result["verified"] != true {
		return errors.New("demo verification failed")
	}
	return nil
}
func retry(ctx context.Context, s *internal.System, args []string) error {
	fs := flag.NewFlagSet("retry", flag.ContinueOnError)
	id := fs.String("id", "", "dead job UUID")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *id == "" {
		return errors.New("retry requires --id")
	}
	j, e := s.Redrive(ctx, *id, "cli:"+s.C.WorkerID)
	if e == nil {
		_ = json.NewEncoder(os.Stdout).Encode(j)
	}
	return e
}
func demo(ctx context.Context, s *internal.System, args []string) error {
	if len(args) > 0 && args[0] == "verify" {
		return verifyCLI(ctx, s, args[1:])
	}
	if len(args) > 0 && args[0] == "status" {
		return demoStatusCLI(ctx, s, args[1:])
	}
	if len(args) == 0 || args[0] != "seed" {
		return errors.New("usage: demo seed [--count N] | demo status --run-id ID | demo verify --run-id ID")
	}
	fs := flag.NewFlagSet("demo seed", flag.ContinueOnError)
	n := fs.Int("count", 100, "number of orders/jobs")
	if e := fs.Parse(args[1:]); e != nil {
		return e
	}
	if *n < 1 || *n > 1000 {
		return errors.New("count must be 1..1000")
	}
	runID := uuid.NewString()
	if _, e := s.DB.ExecContext(ctx, `INSERT INTO demo_runs(id,expected_jobs) VALUES($1,$2)`, runID, *n); e != nil {
		return e
	}
	for i := 0; i < *n; i++ {
		id := uuid.NewString()
		_, e := s.DB.ExecContext(ctx, `INSERT INTO orders(id,customer_id) VALUES($1,$2)`, id, fmt.Sprintf("demo-customer-%d", i))
		if e != nil {
			return e
		}
		p, _ := json.Marshal(map[string]any{"order_id": id, "customer_id": fmt.Sprintf("demo-customer-%d", i), "delay_ms": 700})
		job, e := s.Enqueue(ctx, "fulfill_order", "demo-"+id, p, s.C.MaxAttempts)
		if e != nil {
			return e
		}
		if _, e = s.DB.ExecContext(ctx, `UPDATE jobs SET demo_run_id=$1 WHERE id=$2`, runID, job.ID); e != nil {
			return e
		}
	}
	fmt.Printf("Seeded %d orders in run %s\n", *n, runID)
	return nil
}

func decodeRequest(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("request must contain one JSON value")
	}
	return nil
}
