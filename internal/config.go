package internal

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL, NATSURL, WorkerID, HTTPAddr, AdminToken                                   string
	Lease, Heartbeat, AckWait, Poll, ShutdownTimeout, DispatchRepairInterval, QueryTimeout time.Duration
	MaxAttempts, WorkerConcurrency, DBMaxConnections                                       int
	DemoModeEnabled                                                                        bool
}

func Load() (Config, error) {
	var err error
	c := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		NATSURL:     env("NATS_URL", "nats://localhost:4222"), WorkerID: env("WORKER_ID", "worker-local"),
		HTTPAddr: env("HTTP_ADDR", "127.0.0.1:8080"), AdminToken: os.Getenv("ADMIN_TOKEN"),
		Lease: 15 * time.Second, Heartbeat: 3 * time.Second, AckWait: 30 * time.Second, Poll: time.Second,
		ShutdownTimeout: 20 * time.Second, DispatchRepairInterval: 60 * time.Second,
		MaxAttempts: 5, WorkerConcurrency: 4, DBMaxConnections: 20, QueryTimeout: 5 * time.Second,
	}
	for name, target := range map[string]*time.Duration{
		"JOB_LEASE_DURATION": &c.Lease, "JOB_HEARTBEAT_INTERVAL": &c.Heartbeat,
		"NATS_ACK_WAIT": &c.AckWait, "POLL_INTERVAL": &c.Poll,
		"WORKER_SHUTDOWN_TIMEOUT": &c.ShutdownTimeout, "DISPATCH_REPAIR_INTERVAL": &c.DispatchRepairInterval, "DB_QUERY_TIMEOUT": &c.QueryTimeout,
	} {
		if v := os.Getenv(name); v != "" {
			*target, err = time.ParseDuration(v)
			if err != nil {
				return c, fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	for name, target := range map[string]*int{"JOB_DEFAULT_MAX_ATTEMPTS": &c.MaxAttempts, "WORKER_CONCURRENCY": &c.WorkerConcurrency} {
		if v := os.Getenv(name); v != "" {
			*target, err = strconv.Atoi(v)
			if err != nil {
				return c, fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	if v := os.Getenv("DEMO_MODE_ENABLED"); v != "" {
		c.DemoModeEnabled, err = strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("DEMO_MODE_ENABLED: %w", err)
		}
	}
	host, _, addrErr := net.SplitHostPort(c.HTTPAddr)
	publicBind := addrErr == nil && (host == "" || host == "0.0.0.0" || host == "::")
	if addrErr == nil && host != "" && host != "localhost" {
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			publicBind = true
		}
	}
	if publicBind && c.AdminToken == "" {
		return c, fmt.Errorf("ADMIN_TOKEN is required when HTTP_ADDR is not loopback")
	}
	if c.DatabaseURL == "" || c.WorkerID == "" || c.WorkerConcurrency < 1 || c.WorkerConcurrency > 256 || c.DBMaxConnections < 1 || c.QueryTimeout <= 0 || c.MaxAttempts < 1 || c.MaxAttempts > 100 || c.Poll <= 0 || c.ShutdownTimeout <= 0 || c.DispatchRepairInterval <= 0 || c.Heartbeat <= 0 || c.Lease <= c.Heartbeat*2 || c.AckWait <= c.Heartbeat*2 {
		return c, fmt.Errorf("invalid worker, retry, poll, lease, heartbeat, or ack-wait configuration")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
