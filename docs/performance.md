# Performance

The repository has no benchmark fixtures or measured throughput results. The demo workload is for correctness and recovery behavior; its timings are not throughput measurements.

Performance measurements for this system should record Go, PostgreSQL, NATS, CPU architecture, connection pool limits, worker count and concurrency, handler delay, submission throughput, end-to-end latency, claim latency, publish latency, and recovery latency. Separate handler delay from queue overhead and compare worker concurrency values of 1, 2, 4, 8, and 16.
