# Architecture

The API writes a job and its first dispatch outbox row in one PostgreSQL transaction. The publisher locks a bounded batch with `FOR UPDATE SKIP LOCKED`, commits a publisher lease, then publishes to JetStream and marks the row published after server acknowledgment. Uncertain publish outcomes are retried and can duplicate transport messages.

Workers consume a shared durable pull consumer. Dispatches carry an event ID, job ID, generation, and version. A conditional update claims only a matching queued generation. The worker records an attempt and claim event in that transaction. Business completion locks and validates the job before inserting fulfillment and marking success atomically. The scheduler reclaims expired leases and releases due retries into a new generation and outbox event.

# Job lifecycle

The supported persisted states are `QUEUED`, `RUNNING`, `RETRY_SCHEDULED`, `SUCCEEDED`, and `DEAD`. Claims increment an epoch and execution counter. Retryable errors schedule a persisted delay; expired leases become a new dispatch generation or dead-letter when the attempt budget is exhausted. Completed jobs are terminal under automatic processing.
