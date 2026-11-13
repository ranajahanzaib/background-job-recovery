# Job lifecycle

A submission validates the bounded JSON payload and hashes a versioned canonical representation, then stores the job, audit event, and dispatch outbox row in one transaction. A repeated idempotency key with the same job type, canonical payload, and retry limit returns the original row; a different fingerprint conflicts.

A worker message is only a wake-up notification. A short conditional update claims a queued matching generation, increments the fence and attempt counters, and stores an attempt. The handler runs outside that transaction. For fulfillment, a separate final transaction locks the job row and validates status, generation, worker, epoch, and database-time lease before inserting the business row and setting `SUCCEEDED`.

Retryable failures move to `RETRY_SCHEDULED` with bounded exponential delay. The scheduler moves due retries to a new dispatch generation. It also invalidates and requeues expired leases until the attempt budget is exhausted, then moves the job to `DEAD`. Automatic messages cannot transition a succeeded job back to execution.
