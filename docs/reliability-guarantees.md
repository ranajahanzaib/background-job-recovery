# Reliability guarantees

- JetStream delivery is at least once. Duplicate dispatches and repeated handler execution are possible.
- PostgreSQL is authoritative for state, attempts, leases, and business effects.
- A conditional database claim permits one current owner for a job generation. Lease heartbeats are conditional on the same generation, owner, and fencing epoch and also issue JetStream in-progress acknowledgments.
- Finalization validates the current owner, generation, fence, and lease in the same transaction as the fulfillment insert and success transition.
- `UNIQUE(order_id)` prevents a second committed fulfillment row. A conflicting existing fulfillment from another job is treated as an error.
- Outbox publication can repeat after uncertainty; business correctness does not depend on JetStream's deduplication window.
- Eventual recovery assumes PostgreSQL and JetStream eventually become available and scheduler/publisher processes continue running.
- None of these guarantees makes an arbitrary external side effect atomic. Use downstream idempotency support or an idempotent outbox consumer.
