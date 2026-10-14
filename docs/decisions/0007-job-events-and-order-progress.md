# 0007 — Job events, order progress and the order board

**Date:** 2026-10-14 · **Status:** accepted

## Context
After Phase 2b, robots fulfil pick jobs, but nothing records it: orders stay `NEW` in Postgres, `order.events` only says what was created, and the UI cannot show an order board. Progress must reach the database and the browser without losing a completion, and without the duplicates and reordering that at-least-once delivery from several dispatchers brings.

## Decisions
- **Two topics with different jobs.**
  - `job.events` (new, keyed by order ID): raw facts from dispatchers (`JOB_ASSIGNED`, `JOB_PICKED`, `JOB_COMPLETED`, `JOB_REQUEUED`, `JOB_DEAD_LETTERED`), each with the attempt number. At least once, may repeat.
  - `order.events`: still written only by the order service through its outbox. The order service consumes `job.events`, updates Postgres, and records each *real* change as an order event in the same transaction. Consumers (gateway now, ClickHouse later) read one clean, ordered stream whose content matches the database.

  The alternative, letting the dispatcher write to `order.events` directly, would mix two writers in one topic, give no ordering against the outbox's events, and push deduplication onto every reader.
- **A job is settled on RabbitMQ only after Kafka confirms its final event.** On `COMPLETED` the job enters a `FINISHING` phase: it is acked once `JOB_COMPLETED` is confirmed (acks=all). If Kafka is down, the publish is retried every tick and the job stays unacked, so the outcome is never lost. Non-final events are sent without waiting; losing one only skips an intermediate status.
- **Dispatchers dead-letter on their own, after `max-attempts` (3).** The attempt number comes from the quorum queue's `x-delivery-count` header. When a failed attempt was the last one, the dispatcher publishes `JOB_DEAD_LETTERED` with the reason, waits for the confirm, then rejects without requeue, which sends the job to `pick-jobs.dlq`. Every delivery counts, including the redelivery after a dispatcher dies or restarts: RabbitMQ increments `x-delivery-count` when a closed channel returns its unacked jobs. So a dispatcher restart uses up one attempt for each job it held. The queue's delivery limit (5) stays as the backstop for a job whose dispatcher keeps dying. The job stays visible in the DLQ for Phase 3's DLQ panel, and the order service learns why it failed.
- **Idempotent progress in the order service** (`JobProgress`): lock the order row, then the job row, the same order everywhere so no deadlocks. A `COMPLETED`/`FAILED` job never changes again. An event from an earlier attempt than the one stored is ignored. An event that changes nothing writes nothing. Within one attempt, status only moves forward (`PENDING` < `ASSIGNED` < `PICKED` < final), except a requeue back to `PENDING`.
  - Order status: `NEW` → `IN_PROGRESS` on the first assignment (`ORDER_STARTED`), `COMPLETED` when every job is (`ORDER_COMPLETED`), `FAILED` as soon as one job is dead-lettered (`ORDER_FAILED` with the reason).
- **Gateway order board.** A second broadcast consumer (no group) reads `order.events` starting 30 minutes back, via `offsetsForTimes`, so a restarted gateway shows recent orders at once without replaying the whole topic. It keeps the newest 200 orders, dropping finished ones first, and ignores events for orders created before its window. It dedupes by event ID because the outbox delivers at least once.
  - Served on a **new** socket, `/ws/orders` (snapshot, then deltas of whole orders plus `removed` IDs), and `GET /api/orders`. `/ws/fleet` is unchanged, so the web session's existing client keeps working.
