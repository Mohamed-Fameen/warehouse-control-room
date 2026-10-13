# 0005 — Orders, transactional outbox and the pick-job queue

**Date:** 2026-10-13 · **Status:** accepted

## Context
The order service stores orders in PostgreSQL and must (a) hand each pick job to exactly one dispatcher and (b) publish domain events for everyone else. Writing to the database and then to a broker is a dual write: a crash in between, or a broker outage, leaves them disagreeing.

## Decisions
- **Transactional outbox.** `OrderService.create` inserts the order, its pick jobs and their outgoing messages (`outbox` rows) in **one database transaction**. `OutboxRelay` polls every 200 ms, publishes unpublished rows in ID order and sets `published_at`. A message is sent if and only if the order committed.
  - **At least once.** A crash after the broker accepted a message but before the row is marked means it is sent again. The message ID (job ID for jobs, event ID for events) lets consumers drop duplicates.
  - **`FOR UPDATE SKIP LOCKED`** lets several order-service replicas relay in parallel without sending a row twice.
  - **Order is kept per destination, not globally.** When a row fails, later rows for the *same* destination wait, but the other destination carries on. A first version stopped the whole batch, and during a RabbitMQ outage Kafka events piled up for no reason.
  - **Confirmed sends.** Kafka: `send().get()`. RabbitMQ: publisher confirms (`waitForConfirmsOrDie`), so for a quorum queue a majority of replicas have the job before the row is marked. Timeouts are short (Kafka `max.block.ms` 5 s, RabbitMQ connect 3 s) so a dead broker cannot stall the relay.
  - Published rows are purged after a day.
- **RabbitMQ for jobs, Kafka for events.** A pick job must go to *one* worker, be acknowledged, and be retried or parked if the worker fails: a work queue. Events are facts many consumers read independently and replay: a log.
- **Topology** (declared by the order service, idempotent): direct exchange `wh.jobs` → quorum queue `pick-jobs` (routing key `pick`). Dead letters go to `wh.jobs.dlx` → quorum queue `pick-jobs.dlq`.
  - **Quorum queues** (Raft-replicated; classic mirrored queues are gone in RabbitMQ 4) count deliveries. With `x-delivery-limit: 5`, a job rejected or redelivered 5 times is dead-lettered with reason `delivery_limit` instead of looping forever. Verified by hand: delivery count 0 → 5, then the job moved to the DLQ.
- **Kafka `order.events`**, keyed by order ID so one order's events stay in order: `JOB_CREATED` per line, then `ORDER_CREATED`. The event types grow in Phase 2c.
- **Plain JDBC (`JdbcClient`) and Flyway, no JPA.** The outbox needs exact SQL (`SKIP LOCKED`, partial index), and explicit SQL keeps the data flow easy to read.
- **SKUs map 1:1 to pick spots** (`SKU-0001` … one per aisle cell next to a shelf) via the shared layout, so a job carries the exact cell a robot must drive to.
- **Order generator** creates random orders (default 6/min, `ORDERS_PER_MINUTE`, 0 = off), adjustable at runtime with `PUT /api/generator`. This is the hook for Phase 3's "rush hour".
- **Services run in UTC.** pgjdbc sends the JVM time zone to Postgres on connect, and the `postgres:17.9` image rejects legacy names such as `Asia/Calcutta` (this machine's default). `main()` sets UTC, and the parent POM runs tests with `-Duser.timezone=UTC`.
