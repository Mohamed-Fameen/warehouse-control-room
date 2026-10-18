# Progress log

Newest entries first. Every Claude Code session appends one entry before it ends, then commits and pushes.

Template:

```
## YYYY-MM-DD — <area: services | web | rl | infra> — Phase <n>
**Done:** what changed (files, features)
**Next:** the next concrete step
**Blockers / notes:** anything another session should know
```

---

## 2026-10-14 — services, infra — Phase 2b
**Done:**
- **`services/common` changed:** `RobotCommand` (`ASSIGN_JOB`, jobId, pick cell), `RobotJobUpdate` + `RobotJobState` (`ACCEPTED`, `REJECTED`, `PICKED`, `COMPLETED`, `FAILED`), `Topics.robotJob` / `allRobotJobs`. **New MQTT topic** `wh/{site}/robot/{id}/job` (QoS 1, retained except `REJECTED`).
- `services/simulator`: `SIM_MODE` = `autonomous` (default, Phase 1 behaviour) or `dispatched` (robots take commands from `.../cmd`, report on `.../job`, park on a charger between jobs). Robots refuse jobs when busy, broken, at or below 40 % battery, or for a cell that is not a pick spot.
- `services/dispatcher`: consumes `pick-jobs` with manual ack and holds each job until its robot reports `COMPLETED`. Nearest free robot, 5 s assignment timeout, requeue on failure, offline, broken or a 3 min timeout. Takes over redelivered jobs from the robot already running them. `GET /api/dispatcher`. See ADR 0006 and the README "Dispatcher" section.
- **Shared infra changed:** compose runs the simulator with `SIM_MODE=dispatched` and wires the dispatcher to Mosquitto and RabbitMQ.
- Chaos-tested in compose at 60 orders/min. Killing the dispatcher: the restarted one adopted running jobs, with 0 rejections. Killing the simulator: all 20 active jobs were requeued; after a restart work resumed, with 0 jobs in the DLQ. This testing found two bugs, both fixed and unit-tested (see ADR 0006).
- README: fixed bytes that were not valid UTF-8 (`…`, `×`) in the Gateway section.

**Next:** Phase 2c. Job lifecycle events (`JOB_ASSIGNED`, `JOB_PICKED`, `JOB_COMPLETED`, `JOB_FAILED`) on `order.events`, order status in the order service, then orders and the DLQ in the gateway for the UI.
**Blockers / notes:** **Web session:** with compose's default `SIM_MODE=dispatched` and 6 orders/min, most robots sit on chargers. For lively robots, raise the rate (`PUT /api/generator`, see README) or run with `SIM_MODE=autonomous`. Gateway message shapes are unchanged. Robots still do not avoid each other.

## 2026-10-13 — services, infra — Phase 2a
**Done:**
- **`services/common` changed:** `PickJobMessage` (jobId, orderId, sku, qty, pick cell, createdAt), `OrderEvent` + `OrderEventType` (`ORDER_CREATED`, `JOB_CREATED`), `Queues` (RabbitMQ names), `Topics.KAFKA_ORDER_EVENTS`.
- `services/order-service`: orders and pick jobs in PostgreSQL (plain JDBC + Flyway). One job per order line; SKUs map 1:1 to pick spots (`SKU-0001` to `SKU-0256`). A transactional outbox relays jobs to RabbitMQ `wh.jobs` → quorum queue `pick-jobs` (delivery limit 5, dead letters to `pick-jobs.dlq`) and events to Kafka `order.events` keyed by order ID. Delivery is at least once, ordered per destination; publisher confirms on both brokers. A random order generator (default 6/min, `PUT /api/generator`) is the hook for Phase 3 rush hour. See ADR 0005 and the README "Order service" section.
- Services run in UTC (`postgres:17.9` rejects `Asia/Calcutta`); tests run with `-Duser.timezone=UTC`.
- **Shared infra changed:** compose adds Postgres 17.9, the `order.events` topic, and the order-service entry.

**Next:** Phase 2b, the dispatcher: consume `pick-jobs`, assign each job to an idle robot, and send it over MQTT `.../cmd`. The simulator must accept commands instead of picking random jobs.
**Blockers / notes:** Nothing consumes `pick-jobs` yet, so jobs pile up in the queue while the generator runs. Set `ORDERS_PER_MINUTE=0` if that gets in the way.

## 2026-10-13 — services, infra — Phase 1c
**Done:**
- `services/gateway`: reads Kafka `robot.status` from the beginning (compacted) and `robot.telemetry` from the end, with no consumer group (every replica needs every robot). Merges both into one view per robot. Telemetry older than an `OFFLINE` status is ignored, so a clean shutdown cannot revive a robot.
- `GET /api/layout` (floor plan in metres, racks as rectangles), `GET /api/robots`, and WebSocket `/ws/fleet`: a `snapshot` on connect, then a `delta` every 100 ms with only the robots that changed. Each robot in a delta is complete; the client upserts by `id`. Slow clients are disconnected and get a fresh snapshot when they reconnect. See ADR 0004 and the README "Gateway" section.
- **`services/common` changed:** the warehouse layout moved from the simulator to `com.warehouse.common.layout` (`WarehouseGrid`, `Cell`, `Rect`). Simulator behaviour is unchanged.
- **Shared infra changed:** the gateway's compose entry is wired to Kafka.
- Measured with 20 robots: 9.6 deltas/s, about 15 KB/s per browser, telemetry age in the browser p50 76 ms, p95 108 ms.

**Next:** web session: build the 3D UI against `/api/layout` and `/ws/fleet`. Services session: Phase 2a, the order service.
**Blockers / notes:** **Contract for the web session:** the gateway message shapes in the README "Gateway" section. Position fields can be `null` for a robot known only from its status. Allowed origins default to any localhost port and must be narrowed before the public launch.

## 2026-10-11 — services, infra — Phase 1b
**Done:**
- `services/bridge`: subscribes to `$share/bridge/wh/+/robot/+/telemetry` and `wh/+/robot/+/status`, produces to Kafka `robot.telemetry` and a new compacted `robot.status` topic, keyed by robot ID. Payloads are forwarded unparsed; the MQTT topic goes in the `mqtt_topic` header. See ADR 0003.
- Uses clean MQTT sessions: with persistent sessions, Mosquitto kept routing a stopped replica's share of telemetry to it, and that share was lost.
- **Shared infra changed:** `kafka-init` creates the compacted `robot.status` topic; the bridge's compose entry is wired to Mosquitto and Kafka.

**Next:** Phase 1c, the gateway (Kafka to browser over WebSocket).
**Blockers / notes:** Status changes that happen while the bridge is down are not replayed one by one; the retained MQTT status brings back the latest state on reconnect.

## 2026-10-11 — services, infra — Phase 1a
**Done:**
- **`services/common` changed:** added `RobotTelemetry` (robotId, site, seq, ts, x, y, heading, speed, battery, status) and `RobotStatusMessage` (robotId, status, ts) records. Units are in the Javadoc: metres from the south-west corner, radians counter-clockwise from +x, battery in percent, epoch-millisecond timestamps.
- `services/simulator`: 20 robots (`ROBOT_COUNT`, max 37) on a 40 x 26 m grid warehouse, each with its own MQTT 5 connection (HiveMQ client 1.3.6) on its own virtual thread. Telemetry on `wh/dc1/robot/{id}/telemetry` at 5 Hz, QoS 0. Retained `wh/dc1/robot/{id}/status` on every status change; the last will sets `OFFLINE`, and a clean shutdown publishes `OFFLINE` itself. Robots loop shelf → pick → pack station → drop and charge when low. Layout, BFS pathing and robot behaviour are unit-tested. See ADR 0002.
- **Shared infra changed:** the simulator's compose entry now sets `MQTT_HOST=mosquitto`, passes `ROBOT_COUNT` and waits for Mosquitto to be healthy. README has a simulator section.
- Smoke-tested: about 97 msgs/s for 20 robots; a force-killed process flips every robot to `OFFLINE` through the last will; `docker compose stop simulator` publishes `OFFLINE` with a real timestamp.

**Next:** Phase 1b. The bridge subscribes to `wh/+/robot/+/telemetry` (shared subscription if it ever runs more than one replica) and produces to Kafka `robot.telemetry` keyed by robot ID. The web session can already build against the telemetry payload above. The floor layout (shelves, chargers, pack stations) lives only in `WarehouseGrid` in the simulator. The UI and the dispatcher will need it, so it should probably move to `common` or be served by the gateway.
**Blockers / notes:** Robots do not avoid each other yet; they can overlap. A robot whose battery reaches 0 stays `BROKEN` until restart. That should not happen with the default tuning.

## 2026-10-10 — services, infra — Phase 0
**Done:**
- `services/`: Maven multi-module (parent + `common`, `simulator`, `bridge`, `order-service`, `dispatcher`, `gateway`) on Spring Boot 3.5.16 / Java 21, with the Maven wrapper committed. `common` has `Topics` (MQTT topic templates, `robot.telemetry`) and the `RobotStatus` enum. Each service is an empty Boot app with actuator health, virtual threads, a fixed port (gateway 8080, simulator 8081, bridge 8082, order-service 8083, dispatcher 8084) and a Dockerfile. `./mvnw verify` passes.
- `infra/compose/docker-compose.yml`: Mosquitto 2.0.22 (1883, ws 9001), Kafka 4.3.1 KRaft (host `localhost:9092`, internal `kafka:29092`), `kafka-init` creates `robot.telemetry` (6 partitions), RabbitMQ 4.3.6 with the management UI (5672/15672, `wcr`/`wcr` via `.env`), Redpanda Console v3.12.0 (8090). App services sit under the `apps` profile. Smoke-tested: all containers healthy, MQTT pub/sub works, topic created, gateway image builds and reports UP.
- Root `Makefile` (`up`, `down`, `build`, `apps`, `logs`, `ps`), `README.md`, `.gitignore`, ADR `docs/decisions/0001-local-stack-and-service-skeleton.md`. Moved `PROGRESS.md` into `docs/`.

**Next:** Phase 1. Simulator publishes telemetry to MQTT, then the bridge forwards it to Kafka `robot.telemetry`. Add a `RobotTelemetry` record to `common` first. Separately, scaffold the Vite React app under `web/` (not done this session).
**Blockers / notes:** `make` is not installed on this Windows machine; install it (`winget install ezwinports.make`) or run the compose commands directly. When running `docker exec` from Git Bash, set `MSYS_NO_PATHCONV=1`, or Git Bash rewrites paths like `/opt/...`.

## 2026-10-10 — planning — Phase 0
**Done:** Roadmap written; CLAUDE.md and this log created.
**Next:** Create the repo skeleton: Maven multi-module under `services/`, Vite React app under `web/`, `infra/compose/docker-compose.yml` with Mosquitto, Kafka (KRaft), RabbitMQ and Redpanda Console.
**Blockers / notes:** none
