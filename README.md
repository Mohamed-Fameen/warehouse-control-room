# Warehouse Control Room

A live warehouse robot simulation for learning MQTT, RabbitMQ, Kafka and friends. See `CLAUDE.md` for the architecture and `docs/PROGRESS.md` for status.

## Local setup

Needs Docker Desktop, JDK 21 and `make` (on Windows: `winget install ezwinports.make`, or run the `docker compose` commands from the Makefile directly).

```sh
make up      # Mosquitto, Kafka (KRaft), RabbitMQ, Redpanda Console
make build   # build + test all Java services (uses services/mvnw)
make apps    # infra + all services in containers
make down
```

| What | Where |
|---|---|
| MQTT | `localhost:1883` (WebSockets `localhost:9001`) |
| Kafka | `localhost:9092` from your machine, `kafka:29092` from containers |
| RabbitMQ | AMQP `localhost:5672`, UI http://localhost:15672 (`wcr` / `wcr`) |
| Redpanda Console | http://localhost:8090 |

## Robot simulator

Run `SimulatorApplication` from IntelliJ (or `docker compose -f infra/compose/docker-compose.yml --profile apps up -d simulator`) with Mosquitto up. It starts 20 robots (`ROBOT_COUNT`, max 37) on a 40 x 26 m floor and publishes:

- `wh/dc1/robot/{id}/telemetry`: position, heading, speed, battery and status at 5 Hz (QoS 0)
- `wh/dc1/robot/{id}/status`: retained, on every status change; the last will sets `OFFLINE`

Watch it:

```sh
docker exec warehouse-control-room-mosquitto-1 mosquitto_sub -v -t 'wh/dc1/robot/+/status'
```

Tuning lives under `simulator.*` in `services/simulator/src/main/resources/application.yml`.

## MQTT to Kafka bridge

Run `BridgeApplication` (or `docker compose -f infra/compose/docker-compose.yml --profile apps up -d simulator bridge`) with the infrastructure up. It forwards:

| MQTT | Kafka | Key |
|---|---|---|
| `wh/+/robot/+/telemetry` (shared subscription, group `bridge`) | `robot.telemetry` | robot ID |
| `wh/+/robot/+/status` (incl. retained) | `robot.status` (compacted) | robot ID |

Payloads are forwarded unchanged, with the MQTT topic in the `mqtt_topic` header. Browse the topics in Redpanda Console (http://localhost:8090). Counters: http://localhost:8082/actuator/metrics/bridge.messages.forwarded

`BridgeIntegrationTest` runs Mosquitto and Kafka in Testcontainers; it is skipped when Docker is not running.

## Gateway (Kafka to browser)

Run `GatewayApplication` (or add `gateway` to the compose command above). Port 8080.

- `GET /api/layout`: floor plan in metres: `width`, `height`, `racks` (`x`, `y`, `width`, `height`), `chargers` and `packStations` (cells; centre at `x + 0.5`, `y + 0.5`).
- `GET /api/robots`: current state of every robot (same shape as the snapshot below).
- `ws://localhost:8080/ws/fleet`: `{"type":"snapshot","ts":…,"robots":[…]}` once on connect, then `{"type":"delta","ts":…,"robots":[…]}` up to 10×/s, holding only changed robots. Each robot: `id, x, y, heading, speed, battery, status, lastSeen, statusTs`. Upsert by `id`. Position fields are `null` for a robot known only from its status (for example offline before the gateway started).
- `GET /api/orders`: the order board, newest first (same shape as the orders in the snapshot below).
- `ws://localhost:8080/ws/orders`: `{"type":"snapshot","ts":…,"orders":[…],"removed":[]}` once on connect, then `{"type":"delta","ts":…,"orders":[…],"removed":["<orderId>",…]}` up to 10×/s. Upsert orders by `id`; drop the IDs in `removed` (the board keeps the newest 200 orders from the last 30 minutes). Each order: `id`, `status` (`NEW`, `IN_PROGRESS`, `COMPLETED`, `FAILED`), `createdAt`, `updatedAt`, `reason` (why it failed), `jobs`. Each job: `id`, `sku`, `status` (`PENDING`, `ASSIGNED`, `PICKED`, `COMPLETED`, `FAILED`), `robotId`, `reason` (why the last attempt failed). Join `robotId` with `/ws/fleet` to highlight the robot working on a job.

## Order service

Port 8083, needs Postgres, RabbitMQ and Kafka (`docker compose -f infra/compose/docker-compose.yml --profile apps up -d order-service`).

```sh
curl -X POST localhost:8083/api/orders -H 'content-type: application/json'      -d '{"lines":[{"sku":"SKU-0007","qty":2},{"sku":"SKU-0120","qty":1}]}'
curl localhost:8083/api/orders?limit=10
curl -X PUT localhost:8083/api/generator -H 'content-type: application/json' -d '{"perMinute":30}'
```

Each order line becomes a pick job on RabbitMQ `pick-jobs` (see the queues at http://localhost:15672). Events go to Kafka `order.events`. Both are sent through a transactional outbox, so a broker outage delays messages but never loses them. Valid SKUs are `SKU-0001` to `SKU-0256`, one per pick spot. Postgres: `localhost:5432`, database `orders`, `wcr`/`wcr`.


## Dispatcher

Port 8084, needs RabbitMQ and Mosquitto (`docker compose -f infra/compose/docker-compose.yml --profile apps up -d simulator order-service dispatcher`). Compose runs the simulator with `SIM_MODE=dispatched`, so robots wait on their chargers until the dispatcher sends them a job. Run the simulator on its own (or with `SIM_MODE=autonomous`) and robots roam at random as in Phase 1.

- Takes pick jobs from RabbitMQ `pick-jobs` and sends each to the nearest free robot over MQTT `wh/{site}/robot/{id}/cmd`. Robots report progress on the retained `wh/{site}/robot/{id}/job`.
- A job is acked only when the robot reports `COMPLETED` and Kafka has the `JOB_COMPLETED` event. Failed, offline or broken robots put their job back on the queue; after `MAX_ATTEMPTS` (default 3) the dispatcher rejects it to `pick-jobs.dlq`.
- Every step goes to Kafka `job.events`. The order service turns those into job and order status in Postgres and one event per change on `order.events` (`ORDER_STARTED`, `ORDER_COMPLETED`, `ORDER_FAILED`, `JOB_ASSIGNED`, … see `OrderEventType`), which feeds the gateway's order board.
- `GET /api/dispatcher`: the jobs this replica holds and what it knows about each robot.
- Try it: `curl -X PUT localhost:8083/api/generator -H 'content-type: application/json' -d '{"perMinute":60}'`, then `docker compose ... kill simulator` or `kill dispatcher` and watch the jobs come back.

See ADR 0006 and ADR 0007.
