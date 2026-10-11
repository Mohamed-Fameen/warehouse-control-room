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
