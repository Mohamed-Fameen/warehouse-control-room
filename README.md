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
