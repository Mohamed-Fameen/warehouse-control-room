# Warehouse Control Room

A public, interactive warehouse robot simulation. Visitors watch a live robot fleet and trigger chaos (rush hour, broken robots, killed workers) to see the system recover. The project exists to learn MQTT, RabbitMQ, Kafka, ClickHouse, data warehousing, Kubernetes and RL by giving each one a real job.

## Architecture

- **Robot simulator** (Java 21, virtual threads) publishes telemetry over **MQTT** (Mosquitto)
- **Bridge service** forwards MQTT to **Kafka** (`robot.telemetry`, keyed by robot ID)
- **Order service** (Spring Boot + PostgreSQL) creates pick jobs, sends them via **RabbitMQ** to **dispatchers**, and publishes domain events to Kafka using the outbox pattern
- **Dispatchers** assign jobs to robots and send commands over MQTT `.../cmd`
- **Gateway** consumes Kafka and pushes live state to the browser over WebSocket (throttled to about 10 frames per second)
- **Web UI** (React + React Three Fiber / three.js): 3D warehouse with instanced robots and shelves, order board, DLQ panel, chaos buttons. Gateway sends a full snapshot on connect, then deltas; the UI interpolates robot positions between updates.
- Later: ClickHouse (Kafka engine + materialized views), dbt warehouse, Superset, Kubernetes, RL dispatcher

## Repo layout

```
services/           Maven multi-module (open in IntelliJ)
  common/           shared message schemas, topic names, enums
  simulator/
  bridge/
  order-service/
  dispatcher/
  gateway/
web/                React frontend (open in VS Code)
rl/                 Python RL training (later, VS Code)
infra/
  compose/          docker-compose files
  helm/             Kubernetes charts (later)
  dbt/              warehouse models (later)
docs/
  PROGRESS.md       running log, every session appends here
  decisions/        short ADRs for notable choices
```

## MQTT topics

- `wh/{site}/robot/{id}/telemetry` — position, battery, status (QoS 0)
- `wh/{site}/robot/{id}/status` — retained; last will sets `offline`
- `wh/{site}/robot/{id}/cmd` — commands from dispatchers (QoS 1)

## Conventions

- Java 21, Spring Boot 3, Maven. Message types live only in `services/common`.
- Message payloads are JSON for now; Schema Registry comes in a later phase.
- Every service has a Dockerfile and an entry in `infra/compose/docker-compose.yml`.
- Small commits with clear messages, one feature branch per session.
- Do not commit secrets. Use `.env` files that are git-ignored.

## How sessions coordinate

Several Claude Code sessions work on this repo at once (IntelliJ for `services/`, VS Code for `web/` and `rl/`).

- Stay inside your area. If you must change `services/common` or shared infra, say so in PROGRESS.md.
- At the end of every session, append an entry to `docs/PROGRESS.md` (newest first): date, area, what changed, what is next, any blockers. Then commit and push.
- Before starting, pull and read the latest PROGRESS.md entries.
- Record notable design choices as a short ADR in `docs/decisions/`.

## Current phase

Phase 0 — repo and local setup. See `docs/PROGRESS.md` for status.

## Roadmap phases

0 Setup · 1 Live robots · 2 Orders with RabbitMQ · 3 Chaos control room · 4 Public launch · 5 ClickHouse · 6 Warehouse with dbt · 7 Kubernetes · 8 RL dispatcher · 9 Observability and polish
