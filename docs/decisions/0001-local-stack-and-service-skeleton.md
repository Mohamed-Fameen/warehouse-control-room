# 0001 — Local stack and service skeleton

**Date:** 2026-10-10 · **Status:** accepted

## Context
Phase 0 needs a build every session can rely on and a local stack that matches the architecture in CLAUDE.md.

## Decisions
- **Maven multi-module under `services/`**, parent inherits `spring-boot-starter-parent` 3.5.16. The Maven wrapper (`mvnw`, Maven 3.9.16, `only-script` type) is committed so no local Maven install is needed. Group ID `com.warehouse`, packages `com.warehouse.<module>`.
- **Every service is a Spring Boot app** with web + actuator and `spring.threads.virtual.enabled=true`. That gives each one `/actuator/health` for compose and Kubernetes probes and keeps them all the same shape. `common` is a plain jar with no Spring in it.
- **Ports:** gateway 8080, simulator 8081, bridge 8082, order-service 8083, dispatcher 8084, Redpanda Console 8090.
- **Kafka (apache/kafka 4.3.1, KRaft, single node)** has two listeners: `kafka:29092` for containers and `localhost:9092` for apps run from the IDE. A one-shot `kafka-init` container creates `robot.telemetry` with 6 partitions.
- **App services in compose sit under the `apps` profile**, so `make up` starts only the infrastructure while services are still run from IntelliJ. Each Dockerfile builds from the `services/` context with `./mvnw -pl <module> -am`.
- **RabbitMQ credentials** come from `infra/compose/.env` (git-ignored) and default to `wcr`/`wcr`. The built-in `guest` user only works from loopback inside the container, so it is not used.
- **Image versions are pinned.** Do not use `latest`.
