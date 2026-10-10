COMPOSE := docker compose -f infra/compose/docker-compose.yml

.PHONY: up down logs ps build apps

up:  ## Start infrastructure (Mosquitto, Kafka, RabbitMQ, Redpanda Console)
	$(COMPOSE) up -d

down:  ## Stop everything (keeps volumes; add `-v` by hand to wipe data)
	$(COMPOSE) --profile apps down

logs:  ## Follow logs
	$(COMPOSE) logs -f

ps:  ## Show container status
	$(COMPOSE) ps

build:  ## Build and test all Java services
	cd services && ./mvnw -B verify

apps:  ## Start infrastructure plus the application services
	$(COMPOSE) --profile apps up -d --build
