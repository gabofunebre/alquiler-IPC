DC ?= docker compose
SERVICE ?=

.PHONY: up down rebuild start stop restart

up:
	$(DC) up -d --build $(SERVICE)

down:
	$(DC) down

rebuild:
	$(DC) down
	$(DC) build --no-cache $(SERVICE)
	$(DC) up -d --force-recreate $(SERVICE)

start:
	$(DC) start $(SERVICE)

stop:
	$(DC) stop $(SERVICE)

restart:
	$(DC) restart $(SERVICE)
