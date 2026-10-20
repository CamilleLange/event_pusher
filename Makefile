BINARY  := event_pusher
BIN_DIR := bin
IMAGE   := event_pusher:local
COMPOSE := docker compose
INFRA   := nats redis minio

.DEFAULT_GOAL := help

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## --- Go ---

.PHONY: build
build:
	CGO_ENABLED=0 go build -trimpath -o $(BIN_DIR)/$(BINARY) .

.PHONY: run-producer
run-producer:
	go run . producer

.PHONY: run-service
run-service:
	go run . service

.PHONY: test
test:
	go test -race ./...

.PHONY: cover
cover:
	go test -cover ./...

.PHONY: fmt
fmt:
	gofmt -w .

.PHONY: vet
vet:
	go vet ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)

## --- Docker ---

.PHONY: docker-build
docker-build:
	$(COMPOSE) build

.PHONY: infra-up
infra-up:
	$(COMPOSE) up -d --wait $(INFRA)

.PHONY: up
up:
	$(COMPOSE) up -d --build --wait

.PHONY: down
down:
	$(COMPOSE) down

.PHONY: reset
reset:
	$(COMPOSE) down -v

.PHONY: logs
logs:
	$(COMPOSE) logs -f producer service

.PHONY: ps
ps:
	$(COMPOSE) ps
