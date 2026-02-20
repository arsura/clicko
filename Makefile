BINARY_NAME := clicko
BUILD_DIR := bin

COMPOSE_FILE := dev/cluster/docker-compose.yaml

.PHONY: build clean test cluster-up cluster-down cluster-restart cluster-logs cluster-status

build:
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/cli

clean:
	rm -rf $(BUILD_DIR)

test:
	go test ./... -v -count=1

cluster-up:
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for ClickHouse cluster to be healthy..."
	@docker compose -f $(COMPOSE_FILE) exec ch-1-1 bash -c 'until clickhouse-client -q "SELECT 1" >/dev/null 2>&1; do sleep 1; done'
	@echo "Cluster is ready."

cluster-down:
	docker compose -f $(COMPOSE_FILE) down -v

cluster-restart: cluster-down cluster-up

cluster-logs:
	docker compose -f $(COMPOSE_FILE) logs -f

cluster-status:
	docker compose -f $(COMPOSE_FILE) ps
