.DEFAULT_GOAL := test

.PHONY: test test-unit test-race test-integration cdcstack-up cdcstack-down lint help

COMPOSE := docker compose -f test/cdcstack/docker-compose.yml

test: ## every tier — unit + integration
	go test -tags=integration ./...

test-unit: ## unit tier only
	go test ./...

test-race: ## unit tier with the race detector
	go test ./... -race

test-integration: ## integration tier — packages that only build under -tags=integration (starts its own containers; needs Docker)
	@base=$$(mktemp); tagged=$$(mktemp); \
	go list ./... | sort > $$base; \
	go list -tags=integration ./... | sort > $$tagged; \
	only=$$(comm -13 $$base $$tagged); \
	rm -f $$base $$tagged; \
	if [ -z "$$only" ]; then \
		echo "no integration packages"; \
		exit 0; \
	fi; \
	go test -tags=integration $$only

# The full TiDB + TiCDC + Pulsar stack exists only to re-capture canal-json
# fixtures when a TiDB release might have changed the wire format. No test
# needs it: the integration tier starts its own containers. See
# test/cdcstack/README.md.
cdcstack-up: ## start the full CDC stack (fixture capture only — not needed for tests)
	$(COMPOSE) up -d
	./test/cdcstack/bootstrap.sh

cdcstack-down: ## stop the full CDC stack and delete its state
	$(COMPOSE) down -v

lint: ## gofmt check (fails if it reports files) + go vet, including tagged code
	@fmt_out=$$(gofmt -l .); \
	if [ -n "$$fmt_out" ]; then \
		echo "gofmt needs to be run on:"; \
		echo "$$fmt_out"; \
		exit 1; \
	fi
	go vet ./...
	go vet -tags=integration ./...

help: ## list targets with descriptions
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
