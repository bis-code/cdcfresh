.DEFAULT_GOAL := test

.PHONY: test test-unit test-race test-integration infra-up infra-down lint help

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

# The local development stack: the whole pipeline, for running cdcfresh
# against real change data by hand, and for re-capturing canal-json fixtures.
# No test needs it — the integration tier starts its own containers. See
# test/cdcstack/README.md.
infra-up: ## start the full TiDB + TiCDC + Pulsar stack locally (not needed for tests)
	$(COMPOSE) up -d
	./test/cdcstack/bootstrap.sh

infra-down: ## stop the local stack and delete its state
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
