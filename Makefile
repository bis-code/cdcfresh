.DEFAULT_GOAL := test

.PHONY: test test-race test-integration test-all lint help

test: ## unit tier — fakes only, no Docker, sub-second (go test ./...)
	go test ./...

test-race: ## unit tier with the race detector
	go test ./... -race

test-integration: ## integration tier — packages that only build under -tags=integration (needs Docker); skips cleanly if none exist yet
	@base=$$(mktemp); tagged=$$(mktemp); \
	go list ./... | sort > $$base; \
	go list -tags=integration ./... | sort > $$tagged; \
	only=$$(comm -13 $$base $$tagged); \
	rm -f $$base $$tagged; \
	if [ -z "$$only" ]; then \
		echo "no integration packages yet (see issue #1: harness)"; \
		exit 0; \
	fi; \
	go test -tags=integration $$only

test-all: ## both tiers together (go test -tags=integration ./...)
	go test -tags=integration ./...

lint: ## gofmt check (fails if it reports files) + go vet
	@fmt_out=$$(gofmt -l .); \
	if [ -n "$$fmt_out" ]; then \
		echo "gofmt needs to be run on:"; \
		echo "$$fmt_out"; \
		exit 1; \
	fi
	go vet ./...

help: ## list targets with descriptions
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
