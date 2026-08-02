# Passerelle SMS — developer entry points (plan §1.3, M0).
#
# Every target here is also what CI runs: if it passes locally it passes there, and the reverse.

SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE      := github.com/martialanouman/go-gateway
MIGRATIONS  := migrations
COMPOSE     := docker compose

# Pinned tool versions. Guessing these is how a lint run passes locally and fails in CI.
GOLANGCI_VERSION   := v2.12.2
SQLC_VERSION       := v1.30.0
GOVULNCHECK_VERSION := latest
OASDIFF_VERSION    := v1.26.0
BUF_VERSION              := v1.72.0
# protoc-gen-go tracks the google.golang.org/protobuf runtime version in go.mod — keep them in step.
PROTOC_GEN_GO_VERSION      := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

## ---------------------------------------------------------------------------- tooling

.PHONY: tools
tools: ## Install the Go binaries the workflow needs (sqlc, govulncheck, oasdiff, golangci-lint, buf + protoc plugins)
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	go install github.com/oasdiff/oasdiff@$(OASDIFF_VERSION)
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

## ---------------------------------------------------------------------------- dependencies

.PHONY: up
up: ## Start Postgres, Redis, Kafka and ClickHouse, and wait until they are healthy
	$(COMPOSE) up -d --wait

.PHONY: down
down: ## Stop the dependencies, keeping their volumes
	$(COMPOSE) down

.PHONY: down-clean
down-clean: ## Stop the dependencies AND delete their data
	$(COMPOSE) down -v

.PHONY: migrate
migrate: ## Apply the pending Postgres migrations (make migrate CMD=down to reverse them)
	go run ./cmd/migrate -store postgres -dir $(MIGRATIONS) $(or $(CMD),up)

.PHONY: migrate-clickhouse
migrate-clickhouse: ## Apply the pending ClickHouse CDR migrations (CMD=down to reverse them)
	go run ./cmd/migrate -store clickhouse $(or $(CMD),up)

# Topics are infrastructure schema, so they are provisioned like one — a deliberate operator act, never
# at a service's boot (step-201 D7). Without this, KAFKA_TOPIC_PARTITIONS changes nothing and every
# topic sits at whatever the broker auto-created: one partition, hence an inter-pod parallelism of one.
#
# RF defaults to 1 because the local docker-compose broker is a single Redpanda node and cannot satisfy
# the production default of 3 (spec §2.5). Against a real cluster, export
# KAFKA_TOPIC_REPLICATION_FACTOR — an exported value wins over this default.
#
# Widening a topic re-maps key -> partition on the live data plane: run it outside peak hours.
.PHONY: kafka-topics
kafka-topics: ## Create the Kafka topics and widen them to KAFKA_TOPIC_PARTITIONS: make kafka-topics [PARTITIONS=12] [RF=1] [DRY_RUN=1]
	KAFKA_TOPIC_REPLICATION_FACTOR=$(or $(RF),$(KAFKA_TOPIC_REPLICATION_FACTOR),1) \
	$(if $(PARTITIONS),KAFKA_TOPIC_PARTITIONS=$(PARTITIONS)) \
	go run ./cmd/kafka-provision $(if $(DRY_RUN),-dry-run)

## ---------------------------------------------------------------------------- build & run

.PHONY: build
build: ## Compile every cmd/
	go build ./...

.PHONY: run
run: ## Run a service: make run SVC=router-svc
	@if [ -z "$(SVC)" ]; then echo "usage: make run SVC=router-svc"; exit 2; fi
	go run ./cmd/$(SVC)

.PHONY: fake-smsc
fake-smsc: ## Run the in-repo fake SMSC (SMPP peer) on :2775 for local pipeline runs
	go run ./cmd/fake-smsc

.PHONY: smsc-sim
SMSC_SIM_IMAGE ?= smsc-simulator:dev
SMSC_SIM_REPO  ?= https://github.com/martialanouman/go-smsc-simulator
SMSC_SIM_REF   ?= main
smsc-sim: ## Build the real SMSC simulator image ($(SMSC_SIM_IMAGE)) used by M8 resilience tests (internal/testutil/smscsim). Pin with SMSC_SIM_REF=<tag>; force a rebuild by removing the image first.
	@if docker image inspect $(SMSC_SIM_IMAGE) >/dev/null 2>&1; then \
		echo "$(SMSC_SIM_IMAGE) already present (docker rmi it to rebuild)"; \
	else \
		echo "Building $(SMSC_SIM_IMAGE) from $(SMSC_SIM_REPO)@$(SMSC_SIM_REF) …"; \
		tmp=$$(mktemp -d) && git clone --depth 1 --branch $(SMSC_SIM_REF) $(SMSC_SIM_REPO) $$tmp && \
		docker build -t $(SMSC_SIM_IMAGE) $$tmp && rm -rf $$tmp; \
	fi

.PHONY: proto
proto: ## Generate gRPC code from api/proto/*.proto (buf) — output committed under internal/.../pb
	buf generate

.PHONY: generate
generate: proto ## Run every code generator (sqlc, go:generate, buf)
	go generate ./...

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

## ---------------------------------------------------------------------------- load (M12)

# k6 is a native binary, deliberately outside go.mod (plan §1.3) — `make tools` cannot install it.
# Both targets fail loudly when it is missing rather than skipping: a load harness believed green
# because it never ran is worse than no harness (step-200 D7).
LOAD_PROFILE ?= smoke
# IDEMPOTENCY=on makes the run emit an Idempotency-Key, which switches the gateway to submitIdempotent
# and its two extra Redis round-trips (step-201 D10). Empty means off. It is forwarded explicitly so
# that `make load IDEMPOTENCY=on` works, and not only the exported form.
IDEMPOTENCY ?=

.PHONY: load
load: ## Run the REST load profile against BASE_URL: make load LOAD_PROFILE=smoke|sustained|peak BASE_URL=http://host:port [IDEMPOTENCY=on]
	@command -v k6 >/dev/null 2>&1 || { echo "k6 is not installed — see scripts/load-smoke.sh for install hints"; exit 2; }
	@if [ -z "$(BASE_URL)" ]; then echo "usage: make load BASE_URL=http://host:port [LOAD_PROFILE=smoke|sustained|peak] [IDEMPOTENCY=on]"; exit 2; fi
	PROFILE=$(LOAD_PROFILE) BASE_URL=$(BASE_URL) IDEMPOTENCY=$(IDEMPOTENCY) k6 run test/load/k6/messages.js

.PHONY: load-smoke
load-smoke: ## Prove the k6 thresholds are wired: the same script must pass idle AND fail against a slowed stub
	scripts/load-smoke.sh

.PHONY: load-binds
load-binds: ## Open N concurrent SMPP binds against a peer: make load-binds BINDS=50 ADDR=127.0.0.1:2775
	go run ./cmd/smpp-bindgen -binds $(or $(BINDS),50) -addr $(or $(ADDR),127.0.0.1:2775) -hold $(or $(HOLD),5s)

# The end-to-end budget (spec §1.2: submission -> SMSC delivery attempt, p99 < 2s) is read off the
# gateway's own message_e2e_duration_seconds. Record a baseline, run the load, then check what the run
# added — without the baseline the figure folds in every observation since the process started.
.PHONY: e2e-baseline e2e-check
e2e-baseline: ## Record the pre-run reading: make e2e-baseline [METRICS=http://host:9100] [BASELINE=/tmp/e2e.json]
	go run ./cmd/e2e-budget -metrics $(or $(METRICS),http://127.0.0.1:9100) -baseline $(or $(BASELINE),/tmp/e2e-baseline.json)

e2e-check: ## Score the run against the baseline: make e2e-check [BUDGET=2s] [QUANTILE=0.99]
	go run ./cmd/e2e-budget -metrics $(or $(METRICS),http://127.0.0.1:9100) -baseline $(or $(BASELINE),/tmp/e2e-baseline.json) \
		-check -budget $(or $(BUDGET),2s) -quantile $(or $(QUANTILE),0.99)

# The peer is NOT started here (step-201 D3): the tool takes an address and a metrics URL, so the same
# command works against a remote simulator for the full-scale campaign. Start it first — the docker run
# and the YAML are in the doc comment of cmd/smsc-ceiling.
.PHONY: smsc-ceiling
smsc-ceiling: ## Measure the test peer's submit_sm ceiling (sweeps binds, reads the peer's /metrics): make smsc-ceiling [BINDS=10,20,40,80] [MEASURE=60s]
	go run ./cmd/smsc-ceiling \
		-addr $(or $(ADDR),127.0.0.1:2775) -metrics $(or $(METRICS),http://127.0.0.1:9000) \
		$(if $(BINDS),-binds $(BINDS)) $(if $(REFERENCE),-reference $(REFERENCE)) $(if $(MEASURE),-measure $(MEASURE))

# The local reference run (step-201 D2). It stands the whole MT path up in ONE process against real
# Postgres/Kafka/ClickHouse (testcontainers) and the in-repo fake SMSC, holds a target rate for a full
# minute and scores the steady state. It lives behind the `loadref` build tag so `make test` never
# compiles it, let alone runs its two minutes.
.PHONY: load-reference
load-reference: ## Run the D2 steady-state reference run: make load-reference [RATE=1200] [BIND_POOL=4] [CH_MAX_OPEN=10] [MEASURE=60s]
	REF_RATE=$(RATE) REF_WORKERS=$(WORKERS) REF_MEASURE=$(MEASURE) REF_WARMUP=$(WARMUP) \
	REF_SETTLE=$(SETTLE) REF_BIND_POOL=$(BIND_POOL) REF_WINDOW=$(SMPP_WINDOW) \
	REF_CH_MAX_OPEN=$(CH_MAX_OPEN) REF_CH_MAX_IDLE=$(CH_MAX_IDLE) \
		go test -tags=loadref -count=1 -timeout 30m -v -run $(or $(RUN),TestReferenceRun) ./internal/e2e/

## ---------------------------------------------------------------------------- quality gates

.PHONY: test
test: ## Run the tests with the race detector — the bar for every PR
	go test -race ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: fmt
fmt: ## Format the tree
	golangci-lint fmt

.PHONY: vuln
vuln: ## Scan the dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: contracts
contracts: ## Refuse a contract change without the matching version bump in api/package.json
	scripts/check-contracts.sh

.PHONY: contracts-types
contracts-types: ## Generate the TypeScript types from the contracts and typecheck them (needs Node)
	cd api && npm ci && npm run build && npm run typecheck

# contracts-types is deliberately out: it would make Node a prerequisite of every `make check`, for a
# check that only matters when api/**.yaml moves. CI runs it on every PR.
.PHONY: check
check: lint test vuln contracts ## Everything CI checks, in one command
