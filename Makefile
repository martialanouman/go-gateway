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

.PHONY: proto
proto: ## Generate gRPC code from api/proto/*.proto (buf) — output committed under internal/.../pb
	buf generate

.PHONY: generate
generate: proto ## Run every code generator (sqlc, go:generate, buf)
	go generate ./...

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

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
