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

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

## ---------------------------------------------------------------------------- tooling

.PHONY: tools
tools: ## Install the Go binaries the workflow needs (sqlc, govulncheck, golangci-lint)
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)

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
migrate: ## Apply the pending migrations (make migrate CMD=down to reverse them)
	go run ./cmd/migrate -dir $(MIGRATIONS) $(or $(CMD),up)

## ---------------------------------------------------------------------------- build & run

.PHONY: build
build: ## Compile every cmd/
	go build ./...

.PHONY: run
run: ## Run a service: make run SVC=router-svc
	@if [ -z "$(SVC)" ]; then echo "usage: make run SVC=router-svc"; exit 2; fi
	go run ./cmd/$(SVC)

.PHONY: generate
generate: ## Run every code generator (sqlc, go:generate)
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

.PHONY: check
check: lint test vuln ## Everything CI checks, in one command
