# nw-lb-go
#
# `make` with no arguments prints the target list. Every target below is
# self-documenting: the text after `##` on the target line is what help shows.

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

MODULE  := github.com/hitanshpaliwal/nw-lb-go
BIN     := bin
# Pid files and background logs from run-backends. Kept under bin/ so it lands
# inside the existing .gitignore entry instead of needing a new one.
RUN_DIR := bin/.run

# Stamped into main.version by the linker. Override in CI: make build VERSION=1.2.3
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOBUILD := go build -trimpath -ldflags="$(LDFLAGS)"

# config/lb.local.yaml points at 127.0.0.1:50051-3 (what run-backends starts).
# config/lb.yaml points at the compose service names and is what the lb
# container mounts.
LB_CONFIG     ?= config/lb.local.yaml
COVER_PROFILE ?= cover.out

# --- benchmark knobs (all overridable on the command line) -------------------
# make bench BENCH_RPS=8000 BENCH_DURATION=60s
BENCH_TARGET      ?= 127.0.0.1:8080
BENCH_BASELINE_TARGET ?= 127.0.0.1:50051
BENCH_MODE        ?= open
BENCH_RPS         ?= 2000
BENCH_DURATION    ?= 30s
# A request COUNT, not a duration: executed and discarded before measuring.
BENCH_WARMUP      ?= 2000
BENCH_CONCURRENCY ?= 64
BENCH_CONNS       ?= 4
BENCH_KEYS        ?= 1000
BENCH_PAYLOAD     ?= 256
BENCH_DELAY_MS    ?= 0
BENCH_METHOD      ?= unary
BENCH_SLO         ?= 200ms
BENCH_DIR         ?= bench/results
BENCH_LABEL       ?= lb-$(BENCH_MODE)-$(BENCH_RPS)rps
BENCH_OUT         ?= $(BENCH_DIR)/$(BENCH_LABEL).json
BENCH_BASELINE_LABEL ?= baseline-direct-$(BENCH_MODE)-$(BENCH_RPS)rps
BENCH_BASELINE_OUT   ?= $(BENCH_DIR)/$(BENCH_BASELINE_LABEL).json

.PHONY: help proto build test test-race lint vet cover run-backends stop-backends run-lb \
        bench bench-baseline docker-build up down logs clean viz viz-check viz-open viz-deploy

##@ Help

help: ## Print this help
	@awk 'BEGIN { FS = ":.*##"; printf "\nnw-lb-go — usage: make <target>\n" } \
	     /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next } \
	     /^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } \
	     END { printf "\nCommon overrides: VERSION, LB_CONFIG, BENCH_RPS, BENCH_DURATION, BENCH_TARGET\n\n" }' \
	     $(MAKEFILE_LIST)

##@ Build

proto: ## Regenerate gen/echo/v1 from proto/echo/v1/echo.proto (generated code is committed; only needed if the .proto changes)
	@command -v protoc >/dev/null 2>&1 || { echo "protoc not found: brew install protobuf"; exit 1; }
	@command -v protoc-gen-go >/dev/null 2>&1 || { echo "protoc-gen-go not found: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"; exit 1; }
	@command -v protoc-gen-go-grpc >/dev/null 2>&1 || { echo "protoc-gen-go-grpc not found: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"; exit 1; }
	protoc -I proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		proto/echo/v1/echo.proto

build: ## Build lb, backend and loadgen into ./bin
	@mkdir -p $(BIN)
	$(GOBUILD) -o $(BIN)/lb      ./cmd/lb
	$(GOBUILD) -o $(BIN)/backend ./cmd/backend
	$(GOBUILD) -o $(BIN)/loadgen ./cmd/loadgen
	@echo "built $(BIN)/{lb,backend,loadgen} version=$(VERSION)"

##@ Checks

test: ## Run the unit tests
	go test ./...

test-race: ## Run the unit tests under the race detector (no cache)
	go test -race -count=1 ./...

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint, or fall back to gofmt + go vet if it is not installed
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed (go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)"; \
		echo "falling back to gofmt + go vet"; \
		unformatted=$$(gofmt -l . | grep -v '^gen/' || true); \
		if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi; \
		go vet ./...; \
	fi

cover: ## Run tests with coverage and print the per-function summary
	go test -covermode=atomic -coverprofile=$(COVER_PROFILE) ./...
	go tool cover -func=$(COVER_PROFILE) | tail -n 30
	@echo "html report: go tool cover -html=$(COVER_PROFILE)"

##@ Run locally

run-backends: build ## Start three backends in the background on :50051/:50052/:50053 (admin :9101/:9102/:9103)
	@mkdir -p $(RUN_DIR)
	@if [ -s $(RUN_DIR)/backends.pid ]; then \
		echo "backends already running (pids: $$(tr '\n' ' ' < $(RUN_DIR)/backends.pid)); run 'make stop-backends' first"; exit 1; \
	fi
	@: > $(RUN_DIR)/backends.pid
	@i=1; for spec in "50051 9101 2ms" "50052 9102 8ms" "50053 9103 25ms"; do \
		set -- $$spec; \
		./$(BIN)/backend -listen ":$$1" -admin-listen ":$$2" -id "backend-$$i" -delay "$$3" \
			> $(RUN_DIR)/backend-$$i.log 2>&1 & \
		echo $$! >> $(RUN_DIR)/backends.pid; \
		echo "backend-$$i  grpc :$$1  admin :$$2  delay $$3  log $(RUN_DIR)/backend-$$i.log"; \
		i=$$((i+1)); \
	done
	@sleep 1
	@echo "check: curl -s localhost:9101/healthz ; stop: make stop-backends"

stop-backends: ## Stop the backends started by run-backends
	@if [ ! -s $(RUN_DIR)/backends.pid ]; then echo "no backends recorded in $(RUN_DIR)/backends.pid"; exit 0; fi
	@while read -r pid; do \
		if kill -TERM "$$pid" 2>/dev/null; then echo "stopped $$pid"; fi; \
	done < $(RUN_DIR)/backends.pid
	@rm -f $(RUN_DIR)/backends.pid

run-lb: build ## Run the load balancer in the foreground (gRPC :8080, admin :9090)
	@echo "config=$(LB_CONFIG)  grpc=:8080  admin=:9090 (/metrics /healthz /readyz /backends /debug/pprof)"
	./$(BIN)/lb -config $(LB_CONFIG)

##@ Benchmark

bench: build ## Load-test the LB and write a JSON report to bench/results (needs the LB + backends running)
	@mkdir -p $(BENCH_DIR)
	./$(BIN)/loadgen \
		-target $(BENCH_TARGET) \
		-mode $(BENCH_MODE) \
		-rps $(BENCH_RPS) \
		-duration $(BENCH_DURATION) \
		-warmup $(BENCH_WARMUP) \
		-concurrency $(BENCH_CONCURRENCY) \
		-conns $(BENCH_CONNS) \
		-keys $(BENCH_KEYS) \
		-payload $(BENCH_PAYLOAD) \
		-delay-ms $(BENCH_DELAY_MS) \
		-method $(BENCH_METHOD) \
		-slo $(BENCH_SLO) \
		-label $(BENCH_LABEL) \
		-out $(BENCH_OUT)
	@echo "report: $(BENCH_OUT)"

bench-baseline: build ## Same load straight at backend-1, bypassing the LB (see the note it prints before comparing)
	@mkdir -p $(BENCH_DIR)
	./$(BIN)/loadgen \
		-target $(BENCH_BASELINE_TARGET) \
		-mode $(BENCH_MODE) \
		-rps $(BENCH_RPS) \
		-duration $(BENCH_DURATION) \
		-warmup $(BENCH_WARMUP) \
		-concurrency $(BENCH_CONCURRENCY) \
		-conns $(BENCH_CONNS) \
		-keys 0 \
		-payload $(BENCH_PAYLOAD) \
		-delay-ms $(BENCH_DELAY_MS) \
		-method $(BENCH_METHOD) \
		-slo $(BENCH_SLO) \
		-label $(BENCH_BASELINE_LABEL) \
		-out $(BENCH_BASELINE_OUT)
	@echo "report: $(BENCH_BASELINE_OUT)"
	@echo "NOTE: run-backends injects 2ms/8ms/25ms so the demo shows a latency spread, and this"
	@echo "      baseline only hits backend-1 (2ms). For a like-for-like proxy-overhead number,"
	@echo "      restart the backends with identical -delay values first."

##@ Docker

# VERSION goes through the environment rather than --build-arg so it reaches
# both the build arg AND the `image:` tag, which compose interpolates from the
# same variable. Passing only --build-arg would stamp the binary while leaving
# every image tagged :dev.
docker-build: ## Build every image in docker-compose.yml
	VERSION=$(VERSION) docker compose --profile bench build

up: ## Build and start the whole stack (backends, lb, prometheus, grafana)
	VERSION=$(VERSION) docker compose up -d --build
	@echo "lb        grpc  localhost:8080"
	@echo "lb        admin http://localhost:9090/metrics"
	@echo "prometheus      http://localhost:9091"
	@echo "grafana         http://localhost:3000  (dashboard: nw-lb-go / nw-lb-go RED)"
	@echo "bench           docker compose --profile bench run --rm loadgen"

down: ## Stop the stack and remove its volumes
	docker compose --profile bench down --volumes --remove-orphans

logs: ## Follow the stack's logs
	docker compose logs -f --tail=100

##@ Housekeeping

clean: ## Remove build output, coverage, local run state and benchmark reports
	rm -rf $(BIN)
	rm -f $(COVER_PROFILE)
	rm -f $(BENCH_DIR)/*.json $(BENCH_DIR)/*.md
	go clean -testcache

##@ Visualisation

viz: ## Inline the diagram sources into the single-file docs/viz/index.html
	python3 scripts/build-viz.py

viz-check: ## Build the page, then prove in headless Chrome that every figure mounts
	./scripts/check-viz.sh

viz-open: viz ## Build and open the page in the default browser
	open docs/viz/index.html

viz-deploy: viz ## Build and deploy the page to Vercel production (needs `vercel login` once)
	vercel deploy --prod
