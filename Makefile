MODULES := . redisengine
GOLANGCI_LINT_VERSION := v2.14.0
GOVULNCHECK_VERSION := v1.8.0
REDIS_PORT ?= 6379

.PHONY: all test alloc bench cover fuzz lint vuln tidy integration

all: tidy lint test

## test: race-enabled, shuffled tests for every module
test:
	@for m in $(MODULES); do (cd $$m && go test -race -shuffle=on -count=1 ./...) || exit 1; done

## alloc: allocation budgets (excluded from race builds)
alloc:
	go test -count=1 -run 'Alloc|Budget' ./...

## bench: benchmarks with allocation counts
bench:
	go test -run '^$$' -bench . -benchmem ./...

## cover: coverage report for the core module
cover:
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

## fuzz: fuzz the engine value parser for 30s
fuzz:
	go test -run '^$$' -fuzz FuzzParseWindowEnd -fuzztime 30s ./throttle

## lint: go vet + golangci-lint for every module
lint:
	@for m in $(MODULES); do (cd $$m && go vet ./... && \
		go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run --config $(CURDIR)/.golangci.yaml ./...) || exit 1; done

## vuln: govulncheck for every module
vuln:
	@for m in $(MODULES); do (cd $$m && go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...) || exit 1; done

## tidy: go mod tidy for every module
tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done

## integration: run redisengine against a throwaway Redis in Docker
integration:
	docker run -d --rm --name throttle-go-redis -p $(REDIS_PORT):6379 redis:7-alpine
	cd redisengine && REDIS_ADDR=localhost:$(REDIS_PORT) go test -race -count=1 -run Integration -v ./...; \
		status=$$?; docker stop throttle-go-redis; exit $$status
