.PHONY: lint build test

lint:
	golangci-lint run ./...

build:
	go build ./main.go

test:
	go test -v -cover ./...

# Check goreleaser
.PHONY: snapshot
snapshot:
	goreleaser release --skip sign --skip publish --snapshot --clean

### INTEGRATION TESTS ###

# Setup docker-compose (for running integration tests in a sandbox)
.PHONY: compose-setup
compose-setup:
	docker compose -f integration/docker-compose.yml up --build -d

# Cleanup docker-compose
.PHONY: compose-teardown
compose-teardown:
	docker compose -f integration/docker-compose.yml down

# Run integration tests
.PHONY: test-integration
test-integration: compose-setup
	PGDUMP_EACH_INTEGRATION_TESTS_AVAILABLE=0xcafebabe go test -v ./...
	$(MAKE) compose-teardown
