.PHONY: lint build test

lint:
	golangci-lint run ./...

build:
	go build ./main.go

test:
	go test -v -cover ./...
