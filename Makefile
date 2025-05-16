.PHONY: build test run test-e2e 

BINARY_NAME=op-log-parser
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test


build:
	go build -o $(BINARY_NAME) .

test:
	go list ./... | grep op-log-parser/ | xargs go test

test-e2e:
	go test -v ./e2e_test.go

run: build
	./$(BINARY_NAME)


help:
	@echo "Available commands:"
	@echo "  make build        - Build the application"
	@echo "  make test         - Run unit tests"
	@echo "  make test-e2e     - Run end-to-end tests"
	@echo "  make run          - Build and run the application"
