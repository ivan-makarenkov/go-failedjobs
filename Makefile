.PHONY: test lint

.DEFAULT_GOAL := help

.PHONY: help
help: ## Available commands
	@clear
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[0;33m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""

##@ Development

# Run tests
test: ## Run tests with race detection and coverage report generation
	@echo "Running tests..."
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out

# Run linter
lint: ## Run golangci-lint
	@echo "Running linter..."
	@golangci-lint run ./... --config .golangci.yml

# Run tests and linter
all: test lint ## Run all checks (tests and linter)

# Clean up
clean: ## Clean temporary files and directories
	@echo "Cleaning up..."
	@rm -f coverage.out

##@ Aliases
t: test ## Run tests
l: lint ## Run linter
a: all ## Run all checks (tests and linter)
c: clean ## Clean temporary files and directories

