# =============================================================================
# fluxio — Development Task Runner
# =============================================================================
# Also see cargo aliases in .cargo/config.toml (c, br, t, cl, fc, prepr).
#
# Common:
#   make help      show available targets
#   make db-up     start dev infra (none configured yet — no-op until backends land)
#   make dev       up services (if any) + run the app
#   make prepr     full pre-PR check (fmt + clippy + test)

.DEFAULT_GOAL := help
.PHONY: help deps build run dev test fmt fc lint check clean db-up db-down db-reset deny audit prepr flamegraph install uninstall

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

deps: ## Fetch dependencies
	cargo fetch

build: ## Build debug binary
	cargo build

run: ## Run the application
	cargo run

dev: db-up ## Start dev databases then run the app
	@echo "Services up. Starting app..."
	cargo run

test: ## Run all tests
	cargo test

fmt: ## Format code in place
	cargo fmt

fc: ## Check formatting without changing files
	cargo fmt -- --check

lint: ## Run clippy (deny warnings)
	cargo clippy --all-targets --all-features -- -D warnings

check: ## Fast type-check (no codegen)
	cargo check

clean: ## Remove build artifacts
	cargo clean

db-up: ## Start dev infra (no-op until transport/storage backends are configured)
	@docker compose up -d 2>/dev/null || echo "No dev services defined yet (see docker-compose.yml)."

db-down: ## Stop dev infra
	@docker compose down 2>/dev/null || true

db-reset: ## Stop dev infra AND delete data volumes
	@docker compose down -v 2>/dev/null || true

deny: ## Run cargo-deny (licenses, advisories, bans)
	cargo deny check

audit: ## Run security advisory audit
	cargo audit

prepr: ## Full pre-PR check (fmt + clippy + test)
	cargo fmt -- --check
	cargo clippy --all-targets --all-features -- -D warnings
	cargo test

flamegraph: ## Generate flamegraph of server under bench load (needs perf + inferno)
	./scripts/flamegraph.sh

install: ## System install (systemd service + /etc/vireon + /var/lib/vireon)
	sudo ./scripts/install.sh

uninstall: ## Remove vireon system install (prompts before deleting data)
	sudo ./scripts/install.sh --uninstall
