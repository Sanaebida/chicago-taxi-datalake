.DEFAULT_GOAL := help
COMPOSE := docker compose

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

init: ## Create .env from the template (if missing)
	@test -f .env || cp .env.example .env && echo ".env ready"

up: init ## Build and start the whole stack (MinIO, Postgres, Dagster, dashboard)
	$(COMPOSE) up -d --build
	@echo "Dagster  : http://localhost:3000"
	@echo "Dashboard: http://localhost:8501"
	@echo "MinIO    : http://localhost:9001 (minioadmin / minioadmin)"

run: ## Run the full pipeline end to end (bronze -> silver -> gold)
	$(COMPOSE) run --rm pipeline-runner

demo: init ## Offline demo: same pipeline on synthetic data (no internet needed)
	$(COMPOSE) run --rm -e SOURCE_MODE=sample -e INGEST_MAX_ROWS_PER_PARTITION=50000 pipeline-runner

logs: ## Follow Dagster logs
	$(COMPOSE) logs -f dagster-code dagster-daemon

test: ## Run unit + end-to-end tests inside the Dagster image (local filesystem)
	$(COMPOSE) run --rm --no-deps -e DATALAKE_ROOT=file:///tmp/lake -e SOURCE_MODE=sample \
		--entrypoint bash dagster-code -c "pip install -q pytest duckdb && pytest -q /opt/pipeline/tests"

down: ## Stop the stack (data is kept)
	$(COMPOSE) down

clean: ## Stop the stack and delete all data (volumes)
	$(COMPOSE) down -v --remove-orphans
