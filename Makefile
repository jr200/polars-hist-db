NATS_PORT ?= 14222
MARIADB_PORT ?= 13307

test: test-up run-tests test-down

test-up:
	MARIADB_PORT=$(MARIADB_PORT) docker compose -f infra/compose-mariadb.yml up -d

test-down:
	MARIADB_PORT=$(MARIADB_PORT) docker compose -f infra/compose-mariadb.yml down

cov:
	NATS_PORT=$(NATS_PORT) MARIADB_PORT=$(MARIADB_PORT) uv run coverage run -m pytest
	uv run coverage report -m
	uv run coverage html

check:
	uv run ruff check --output-format=github --fix .
	uv run ruff format
	uv run mypy .

run-tests:
	NATS_PORT=$(NATS_PORT) MARIADB_PORT=$(MARIADB_PORT) uv run pytest tests/
