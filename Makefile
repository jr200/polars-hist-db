test: test-up run-tests test-down

test-up:
	docker compose -f infra/compose-mariadb.yml up -d

test-down:
	docker compose -f infra/compose-mariadb.yml down

cov:
	uv run coverage run -m pytest
	uv run coverage report -m
	uv run coverage html

check:
	uv run ruff check --output-format=github --fix .
	uv run ruff format
	uv run mypy .

run-tests:
	uv run pytest tests/
