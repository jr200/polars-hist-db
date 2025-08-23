
test-up:
	docker compose -f infra/compose-mariadb.yml up -d

test-down:
	docker compose -f infra/compose-mariadb.yml down

cov:
	coverage run -m pytest
	coverage report -m
	coverage html

check:
	ruff check --fix
	ruff format
	mypy .

test:
	pytest tests/