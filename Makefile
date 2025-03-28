
infra-up:
	podman compose -f infra/compose-mariadb.yml up -d

infra-down:
	podman compose -f infra/compose-mariadb.yml down

cov:
	coverage run -m pytest
	coverage report -m
	coverage html

check:
	ruff check --fix
	ruff format
	mypy .
