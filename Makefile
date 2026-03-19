NATS_PORT ?= 14222
MARIADB_PORT ?= 13307
VERSION := $(shell grep '^version' pyproject.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
PROJECT_ROOT := docs

test: test-up run-tests test-down

test-up:
	MARIADB_PORT=$(MARIADB_PORT) docker compose -f docker-compose.yaml up -d

test-down:
	MARIADB_PORT=$(MARIADB_PORT) docker compose -f docker-compose.yaml down

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

render:
	@echo "Rendering Quarto project in $(PROJECT_ROOT)/..."
	uv run quarto render "$(PROJECT_ROOT)" --no-execute

preview: render
	@echo "Starting Quarto preview for $(PROJECT_ROOT)/..."
	uv run quarto preview "$(PROJECT_ROOT)"

clean:
	rm -rf dist htmlcov .coverage .mypy_cache .pytest_cache .ruff_cache
	rm -rf "$(PROJECT_ROOT)/_site" "$(PROJECT_ROOT)/.quarto"
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name '*.egg-info' -exec rm -rf {} +

bump:
	@if [ -z "$(PART)" ]; then echo "Usage: make bump PART=major|minor|patch"; exit 1; fi
	@IFS='.' read -r major minor patch <<< "$(VERSION)"; \
	case "$(PART)" in \
		major) major=$$((major + 1)); minor=0; patch=0;; \
		minor) minor=$$((minor + 1)); patch=0;; \
		patch) patch=$$((patch + 1));; \
		*) echo "PART must be major, minor, or patch"; exit 1;; \
	esac; \
	new_version="$$major.$$minor.$$patch"; \
	sed -i '' "s/^version = \"$(VERSION)\"/version = \"$$new_version\"/" pyproject.toml; \
	echo "Bumped version: $(VERSION) -> $$new_version"; \
	uv sync

release: check test
	@echo "Creating release v$(VERSION)..."
	git tag "v$(VERSION)"
	git push origin "v$(VERSION)"
	gh release create "v$(VERSION)" --generate-notes
