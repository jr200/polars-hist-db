# polars-hist-db

A Python library for building bitemporal data pipelines with [Polars](https://pola.rs/) and [MariaDB](https://mariadb.com/).

It ingests data from DSV files or [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) subjects, tracks history using MariaDB's system-versioned tables, and exposes everything as strongly-typed Polars DataFrames.

### Features

- **Typed uploads** — push Polars DataFrames into MariaDB with automatic type mapping between Polars, SQL, and SQLAlchemy.
- **Typed queries** — read tables back into DataFrames with column types inferred from the database schema. Temporal query hints (`asof`, `span`, `all`) let you slice history without writing SQL.
- **YAML-driven pipelines** — define scrape specifications that handle column typing, enrichment via custom transform functions, normalization across tables, and foreign-key deduction.
- **Deduplication** — an audit log tracks what has already been ingested so re-runs skip previously processed files or messages.
- **Transactional ingestion** — each file or message is processed in its own transaction; failures roll back cleanly without affecting other items.
- **Dual input sources** — crawl directories for DSV files, or consume messages from NATS JetStream with configurable fetch/subscription strategies.
- **Delta upserts** — staging tables detect changed rows, handle duplicates (`take_first`, `take_last`, `error`), and optionally mark disappeared rows as dropped.

Full documentation: [jr200.github.io/polars-hist-db](https://jr200.github.io/polars-hist-db)

## Quick Start

```bash
uv sync
make test
```

## Development

```bash
make check       # ruff + mypy
make docs        # render and preview quarto docs
make bump PART=patch
make release
```

## License

This project is licensed under the [MIT](LICENSE) license.
