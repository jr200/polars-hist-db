# polars-hist-db

This library is for scraping data from CSV style files, temporally, into MariaDB.

Main features are:
- Uploading data from strongly-typed Polars DataFrames.
- Querying data into Polars DataFrames, with column types inferred from the database schema.
- A scrape specification that:
    - Defines pipelines for typing, enriching, and normalizing data before uploading.
    - Allows construction of the 'as-of' time from file attributes or as a function of the input columns.
    - Catalogs the history of scrape inputs to prevent duplication.
    - Supports per-file transactional scraping (either the processing for a file succeeds, or the transaction is rolled back).

## Development Setup

1. Install NATS server
```bash
brew install nats-server
```

1. Create a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install development dependencies:
```bash
poetry install --with dev
```

3. Run tests:
```bash
poetry run pytest
```

4. Make docs. The documentation will be generated in the ``docs/_build/html`` directory:
```bash
cd docs && poetry run make html
```

## Code Style

This project follows the following code style guidelines:

* Use type hints for all function parameters and return values
* Follow PEP 8 style guide
* Use Google-style docstrings
* Keep functions focused and single-purpose
* Write comprehensive tests for new features

Run ``make check`` to check the code style.


## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the terms specified in the LICENSE file.

## References

- [Polars Documentation](https://docs.pola.rs/api/python/stable/reference/index.html)
- [SQLAlchemy Core Documentation](https://docs.sqlalchemy.org/en/20/core/index.html)
- [MariaDB Bitemporal Tables](https://mariadb.com/kb/en/bitemporal-tables)

