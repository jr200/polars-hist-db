# polars-db-engine

A Python library that provides seamless integration between Polars DataFrames and MariaDB, with built-in support for bitemporal data management. This library uses SQLAlchemy (non-ORM) to interface with Polars and leverages MariaDB's system versioning capabilities.


## Development Setup

1. Create a virtual environment:
```bash
poetry env use python3.12
```

2. Install development dependencies:
```bash
poetry install --with dev
```

3. Run tests:
```bash
poetry run pytest
```

## MariaDB System Versioning

This library leverages MariaDB's system versioning capabilities for bitemporal data management. Tables are automatically created with:

- `__valid_from` and `__valid_to` timestamp columns
- System versioning enabled
- Partitioning by system time

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
- [MariaDB Performance Optimization](https://www.youtube.com/watch?v=zISiQifPNT8)
- [MariaDB EXPLAIN Documentation](https://mariadb.com/kb/en/explain/)

