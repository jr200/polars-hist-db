# polars-db-engine

Reads and writes polars dataframes to MariaDB, while introducing/maintaining bitemporality. Uses SQLAlchemy (non-ORM) to interface with polars.

## MariaDB commands

```
SELECT * FROM information_schema.tables;
```

## Todo

- implement temporal overlays
- all config and inputs should be dataframe based

## References

- polars docs (python): https://docs.pola.rs/api/python/stable/reference/index.html
- mariadb perf: https://www.youtube.com/watch?v=zISiQifPNT8
- pmm: https://docs.percona.com/percona-monitoring-and-management/quickstart/index.html#connect-database
- explain: https://mariadb.com/kb/en/explain/

