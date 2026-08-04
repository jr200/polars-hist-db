from typing import ClassVar

from polars_hist_db.core.audit import AuditOps


class _XtdbConnection:
    class _Dialect:
        name = "postgresql"

    dialect = _Dialect()


class _MariaDialect:
    name = "mariadb"


class _MariaConnection:
    dialect = _MariaDialect()

    def __init__(self):
        self.text_execs = []

    def execute(self, clause):
        self.text_execs.append(str(clause))


class _FakeAuditTable:
    c: ClassVar = {"table_name": None}


class _Stmt:
    def where(self, *args, **kwargs):
        return self


def _patch_orm_layer(monkeypatch, *, is_temporal):
    """Stub TableOps / DbOps / delete so the SQLAlchemy branch is
    exercised without touching a real MariaDB."""
    from polars_hist_db.core import audit as audit_module

    class _TableOps:
        def __init__(self, schema, name, connection):
            self.name = name

        def get_table_metadata(self):
            return _FakeAuditTable()

        def is_temporal_table(self):
            return bool(is_temporal)

    orm_deletes = []

    class _DbOps:
        def __init__(self, connection):
            pass

        def execute_sqlalchemy(self, label, stmt):
            orm_deletes.append(label)

    monkeypatch.setattr(audit_module, "TableOps", _TableOps)
    monkeypatch.setattr(audit_module, "DbOps", _DbOps)
    monkeypatch.setattr(audit_module, "delete", lambda tbl: _Stmt())
    return orm_deletes


def test_xtdb_reset_dataset_erases_target_and_audit(monkeypatch):
    executed = []

    class _TableConfigOps:
        def __init__(self, connection):
            pass

        def table_exists(self, table_schema, table_name):
            return True

        def from_table(self, table_schema, table_name):
            return AuditOps(table_schema)._table_config(xtdb=True)

        def create(self, table_config):
            return table_config

    monkeypatch.setattr(
        "polars_hist_db.core.audit._xtdb_table_config_ops",
        _TableConfigOps,
    )
    monkeypatch.setattr(
        "polars_hist_db.backends.xtdb_transport._execute_xtdb_transaction",
        lambda connection, statements: executed.append(list(statements)),
    )

    AuditOps("fakedata").reset_dataset("records", _XtdbConnection())

    assert len(executed) == 1
    assert executed[0][0] == "ERASE FROM fakedata.records WHERE TRUE"
    assert "ERASE FROM fakedata.__audit_log" in executed[0][1]
    assert "WHERE table_name = 'records'" in executed[0][1]


def test_mariadb_reset_dataset_wipes_history_on_temporal_table(monkeypatch):
    """System-versioned target table needs both DELETE and DELETE HISTORY.
    Audit-log itself is non-temporal and only needs DELETE."""
    orm_deletes = _patch_orm_layer(monkeypatch, is_temporal=True)
    conn = _MariaConnection()

    AuditOps("fakedata").reset_dataset("records", conn)

    assert orm_deletes == [
        "sql.audit.reset_dataset.data",
        "sql.audit.reset_dataset.audit",
    ]
    assert conn.text_execs == ["DELETE HISTORY FROM fakedata.records"]


def test_mariadb_reset_dataset_skips_history_on_nontemporal(monkeypatch):
    """Non-temporal target: no DELETE HISTORY emitted."""
    _patch_orm_layer(monkeypatch, is_temporal=False)
    conn = _MariaConnection()

    AuditOps("fakedata").reset_dataset("stateless_flat", conn)

    assert conn.text_execs == []
