from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from polars_hist_db.overrides import (
    CrdtDocumentStoreConfig,
    DocumentAccessStoreConfig,
    build_crdt_document_table_config,
    build_crdt_update_table_config,
    build_document_access_table_configs,
)


def test_crdt_document_tables_use_portable_base64_payload_columns():
    config = CrdtDocumentStoreConfig()
    documents = build_crdt_document_table_config(config)
    updates = build_crdt_update_table_config(config)

    assert documents.name == "crdt_documents"
    assert documents.primary_keys == ("document_id",)
    assert {column.name for column in documents.columns} == {
        "document_id",
        "revision",
        "generation",
        "head_state_vector_base64",
        "snapshot_update_base64",
        "snapshot_update_hash",
        "snapshot_state_vector_base64",
        "snapshot_through_revision",
        "updated_at",
    }
    assert {
        column.name for column in documents.columns if column.data_type == "MEDIUMTEXT"
    } == {
        "head_state_vector_base64",
        "snapshot_update_base64",
        "snapshot_state_vector_base64",
    }
    assert updates.primary_keys == ("document_id", "revision")
    assert "generation" in {column.name for column in updates.columns}
    source_hash_constraint = [
        column
        for column in updates.columns
        if "document_source_update_hash" in column.unique_constraint
    ]
    assert {column.name for column in source_hash_constraint} == {
        "document_id",
        "source_update_hash",
    }
    assert {
        column.name for column in updates.columns if column.data_type == "VARCHAR(64)"
    } == {"source_update_hash", "accepted_update_hash"}
    assert (
        next(
            column for column in updates.columns if column.name == "update_base64"
        ).data_type
        == "MEDIUMTEXT"
    )


def test_crdt_document_tables_compile_mediumtext_for_mariadb():
    config = CrdtDocumentStoreConfig()
    table_config = build_crdt_document_table_config(config)
    table = Table(
        table_config.name,
        MetaData(schema=table_config.schema),
        *table_config.build_sqlalchemy_columns(is_delta_table=False),
    )

    ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))

    assert "MEDIUMTEXT" in ddl


def test_document_names_are_unique_within_the_owning_group():
    documents, _, _ = build_document_access_table_configs(DocumentAccessStoreConfig())
    constrained = {
        column.name
        for column in documents.columns
        if "document_access_group_name" in column.unique_constraint
    }

    assert constrained == {"normalized_name", "owning_group"}
