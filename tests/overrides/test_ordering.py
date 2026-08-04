from datetime import UTC, datetime, timezone

from polars_hist_db.overrides import (
    OverrideOperation,
    OverrideTypedValue,
    override_recorded_order_sql,
    project_personal_override_operations,
)


def test_xtdb_order_preserves_close_before_set_within_one_transaction() -> None:
    sql = override_recorded_order_sql("xtdb")

    assert sql.startswith("_system_from, CASE")
    assert "recorded_at" not in sql
    assert "valid_from" not in sql
    assert "THEN 0 ELSE 1 END, operation_id" in sql


def test_personal_projection_preserves_authoritative_store_order() -> None:
    at = datetime(2026, 7, 17, tzinfo=UTC)
    close = OverrideOperation(
        operation_id="op-z-close",
        change_set_id="change-1",
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="feed-1",
        entity_id="entity-1",
        field_path="classification",
        observed_canonical_value_json=None,
        created_against_stale_source=False,
        valid_from=at,
        recorded_at=at,
        operation_type="close",
        value=None,
        valid_to=at,
    )
    replacement = OverrideOperation(
        operation_id="op-a-set",
        change_set_id="change-1",
        owner_user_id="user-1",
        actor_user_id="user-1",
        feed_id="feed-1",
        entity_id="entity-1",
        field_path="classification",
        operation_type="set",
        value=OverrideTypedValue("enum", {"value": "Possible"}),
        observed_canonical_value_json=None,
        created_against_stale_source=False,
        valid_from=at,
        valid_to=None,
        recorded_at=at,
    )

    projected = project_personal_override_operations([close, replacement])

    assert projected[-1].operation_id == replacement.operation_id
    assert projected[-1].valid_to is None
