from datetime import datetime
import logging
from types import MappingProxyType
from typing import List, Literal, Mapping, Optional, Tuple

from sqlalchemy import (
    and_,
    ColumnElement,
    Connection,
    DefaultClause,
    delete,
    exists,
    func,
    not_,
    select,
    Table,
)
from sqlalchemy.future import select as future_select
from sqlalchemy.sql.functions import coalesce

from .db import DbOps
from .table import TableOps

from ..config import DeltaConfig, TableConfig, TableColumnConfig
from ..utils.db_utils import is_text_col


LOGGER = logging.getLogger(__name__)


class DeltaTableOps:
    def __init__(
        self,
        table_schema: str,
        table_name: str,
        delta_config: DeltaConfig,
        connection: Connection,
    ):
        self.table_schema = table_schema
        self.table_name = table_name
        self.delta_config = delta_config
        self.connection = connection

    def table_config(self, column_definitions: List[TableColumnConfig]) -> TableConfig:
        return TableConfig(self.table_name, self.table_schema, column_definitions)

    def upsert(
        self,
        target_table: str,
        update_time: Optional[datetime] = None,
        is_main_table: bool = True,
        source_columns: Optional[List[str]] = None,
        src_tgt_colname_map: Mapping[str, str] = MappingProxyType({}),
    ) -> Tuple[int, int, int]:
        DbOps(self.connection).set_system_versioning_time(update_time)

        tgt_to_src_map = {v: k for k, v in src_tgt_colname_map.items()}

        if self.delta_config.row_finality == "dropout":
            deleted_keys = self._drop_missing_rows(
                self.table_schema, target_table, self.table_name, tgt_to_src_map
            )
            num_deletions = len(deleted_keys)
        else:
            num_deletions = 0

        tbo = TableOps(self.table_schema, self.table_name, self.connection)
        if source_columns is None:
            source_tbl = tbo.get_table_metadata()
            source_columns = [c.name for c in source_tbl.columns]

        if is_main_table and self.delta_config.drop_unchanged_rows:
            ref_columns = [src_tgt_colname_map.get(c, c) for c in source_columns]
            num_deletions += self._drop_unchanged_rows(
                self.table_schema,
                target_table=self.table_name,
                ref_table=target_table,
                ref_cmp_columns=ref_columns,
                ref_tgt_colname_map=tgt_to_src_map,
            )

        num_inserts, num_updates = self._table_upsert_nontemporal(
            self.table_schema,
            self.table_name,
            target_table,
            source_columns,
            src_tgt_colname_map,
            on_duplicate_key=self.delta_config.on_duplicate_key,
        )

        DbOps(self.connection).set_system_versioning_time(None)

        return num_inserts, num_updates, num_deletions

    def _table_upsert_nontemporal(
        self,
        table_schema: str,
        src_table: str,
        target_table: str,
        source_columns: Optional[List[str]] = None,
        src_tgt_colname_map: Mapping[str, str] = MappingProxyType({}),
        on_duplicate_key: Literal["error", "take_last", "take_first"] = "error",
    ) -> Tuple[int, int]:
        target_tbo = TableOps(table_schema, target_table, self.connection)
        target_tbl = target_tbo.get_table_metadata()
        src_tbo = TableOps(table_schema, src_table, self.connection)
        src_tbl = src_tbo.get_table_metadata()

        if source_columns is None:
            source_columns = [c.name for c in src_tbl.columns]

        _prevalidate_upsert_from_table(
            src_tbl,
            target_tbl,
            source_columns,
            src_tgt_colname_map,
            disable_check=False,
        )

        tgt_to_src_map = {v: k for k, v in src_tgt_colname_map.items()}

        if target_tbl.autoincrement_column is None:
            tgt_id_col = None
        else:
            tgt_id_col = target_tbl.autoincrement_column.name

        tgt_pk = target_tbo.get_primary_keys(target_tbl)

        update_set = {
            src_tgt_colname_map.get(sc_name, sc_name): self._coalesce_if_nullable(
                src_tbl,
                target_tbl,
                sc_name,
                src_tgt_colname_map.get(sc_name, sc_name),
            )
            for sc_name in source_columns
            if src_tgt_colname_map.get(sc_name, sc_name) != tgt_id_col
        }

        if len(update_set) == 0:
            num_updates = 0
        else:
            update_existing_keys = (
                target_tbl.update()
                .values(update_set)
                .where(
                    and_(
                        *[
                            tk == src_tbl.c[tgt_to_src_map.get(tk.name, tk.name)]
                            for tk in tgt_pk
                        ]
                    )
                )
            )

            result = DbOps(self.connection).execute_sqlalchemy(
                "sql.base.upsert.update", update_existing_keys
            )

            num_updates = result.rowcount

        primary_key_matches = (
            tk == src_tbl.c[tgt_to_src_map.get(tk.name, tk.name)] for tk in tgt_pk
        )

        subq = select(*tgt_pk).where(and_(*primary_key_matches))

        not_exists_clause: ColumnElement = not_(exists(subq))

        key_not_null = [
            src_tbl.c[tgt_to_src_map.get(tk.name, tk.name)].isnot(None) for tk in tgt_pk
        ]

        if on_duplicate_key == "error":
            insert_new_keys = target_tbl.insert().from_select(
                [src_tgt_colname_map.get(sc, sc) for sc in source_columns],
                future_select(*[src_tbl.c[sc] for sc in source_columns])
                .select_from(src_tbl)
                .where(and_(not_exists_clause, *key_not_null))
                .distinct(),
            )
        else:
            partition_keys = [tgt_to_src_map.get(tk.name, tk.name) for tk in tgt_pk]

            src_cols = [src_tbl.c[sk] for sk in source_columns]

            rownums_foreach_target_tbl_key = [
                func.row_number()
                .over(partition_by=partition_keys, order_by=partition_keys)
                .label("rn")
            ]

            src_selection = src_cols + rownums_foreach_target_tbl_key

            new_items = (
                select(*src_selection)
                .distinct()
                .where(and_(not_exists_clause, *key_not_null))
                .subquery()
                .select()
                .cte("new_items")
            )

            tgt_keys_in_src_tbl = [tgt_to_src_map.get(tk, tk) for tk in partition_keys]

            if on_duplicate_key == "take_last":
                selected_row_number = func.max(new_items.c["rn"]).label("selected_rn")
            else:
                selected_row_number = func.min(new_items.c["rn"]).label("selected_rn")

            selected_rns = (
                select(
                    *[new_items.c[k] for k in tgt_keys_in_src_tbl], selected_row_number
                )
                .group_by(*tgt_keys_in_src_tbl)
                .cte("selected_rns_by_key")
            )

            unique_items = (
                future_select(*[new_items.c[sc] for sc in source_columns])
                .select_from(new_items)
                .where(
                    and_(
                        new_items.c["rn"] == selected_rns.c["selected_rn"],
                        *[
                            new_items.c[k] == selected_rns.c[k]
                            for k in tgt_keys_in_src_tbl
                        ],
                    )
                )
                .cte()
            )

            insert_new_keys = target_tbl.insert().from_select(
                [src_tgt_colname_map.get(sc, sc) for sc in source_columns],
                future_select(unique_items),
            )

        # debug_result = pl.read_database(f"select * from {table_schema}.{src_table}", connection)

        try:
            result = DbOps(self.connection).execute_sqlalchemy(
                "sql.base.upsert.insert",
                insert_new_keys,
                # disable_foreign_key_checks=True,
                # disable_keys=f"{table_schema}.{target_table}",
            )
        except Exception as e:
            LOGGER.error(e)
            raise e
        num_inserts = result.rowcount

        if num_inserts > 0 or num_updates > 0:
            LOGGER.debug(
                "Table[%s.%s]: inserted %d, updated %d",
                table_schema,
                target_table,
                num_inserts,
                num_updates,
            )

        return num_inserts, num_updates

    def _coalesce_if_nullable(
        self, src_tbl: Table, target_tbl: Table, src_col_name: str, target_col_name: str
    ):
        src_col = src_tbl.c[src_col_name]
        target_col = target_tbl.c[target_col_name]

        if target_col.nullable:
            return src_col

        if (
            target_col.server_default is not None
            and target_col.server_default.has_argument
        ):
            assert isinstance(target_col.server_default, DefaultClause)
            default_value = target_col.server_default.arg
            return coalesce(src_col, default_value)

        return coalesce(src_col, target_col)

    def _drop_unchanged_rows(
        self,
        table_schema: str,
        target_table: str,
        ref_table: str,
        ref_cmp_columns: Optional[List[str]] = None,
        ref_tgt_colname_map: Mapping[str, str] = MappingProxyType({}),
    ) -> int:
        target_tbo = TableOps(table_schema, target_table, self.connection)
        ref_tbo = TableOps(table_schema, ref_table, self.connection)
        target_tbl = target_tbo.get_table_metadata()
        ref_tbl = ref_tbo.get_table_metadata()

        tgt_ref_colname_map = {v: k for k, v in ref_tgt_colname_map.items()}

        # Determine columns to compare if not provided
        if ref_cmp_columns is None:
            target_cols = {
                tgt_ref_colname_map.get(c.name, c.name) for c in target_tbl.columns
            }
            ref_cols = {c.name for c in ref_tbl.columns}
            ref_cmp_columns = sorted(target_cols.intersection(ref_cols))

        identical_rows = (
            select(
                *[
                    target_tbl.c[ref_tgt_colname_map.get(col_name, col_name)]
                    for col_name in ref_cmp_columns
                ]
            )
            .intersect(
                select(
                    *[
                        ref_tbl.c[col_name].label(
                            ref_tgt_colname_map.get(col_name, col_name)
                        )
                        for col_name in ref_cmp_columns
                    ]
                )
            )
            .subquery()
        )

        tgt_primary_keys = [
            ref_tgt_colname_map.get(c.name, c.name)
            for c in ref_tbl.primary_key.columns
            if c.name in ref_cmp_columns
        ]

        if not tgt_primary_keys:
            raise ValueError(
                f"no target primary key found in intersect({target_table}, {ref_table})"
            )

        delete_sql = delete(target_tbl).where(
            and_(*[target_tbl.c[k] == identical_rows.c[k] for k in tgt_primary_keys])
        )

        result = DbOps(self.connection).execute_sqlalchemy(
            "sql.delta.drop_unchanged_rows", delete_sql
        )

        num_deletes = result.rowcount

        LOGGER.debug(
            "removed %d unchanged rows from %s.%s",
            num_deletes,
            table_schema,
            target_table,
        )

        return num_deletes

    def _drop_missing_rows(
        self,
        table_schema: str,
        deletion_table: str,
        ref_table: str,
        del_ref_colname_map: Mapping[str, str],
    ):
        deletion_tbo = TableOps(table_schema, deletion_table, self.connection)
        ref_tbo = TableOps(table_schema, ref_table, self.connection)
        deletion_tbl = deletion_tbo.get_table_metadata()
        ref_tbl = ref_tbo.get_table_metadata()

        deletion_pks = deletion_tbo.get_primary_keys(deletion_tbl)

        subq = select(*deletion_pks).where(
            and_(
                *[
                    deletion_tbl.c[k.name]
                    == ref_tbl.c[del_ref_colname_map.get(k.name, k.name)]
                    for k in deletion_pks
                ]
            )
        )

        select_sql = select(*deletion_pks).where(~subq.exists())
        delete_sql = delete(deletion_tbl).where(~subq.exists())

        pkeys_to_delete = (
            DbOps(self.connection)
            .execute_sqlalchemy("delta.drop_missing_rows.pkeys_to_delete", select_sql)
            .fetchall()
        )
        result = DbOps(self.connection).execute_sqlalchemy(
            "sql.delta.drop_missing_rows", delete_sql
        )

        if len(pkeys_to_delete) != result.rowcount:
            raise ValueError("mismatch on deleted row count")

        LOGGER.debug("deleted %d dropped-out rows", result.rowcount)
        return pkeys_to_delete


def _prevalidate_upsert_from_table(
    src_tbl: Table,
    target_tbl: Table,
    candidate_src_cols: List[str],
    src_tgt_colname_map: Mapping[str, str],
    disable_check: bool,
):
    if disable_check:
        return

    check_failed = False

    for src_col_name in candidate_src_cols:
        src_col = src_tbl.c[src_col_name]
        tgt_col_name = src_tgt_colname_map.get(src_col_name, src_col_name)
        tgt_col = target_tbl.c[tgt_col_name]

        mismatch_type = tgt_col.type.__visit_name__ != src_col.type.__visit_name__
        if mismatch_type:
            LOGGER.error(
                "mismatch type on %s.%s=%s vs %s.%s=%s",
                src_tbl.name,
                src_col_name,
                src_col.type.__visit_name__,
                target_tbl.name,
                tgt_col_name,
                tgt_col.type.__visit_name__,
            )

            check_failed = True
            continue

        if is_text_col(str(src_col.type)):
            tgt_col_length = tgt_col.type.length  # type: ignore[attr-defined]
            src_col_length = src_col.type.length  # type: ignore[attr-defined]
            if tgt_col_length != src_col_length:
                LOGGER.error(
                    "mismatch length on  %s.%s=%s vs %s.%s=%s",
                    src_tbl.name,
                    src_col_name,
                    src_col_length,
                    target_tbl.name,
                    tgt_col_name,
                    tgt_col_length,
                )

                check_failed = True
                continue

        if check_failed:
            raise ValueError("column type mismatches. check logs")
