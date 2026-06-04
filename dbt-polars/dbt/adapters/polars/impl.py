import os
import shutil
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple, Type

import polars as pl
from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.base.impl import PythonJobHelper
from dbt.adapters.base.meta import available
from dbt.adapters.base.column import Column
from dbt.adapters.base.relation import RelationType
from dbt.adapters.contracts.connection import AdapterResponse

from dbt.adapters.polars.connections import (
    DatabricksConnectionHandle,
    PolarsConnectionManager,
)

# Maps SQL type strings (from seeds `column_types` config) to Polars types.
_SQL_TO_POLARS: Dict[str, type] = {
    "int": pl.Int64,
    "integer": pl.Int64,
    "bigint": pl.Int64,
    "smallint": pl.Int32,
    "float": pl.Float64,
    "double": pl.Float64,
    "numeric": pl.Float64,
    "decimal": pl.Float64,
    "varchar": pl.Utf8,
    "string": pl.Utf8,
    "text": pl.Utf8,
    "char": pl.Utf8,
    "boolean": pl.Boolean,
    "bool": pl.Boolean,
    "date": pl.Date,
    "timestamp": pl.Datetime,
    "datetime": pl.Datetime,
}


def _agate_to_polars(agate_table, column_override: Dict[str, str]) -> pl.DataFrame:
    """Convert dbt's agate.Table (loaded from CSV) to a Polars DataFrame."""
    columns = {
        col: [row[i] for row in agate_table.rows]
        for i, col in enumerate(agate_table.column_names)
    }
    df = pl.DataFrame(columns, infer_schema_length=None)

    for col, type_str in (column_override or {}).items():
        base_type = type_str.lower().split("(")[0].strip()
        polars_type = _SQL_TO_POLARS.get(base_type)
        if polars_type and col in df.columns:
            df = df.with_columns(pl.col(col).cast(polars_type))

    return df


class PolarsAdapter(BaseAdapter):
    ConnectionManager = PolarsConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    # ------------------------------------------------------------------
    # Python model support
    # ------------------------------------------------------------------

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        from dbt.adapters.polars._python import PolarsPythonJobHelper
        return {"polars": PolarsPythonJobHelper}

    @property
    def default_python_submission_method(self) -> str:
        return "polars"

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        return AdapterResponse(_message="OK")

    # ------------------------------------------------------------------
    # Handle helpers
    # ------------------------------------------------------------------

    def _handle(self):
        return self.connections.get_thread_connection().handle

    def _is_databricks(self) -> bool:
        return isinstance(self._handle(), DatabricksConnectionHandle)

    def _uc(self) -> DatabricksConnectionHandle:
        return self._handle()

    # ------------------------------------------------------------------
    # Local helpers (only used when not Databricks)
    # ------------------------------------------------------------------

    def _catalog_path(self) -> str:
        return self.config.credentials.path

    def _table_path(self, schema: str, identifier: str) -> str:
        return os.path.join(self._catalog_path(), schema, identifier)

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def create_schema(self, relation: BaseRelation) -> None:
        if self._is_databricks():
            uc = self._uc()
            try:
                uc.client.create_schema(uc.catalog, relation.schema)
            except Exception:
                pass  # already exists
        else:
            os.makedirs(
                os.path.join(self._catalog_path(), relation.schema),
                exist_ok=True,
            )

    def drop_schema(self, relation: BaseRelation) -> None:
        if self._is_databricks():
            uc = self._uc()
            uc.client.drop_schema(uc.catalog, relation.schema)
        else:
            schema_dir = os.path.join(self._catalog_path(), relation.schema)
            if os.path.isdir(schema_dir):
                shutil.rmtree(schema_dir)

    def list_schemas(self, database: str) -> List[str]:
        if self._is_databricks():
            uc = self._uc()
            return uc.client.list_schemas(uc.catalog)
        root = self._catalog_path()
        if not os.path.isdir(root):
            return []
        return [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]

    # ------------------------------------------------------------------
    # Relation management
    # ------------------------------------------------------------------

    def list_relations_without_caching(
        self, schema_relation: BaseRelation
    ) -> List[BaseRelation]:
        if self._is_databricks():
            uc = self._uc()
            tables = uc.client.list_tables(uc.catalog, schema_relation.schema)
            return [
                self.Relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=t["name"],
                    type=RelationType.Table,
                )
                for t in tables
            ]
        schema_dir = os.path.join(self._catalog_path(), schema_relation.schema)
        if not os.path.isdir(schema_dir):
            return []
        return [
            self.Relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=name,
                type=RelationType.Table,
            )
            for name in os.listdir(schema_dir)
            if os.path.isdir(os.path.join(schema_dir, name))
        ]

    def drop_relation(self, relation: BaseRelation) -> None:
        if self._is_databricks():
            uc = self._uc()
            try:
                uc.client.drop_table(uc.catalog, relation.schema, relation.identifier)
            except Exception:
                pass
        else:
            path = self._table_path(relation.schema, relation.identifier)
            if os.path.isdir(path):
                shutil.rmtree(path)

    def rename_relation(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> None:
        if self._is_databricks():
            # UC rename only updates the table name in the metastore — the
            # storage_location stays pointing at the old path (e.g.
            # test_select__dbt_tmp).  On the next run dbt tries to create
            # test_select__dbt_tmp again, and UC rejects it as LOCATION_OVERLAP.
            #
            # Fix: copy the data to the canonical final path and re-register,
            # then drop the source entry.  The final table always lives at
            # {schema_root}/{final_name}, regardless of what name dbt used
            # for the temp relation.
            uc = self._uc()
            from_info = uc.client.get_table(
                uc.catalog, from_relation.schema, from_relation.identifier
            )
            read_creds = uc.client.vend_storage_credentials(
                from_info["table_id"], "READ"
            )
            df = pl.scan_delta(
                from_info["storage_location"], storage_options=read_creds
            ).collect()
            uc.client.write_and_register(
                df, uc.catalog, to_relation.schema, to_relation.identifier
            )
            uc.client.drop_table(
                uc.catalog, from_relation.schema, from_relation.identifier
            )
        else:
            shutil.move(
                self._table_path(from_relation.schema, from_relation.identifier),
                self._table_path(to_relation.schema, to_relation.identifier),
            )

    def truncate_relation(self, relation: BaseRelation) -> None:
        if self._is_databricks():
            uc = self._uc()
            try:
                info = uc.client.get_table(
                    uc.catalog, relation.schema, relation.identifier
                )
                table_id = info["table_id"]
                location = info["storage_location"]
                write_creds = uc.client.vend_storage_credentials(table_id, "READ_WRITE")
                schema = pl.scan_delta(location, storage_options=write_creds).schema
                pl.DataFrame(schema=schema).write_delta(
                    location, mode="overwrite", storage_options=write_creds
                )
            except Exception:
                pass
        else:
            path = self._table_path(relation.schema, relation.identifier)
            try:
                self._scan_delta_table(path).limit(0).collect().write_delta(
                    path, mode="overwrite"
                )
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Column operations
    # ------------------------------------------------------------------

    def get_columns_in_relation(self, relation: BaseRelation) -> List[Column]:
        if self._is_databricks():
            uc = self._uc()
            try:
                info = uc.client.get_table(
                    uc.catalog, relation.schema, relation.identifier
                )
                return [
                    Column(col["name"], col.get("type_text", "string"))
                    for col in info.get("columns", [])
                ]
            except Exception:
                return []
        path = self._table_path(relation.schema, relation.identifier)
        try:
            schema = self._scan_delta_table(path).schema
            return [Column(col, str(dtype)) for col, dtype in schema.items()]
        except Exception:
            return []

    def _get_one_catalog(
        self,
        information_schema: Any,
        schemas: Set[str],
        used_schemas: FrozenSet[Tuple[str, str]],
    ) -> "agate.Table":
        import agate

        column_names = [
            "table_database", "table_schema", "table_name", "table_type",
            "table_comment", "column_name", "column_index", "column_type",
            "column_comment",
        ]
        rows = []

        if self._is_databricks():
            uc = self._uc()
            for schema_name in schemas:
                for table in uc.client.list_tables(uc.catalog, schema_name):
                    name = table.get("name", "")
                    if not name:
                        continue
                    try:
                        info = uc.client.get_table(uc.catalog, schema_name, name)
                        for i, col in enumerate(info.get("columns", [])):
                            rows.append((
                                uc.catalog, schema_name, name, "table", None,
                                col["name"], i, col.get("type_text", "string"), None,
                            ))
                    except Exception:
                        pass
        else:
            database = self.config.credentials.database
            for schema_name in schemas:
                schema_dir = os.path.join(self._catalog_path(), schema_name)
                if not os.path.isdir(schema_dir):
                    continue
                for table_name in os.listdir(schema_dir):
                    table_dir = os.path.join(schema_dir, table_name)
                    if not os.path.isdir(table_dir):
                        continue
                    if not os.path.exists(os.path.join(table_dir, "_delta_log")):
                        continue
                    try:
                        table_schema = self._scan_delta_table(table_dir).collect_schema()
                        for i, (col_name, dtype) in enumerate(table_schema.items()):
                            rows.append((
                                database, schema_name, table_name, "table", None,
                                col_name, i, str(dtype), None,
                            ))
                    except Exception:
                        pass

        return agate.Table(rows, column_names=column_names)

    def expand_column_types_if_needed(
        self, goal: BaseRelation, current: BaseRelation
    ) -> None:
        pass  # Delta Lake handles schema evolution natively

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        pass

    @classmethod
    def quote(cls, identifier: str) -> str:
        return '"{}"'.format(identifier)

    @classmethod
    def convert_text_type(cls, agate_table, col_idx: int) -> str:
        return "text"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float8" if decimals else "integer"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx: int) -> str:
        return "boolean"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx: int) -> str:
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx: int) -> str:
        return "timestamp"

    # ------------------------------------------------------------------
    # dbt run: execute a compiled SQL model and write as a Delta table
    # ------------------------------------------------------------------

    def _scan_delta_table(self, table_dir: str) -> pl.LazyFrame:
        from dbt.adapters.polars._catalog import scan_delta_table
        return scan_delta_table(table_dir)

    @available
    def execute_select_as_delta(
        self,
        sql: str,
        database: str,
        schema: str,
        identifier: str,
    ) -> None:
        import re as _re
        import time as _time
        from dbt.adapters.polars._catalog import cte_names, strip_qualifiers

        t0 = _time.perf_counter()
        cleaned = strip_qualifiers(sql)
        exclude = cte_names(cleaned)
        needed = set(_re.findall(r'"(\w+)"', cleaned)) - exclude

        if self._is_databricks():
            from dbt.adapters.polars._databricks import build_uc_sql_context
            uc = self._uc()

            t1 = _time.perf_counter()
            ctx = build_uc_sql_context(uc.client, uc.catalog, schema, exclude=exclude, needed=needed)
            t2 = _time.perf_counter()

            result_df = ctx.execute(cleaned).collect()
            t3 = _time.perf_counter()

            uc.client.write_and_register(result_df, uc.catalog, schema, identifier)
            t4 = _time.perf_counter()

            import sys as _sys
            print(
                f"[timing] {identifier}: "
                f"build_context={t2-t1:.2f}s  "
                f"execute+collect={t3-t2:.2f}s  "
                f"write={t4-t3:.2f}s  "
                f"total={t4-t0:.2f}s  "
                f"(needed={sorted(needed)})",
                file=_sys.stderr,
            )
        else:
            from dbt.adapters.polars._catalog import build_sql_context
            build_sql_context(self._catalog_path(), exclude=exclude).execute(cleaned).collect(
            ).write_delta(self._table_path(schema, identifier), mode="overwrite",
                          delta_write_options={"schema_mode": "overwrite"})

    # ------------------------------------------------------------------
    # dbt seed: write an agate.Table (CSV) as a Delta table
    # ------------------------------------------------------------------

    @available
    def load_dataframe(
        self,
        database: Optional[str],
        schema: str,
        table_name: str,
        agate_table,
        column_override: Dict[str, str],
    ) -> None:
        df = _agate_to_polars(agate_table, column_override)

        if self._is_databricks():
            uc = self._uc()
            uc.client.write_and_register(df, uc.catalog, schema, table_name)
        else:
            path = self._table_path(schema, table_name)
            os.makedirs(os.path.join(self._catalog_path(), schema), exist_ok=True)
            df.write_delta(path, mode="overwrite",
                           delta_write_options={"schema_mode": "overwrite"})
