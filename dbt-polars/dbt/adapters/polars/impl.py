from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from dbt.adapters.base.column import Column
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.contracts.relation import RelationConfig, RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs import CATALOG_REGISTRY, BaseCatalog
from dbt.adapters.polars.connections import PolarsConnectionManager, PolarsCredentials
from dbt.adapters.polars.relation import PolarsRelation
from dbt.adapters.polars.sql_rewrite import parse_and_rewrite
from dbt_common.clients.agate_helper import (
    Integer as DbtInteger,
)
from dbt_common.clients.agate_helper import (
    Number,
    empty_table,
    table_from_data,
)
from dbt_common.exceptions import CompilationError, DbtRuntimeError
from dbt.artifacts.resources.v1.snapshot import SnapshotMetaColumnNames

import polars as pl

if TYPE_CHECKING:
    import agate

_POLARS_TYPE_MAP: dict[str, type[pl.DataType]] = {
    # Polars-native names
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "UInt8": pl.UInt8,
    "UInt16": pl.UInt16,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Boolean": pl.Boolean,
    "Utf8": pl.Utf8,
    "String": pl.Utf8,
    "Categorical": pl.Categorical,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
    "Time": pl.Time,
    "Duration": pl.Duration,
    "Decimal": pl.Decimal,
    "Binary": pl.Binary,
    # SQL aliases (lowercase)
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "tinyint": pl.Int8,
    "smallint": pl.Int16,
    "integer": pl.Int32,
    "int": pl.Int32,
    "int4": pl.Int32,
    "int2": pl.Int16,
    "bigint": pl.Int64,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "float4": pl.Float32,
    "real": pl.Float32,
    "float32": pl.Float32,
    "float8": pl.Float64,
    "double": pl.Float64,
    "float64": pl.Float64,
    "float": pl.Float64,
    "double precision": pl.Float64,
    "varchar": pl.Utf8,
    "text": pl.Utf8,
    "string": pl.Utf8,
    "char": pl.Utf8,
    "character varying": pl.Utf8,
    "utf8": pl.Utf8,
    "categorical": pl.Categorical,
    "boolean": pl.Boolean,
    "bool": pl.Boolean,
    "date": pl.Date,
    "timestamp": pl.Datetime,
    "datetime": pl.Datetime,
    "time": pl.Time,
    "duration": pl.Duration,
    "decimal": pl.Decimal,
    "numeric": pl.Decimal,
    "binary": pl.Binary,
}


def _resolve_polars_type(type_str: str) -> type[pl.DataType]:
    key = type_str.split("(")[0].strip()
    dtype = _POLARS_TYPE_MAP.get(key) or _POLARS_TYPE_MAP.get(key.lower())
    if dtype is None:
        raise DbtRuntimeError(f"Unknown column type: '{type_str}'")
    return dtype


def _normalize_partition_by(partition_by: str | list[str] | None) -> list[str]:
    if partition_by is None:
        return []
    return [partition_by] if isinstance(partition_by, str) else list(partition_by)


logger = AdapterLogger("polars")


def _key_part(df: pl.DataFrame, k: str) -> pl.Expr:
    """Column reference if k is in df, sql_expr otherwise; always cast to String."""
    return (
        pl.col(k).cast(pl.String).fill_null("")
        if k in df.columns
        else pl.sql_expr(k).cast(pl.String).fill_null("")
    )


def _resolve_join_keys(
    df: pl.DataFrame, unique_key: str | list[str]
) -> tuple[pl.DataFrame, list[str]]:
    """Materialise any SQL expression keys as derived columns; return (df, col_names).

    Column names are returned as-is; expressions become _scd_key_0, _scd_key_1, …
    so the alias is consistent when the call is repeated for both sides of a join.
    """
    raw = [unique_key] if isinstance(unique_key, str) else list(unique_key)
    result: list[str] = []
    for i, k in enumerate(raw):
        if k in df.columns:
            result.append(k)
        else:
            alias = f"_scd_key_{i}"
            df = df.with_columns(pl.sql_expr(k).alias(alias))
            result.append(alias)
    return df, result


def _add_scd_id(df: pl.DataFrame, unique_key: str | list[str]) -> pl.DataFrame:
    raw = [unique_key] if isinstance(unique_key, str) else list(unique_key)
    parts = [_key_part(df, k) for k in raw]
    key_expr = pl.concat_str(parts, separator="|") if len(parts) > 1 else parts[0]
    raw_str = pl.concat_str(
        [key_expr, pl.lit("|"), pl.col("dbt_updated_at").cast(pl.String).fill_null("")]
    )
    return df.with_columns(
        raw_str.map_elements(
            lambda s: hashlib.md5(s.encode()).hexdigest(),
            return_dtype=pl.String,
        ).alias("dbt_scd_id")
    )


class PolarsAdapter(BaseAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = PolarsConnectionManager
    Relation = PolarsRelation
    CatalogAdapters: dict[str, BaseCatalog] = {}

    def get_storage_catalog(self, name: str | None) -> BaseCatalog:
        connection = self.connections.get_thread_connection()
        credentials = cast(PolarsCredentials, connection.credentials)

        # Unquote the catalog name if it's quoted
        if name is not None:
            name = name.strip('"')

        if not name:
            name = credentials.catalog

        if name in self.CatalogAdapters:
            return self.CatalogAdapters[name]

        if name not in credentials.catalog_configs:
            raise DbtRuntimeError(f"Unknown catalog {name}")

        config = credentials.catalog_configs.get(name)
        if config is None:
            raise DbtRuntimeError(f"Unknown catalog {name}")
        self.CatalogAdapters[name] = CATALOG_REGISTRY[config.type](config)

        return self.CatalogAdapters[name]

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    # --- Catalog operations ---
    def create_schema(self, relation: PolarsRelation) -> None:  # type: ignore[override]
        self.get_storage_catalog(relation.catalog).create_schema(relation)

    def drop_schema(self, relation: PolarsRelation) -> None:  # type: ignore[override]
        self.get_storage_catalog(relation.catalog).drop_schema(relation)
        self.cache.drop_schema(relation.database, relation.schema)

    def list_schemas(self, database: str) -> list[str]:
        return self.get_storage_catalog(database).list_schemas()

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        polars_goal = cast(PolarsRelation, goal)
        polars_current = cast(PolarsRelation, current)
        if polars_goal.catalog != polars_current.catalog:
            # TODO: test this
            raise DbtRuntimeError(
                f"The provider currently doesn't support expanding column "
                f"types across catalogs. "
                f"{polars_current.catalog}.{polars_current.schema}"
                f".{polars_current.table} to "
                f"{polars_goal.catalog}.{polars_goal.schema}.{polars_goal.table} "
            )

        self.get_storage_catalog(polars_goal.catalog).expand_column_types(
            polars_goal, polars_current
        )

    def get_columns_in_relation(self, relation: PolarsRelation) -> list[Column]:  # type: ignore[override]
        catalog = self.get_storage_catalog(relation.catalog)
        if not catalog.table_exists(relation):
            return []
        schema = catalog.get_relation(relation).collect_schema()
        return [Column(col, str(type)) for col, type in schema.items()]

    def list_relations_without_caching(
        self,
        schema_relation: PolarsRelation,  # type: ignore[override]
    ) -> list[BaseRelation]:
        return cast(
            list[BaseRelation],
            self.get_storage_catalog(
                schema_relation.catalog
            ).list_relations_without_caching(schema_relation),
        )

    def rename_relation(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> None:
        # TODO: Do we need this
        # Normally this is used in dbt to write data first to a temp location and
        # then swap, but this is not necessary when using delta
        raise DbtRuntimeError("dbt-polars doesn't support renaming relations.")

    def drop_relation(self, relation: PolarsRelation) -> None:
        self.get_storage_catalog(relation.catalog).drop_relation(relation)
        self.cache_dropped(relation)

    def truncate_relation(self, relation: PolarsRelation) -> None:  # type: ignore[override]
        self.get_storage_catalog(relation.catalog).truncate_relation(relation)

    # --- persist_docs ---
    @available
    def polars_set_relation_comment(
        self, relation: PolarsRelation, comment: str
    ) -> None:
        self.get_storage_catalog(relation.catalog).set_relation_comment(
            relation, comment
        )

    @available
    def polars_set_column_comments(
        self, relation: PolarsRelation, columns: dict[str, Any]
    ) -> None:
        comments = {
            name: info["description"]
            for name, info in columns.items()
            if info.get("description")
        }
        if comments:
            self.get_storage_catalog(relation.catalog).set_column_comments(
                relation, comments
            )

    # --- docs generate / catalog.json ---
    def get_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: frozenset[tuple[str | None, str]],
    ) -> tuple[agate.Table, list[Exception]]:
        """Builds the catalog.json data directly from the filesystem/Delta metadata.

        Polars has no information_schema to query via SQL, so unlike most adapters this
        bypasses macro execution (the `get_catalog` macro) entirely and is implemented
        in pure Python.
        """

        # TODO: When adding more catalogs (like Databricks) see if this method
        # has to be pushed partially to the catalog and make a union over the
        # tables found in each catalog.

        column_names = [
            "table_database",
            "table_schema",
            "table_name",
            "table_type",
            "table_comment",
            "table_owner",
            "column_name",
            "column_index",
            "column_type",
            "column_comment",
        ]
        rows: list[dict[str, Any]] = []
        exceptions: list[Exception] = []

        for database, schema in used_schemas:
            try:
                storage_catalog = self.get_storage_catalog(database)
                schema_relation = self.Relation.create(
                    database=database, schema=schema, catalog=database
                )
                for relation in storage_catalog.list_relations_without_caching(
                    schema_relation
                ):
                    table_comment = storage_catalog.get_relation_comment(relation)
                    column_comments = storage_catalog.get_column_comments(relation)
                    for index, column in enumerate(
                        self.get_columns_in_relation(relation)
                    ):
                        rows.append(
                            {
                                "table_database": database,
                                "table_schema": schema,
                                "table_name": relation.identifier,
                                "table_type": "BASE TABLE",
                                "table_comment": table_comment,
                                "table_owner": None,
                                "column_name": column.name,
                                "column_index": index,
                                "column_type": column.dtype,
                                "column_comment": column_comments.get(column.name),
                            }
                        )
            except Exception as e:
                exceptions.append(e)

        return table_from_data(rows, column_names), exceptions

    # --- Type conversions ---
    @classmethod
    def convert_dbt_integer_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "Int64"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "Utf8"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "Float64" if decimals else "Int64"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "Boolean"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "Datetime"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "Date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "Duration"

    @available
    def polars_load_csv_rows(
        self,
        relation: PolarsRelation,
        agate_table: agate.Table,
        column_types: dict[str, str],
        partition_by: str | list[str] | None = None,
    ) -> None:
        import agate

        data = {
            col: [row[i] for row in agate_table.rows]
            for i, col in enumerate(agate_table.column_names)
        }
        df = pl.DataFrame(data)

        agate_type_converters = {
            DbtInteger: self.convert_dbt_integer_type,
            Number: self.convert_number_type,
            agate.Text: self.convert_text_type,
            agate.Number: self.convert_number_type,
            agate.Boolean: self.convert_boolean_type,
            agate.DateTime: self.convert_datetime_type,
            agate.Date: self.convert_date_type,
            agate.TimeDelta: self.convert_time_type,
        }

        casts = []
        for col_idx, col_name in enumerate(agate_table.column_names):
            if col_name in column_types:
                type_str = column_types[col_name]
            else:
                converter = agate_type_converters.get(
                    type(agate_table.column_types[col_idx])
                )
                if converter is None:
                    continue
                type_str = converter(agate_table, col_idx)
            casts.append(pl.col(col_name).cast(_resolve_polars_type(type_str)))

        if casts:
            df = df.with_columns(casts)

        self.get_storage_catalog(relation.catalog).write_relation(
            relation, df, _normalize_partition_by(partition_by)
        )

    @classmethod
    def quote(cls, identifier: str) -> str:
        return f'"{identifier}"'

    def _build_sql_context(
        self,
        refs: dict[str, PolarsRelation],
        extra_frames: dict[str, pl.LazyFrame] | None = None,
    ) -> pl.SQLContext:
        frames = {
            frame_name: self.get_storage_catalog(rel.database).get_relation(rel)
            for frame_name, rel in refs.items()
        }
        if extra_frames:
            frames.update(extra_frames)
        return pl.SQLContext(frames)

    @overload
    def _run_sql(
        self,
        sql: str,
        eager: Literal[True] = ...,
        extra_frames: dict[str, pl.LazyFrame] | None = ...,
    ) -> pl.DataFrame: ...

    @overload
    def _run_sql(
        self,
        sql: str,
        eager: Literal[False],
        extra_frames: dict[str, pl.LazyFrame] | None = ...,
    ) -> pl.LazyFrame: ...

    def _run_sql(
        self,
        sql: str,
        eager: bool = True,
        extra_frames: dict[str, pl.LazyFrame] | None = None,
    ) -> pl.DataFrame | pl.LazyFrame:
        rewritten_sql, refs = parse_and_rewrite(sql)
        return self._build_sql_context(refs, extra_frames).execute(
            rewritten_sql, eager=eager
        )

    def _is_python_cte(self, cte: dict) -> bool:
        return "def model(" in cte["sql"]

    @overload
    def _run_python_model(
        self,
        python_code: str,
        cte_frames: dict[str, pl.LazyFrame] | None = ...,
        allow_bool: Literal[False] = ...,
    ) -> pl.LazyFrame: ...

    @overload
    def _run_python_model(
        self,
        python_code: str,
        cte_frames: dict[str, pl.LazyFrame] | None = ...,
        allow_bool: Literal[True] = ...,
    ) -> pl.LazyFrame | bool: ...

    def _run_python_model(
        self,
        python_code: str,
        cte_frames: dict[str, pl.LazyFrame] | None = None,
        allow_bool: bool = False,
    ) -> pl.LazyFrame | bool:
        namespace: dict[str, Any] = {}
        exec(python_code, namespace)  # noqa: S102

        def load_df_function(ref_key: str) -> pl.LazyFrame:
            if cte_frames and ref_key in cte_frames:
                return cte_frames[ref_key]
            parts = re.findall(r'"([^"]+)"', ref_key)
            if len(parts) != 3:
                raise DbtRuntimeError(
                    f"Could not parse relation '{ref_key}': expected"
                    ' "catalog"."schema"."identifier"'
                )
            catalog, schema, identifier = parts
            relation = PolarsRelation.create(
                database=catalog,
                schema=schema,
                identifier=identifier,
                type=RelationType.Table,
                catalog=catalog,
            )
            return self.get_storage_catalog(catalog).get_relation(relation)

        dbt_obj = namespace["dbtObj"](load_df_function)
        result = namespace["model"](dbt_obj, pl)
        if isinstance(result, pl.DataFrame):
            return result.lazy()
        if isinstance(result, bool) and not allow_bool:
            raise DbtRuntimeError(
                "Python model code returned a boolean; only Python singular tests "
                "may return a boolean pass/fail result."
            )
        return result

    def _execute_python_cte(
        self, cte: dict, cte_frames: dict[str, pl.LazyFrame] | None = None
    ) -> pl.LazyFrame:
        body_match = re.search(r"\((.+)\)", cte["sql"], re.DOTALL)
        if not body_match:
            raise DbtRuntimeError(
                f"Could not extract Python code from CTE: {cte['id']}"
            )
        return self._run_python_model(body_match.group(1).strip(), cte_frames)

    def _evaluate_ctes(self, extra_ctes: list) -> dict[str, pl.LazyFrame]:
        cte_frames: dict[str, pl.LazyFrame] = {}
        for cte in extra_ctes:
            cte_name = f"__dbt__cte__{cte['id'].split('.')[-1]}"
            if self._is_python_cte(cte):
                cte_frames[cte_name] = self._execute_python_cte(cte, cte_frames)
            else:
                body_match = re.search(r"\((.+)\)", cte["sql"], re.DOTALL)
                if not body_match:
                    raise DbtRuntimeError(
                        f"Could not extract SQL from CTE: {cte['id']}"
                    )
                cte_frames[cte_name] = self._run_sql(
                    body_match.group(1).strip(),
                    eager=False,
                    extra_frames=cte_frames or None,
                )
        return cte_frames

    def _strip_all_ctes(self, code: str, extra_ctes: list) -> str:
        if not extra_ctes:
            return code
        prefix = "with" + ", ".join(cte["sql"] for cte in extra_ctes)
        tokens = [re.escape(t) for t in re.split(r"\s+", prefix) if t]
        prefix_pattern = r"\s+".join(tokens)

        stripped = code.lstrip()
        m = re.match(prefix_pattern, stripped, re.DOTALL | re.IGNORECASE)

        if not m:
            raise DbtRuntimeError(
                "Could not locate expected CTE prefix in compiled code. "
                "This is a dbt-polars bug — please report it."
                f"Code: {stripped}"
                f"Prefix {prefix_pattern}"
            )

        remainder = stripped[m.end() :].lstrip()
        # Remainder is a CTE - inject a with statement
        if remainder.startswith(","):
            return "with " + remainder[1:]
        else:
            return remainder

    def _apply_schema_change(
        self,
        catalog,
        relation: PolarsRelation,
        new_data: pl.DataFrame,
        on_schema_change: str,
        partition_by: list[str],
        model_config: dict = {},
    ) -> tuple[pl.DataFrame | None, bool]:
        """Apply on_schema_change policy before an incremental write.

        Returns (data, allow_evolution):
          - data: the DataFrame to write, possibly column-filtered or None if
            sync_all_columns already performed a full rewrite (caller
            should return early)
          - allow_evolution: True when the catalog should enable schema
            evolution on the append (new columns present and policy permits)
        """
        existing = {
            col.name: col.dtype for col in self.get_columns_in_relation(relation)
        }
        new_cols = new_data.columns
        added = set(new_cols) - set(existing.keys())
        removed = set(existing.keys()) - set(new_cols)

        if on_schema_change == "fail":
            type_changes = {
                col: (existing[col], str(new_data.schema[col]))
                for col in new_cols
                if col in existing and existing[col] != str(new_data.schema[col])
            }
            if added or removed or type_changes:
                raise CompilationError(
                    f"Schema change detected with on_schema_change='fail'. "
                    f"Added: {added or 'none'}. Removed: {removed or 'none'}. "
                    f"Type changes: {type_changes or 'none'}."
                )
            return new_data, False

        if on_schema_change == "ignore":
            return new_data.select([c for c in new_cols if c in existing]), False

        if on_schema_change == "append_new_columns":
            return new_data, bool(added)

        if on_schema_change == "sync_all_columns":
            if not removed:
                return new_data, bool(added)
            existing_df = catalog.get_relation(relation).collect()
            if added:
                existing_df = existing_df.with_columns(
                    [
                        pl.lit(None).cast(new_data.schema[col]).alias(col)
                        for col in added
                    ]
                )
            existing_df = existing_df.select(new_cols)
            catalog.write_relation(
                relation, pl.concat([existing_df, new_data]), partition_by, model_config
            )
            return None, False

        return new_data, False

    def _resolve_merge_except_cols(
        self,
        new_data: pl.DataFrame,
        merge_update_columns: str | list[str] | None,
        merge_exclude_columns: str | list[str] | None,
    ) -> list[str] | None:
        """Resolve merge_update_columns/merge_exclude_columns into an except_cols
        list for deltalake's when_matched_update_all(except_cols=...).

        Returns None when neither is set (update all columns - current
        default). Dest columns are taken from new_data (the post
        on_schema_change dataframe about to be written), which already
        reflects the destination's post-write column set.
        """
        if merge_update_columns and merge_exclude_columns:
            raise DbtRuntimeError(
                "Model cannot specify merge_update_columns and merge_exclude_columns. "
                "Please update model to use only one config"
            )

        if not merge_update_columns and not merge_exclude_columns:
            return None

        dest_cols = new_data.columns

        if merge_exclude_columns:
            exclude = (
                [merge_exclude_columns]
                if isinstance(merge_exclude_columns, str)
                else merge_exclude_columns
            )
            exclude_lower = {c.lower() for c in exclude}
            return [c for c in dest_cols if c.lower() in exclude_lower]

        # merge_update_columns is an allow-list; except_cols is its complement
        update = (
            [merge_update_columns]
            if isinstance(merge_update_columns, str)
            else (merge_update_columns or [])
        )
        update_lower = {c.lower() for c in update}
        return [c for c in dest_cols if c.lower() not in update_lower]

    @available
    def _incremental_write(
        self,
        catalog: BaseCatalog,
        relation: PolarsRelation,
        new_data: pl.DataFrame,
        unique_key: str | list[str] | None,
        strategy: str,
        on_schema_change: str,
        merge_update_columns: str | list[str] | None = None,
        merge_exclude_columns: str | list[str] | None = None,
        incremental_predicates: str | list[str] | None = None,
        partition_by: str | list[str] | None = None,
        model_config: dict = {},
    ) -> None:
        partition_by = _normalize_partition_by(partition_by)
        current_partitions = catalog.get_partition_columns(relation)
        if current_partitions != partition_by:
            raise DbtRuntimeError(
                f"Relation {relation} is partitioned by {current_partitions!r}, but "
                f"the model is configured with partition_by={partition_by!r}. "
                "Changing partitioning on an existing incremental model requires "
                "`--full-refresh`."
            )

        schema_result, allow_evolution = self._apply_schema_change(
            catalog, relation, new_data, on_schema_change, partition_by, model_config
        )

        if schema_result is None:
            return  # sync_all_columns already performed a full rewrite

        new_data = schema_result

        if isinstance(incremental_predicates, str):
            incremental_predicates = [incremental_predicates]

        if strategy == "append":
            catalog.append_relation(
                relation,
                new_data,
                allow_schema_evolution=allow_evolution,
                model_config=model_config,
            )
        elif strategy == "merge":
            if not unique_key:
                raise DbtRuntimeError("'merge' strategy requires a unique_key")
            keys = [unique_key] if isinstance(unique_key, str) else unique_key
            except_cols = self._resolve_merge_except_cols(
                new_data, merge_update_columns, merge_exclude_columns
            )
            catalog.merge_relation(
                relation,
                new_data,
                keys,
                except_cols=except_cols,
                incremental_predicates=incremental_predicates or None,
                allow_schema_evolution=allow_evolution,
            )
        elif strategy == "delete+insert":
            if not unique_key:
                raise DbtRuntimeError("'delete+insert' strategy requires a unique_key")
            keys = [unique_key] if isinstance(unique_key, str) else unique_key
            catalog.delete_matched_relation(
                relation,
                new_data,
                keys,
                incremental_predicates=incremental_predicates or None,
            )
            catalog.append_relation(
                relation,
                new_data,
                allow_schema_evolution=allow_evolution,
                model_config=model_config,
            )
        else:
            raise DbtRuntimeError(f"Unknown incremental strategy: {strategy!r}")

    @available
    def polars_execute_incremental_model(
        self,
        relation: PolarsRelation,
        sql: str,
        unique_key: str | list[str] | None,
        strategy: str,
        on_schema_change: str,
        merge_update_columns: str | list[str] | None = None,
        merge_exclude_columns: str | list[str] | None = None,
        incremental_predicates: str | list[str] | None = None,
        extra_ctes: list | None = None,
        partition_by: str | list[str] | None = None,
        model_config: dict = {},
    ) -> None:
        ctes = extra_ctes or []
        cte_frames = self._evaluate_ctes(ctes)
        sql = self._strip_all_ctes(sql, ctes)
        new_data = self._run_sql(sql, extra_frames=cte_frames or None)
        catalog = self.get_storage_catalog(relation.catalog)
        self._incremental_write(
            catalog,
            relation,
            new_data,
            unique_key,
            strategy,
            on_schema_change,
            merge_update_columns,
            merge_exclude_columns,
            incremental_predicates,
            partition_by,
            model_config,
        )

    @available
    def polars_execute_model(
        self,
        relation: PolarsRelation,
        sql: str,
        extra_ctes: list | None = None,
        partition_by: str | list[str] | None = None,
        model_config: dict = {},
    ) -> None:
        ctes = extra_ctes or []
        cte_frames = self._evaluate_ctes(ctes)
        sql = self._strip_all_ctes(sql, ctes)
        result = self._run_sql(sql, eager=False, extra_frames=cte_frames or None)
        catalog = self.get_storage_catalog(relation.catalog)
        catalog.write_relation(
            relation, result, _normalize_partition_by(partition_by), model_config
        )

    def submit_python_job(
        self, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
        extra_ctes = parsed_model.get("extra_ctes", [])
        cte_frames = self._evaluate_ctes(extra_ctes)
        python_code = self._strip_all_ctes(compiled_code, extra_ctes)

        lazy_result = self._run_python_model(python_code, cte_frames or None)

        target_relation = PolarsRelation.create(
            database=parsed_model["database"],
            schema=parsed_model["schema"],
            identifier=parsed_model["alias"],
            type=RelationType.Table,
            catalog=parsed_model["database"],
        )
        catalog = self.get_storage_catalog(target_relation.catalog)
        model_config = parsed_model.get("config", {})

        partition_by = _normalize_partition_by(model_config.get("partition_by"))

        if model_config.get("materialized") == "incremental" and catalog.table_exists(
            target_relation
        ):
            unique_key = model_config.get("unique_key")
            strategy = (
                model_config.get("incremental_strategy")
                or (unique_key and "merge")
                or "append"
            )
            on_schema_change = model_config.get("on_schema_change") or "ignore"
            self._incremental_write(
                catalog,
                target_relation,
                lazy_result.collect(),
                unique_key,
                strategy,
                on_schema_change,
                merge_update_columns=model_config.get("merge_update_columns"),
                merge_exclude_columns=model_config.get("merge_exclude_columns"),
                incremental_predicates=model_config.get("predicates")
                or model_config.get("incremental_predicates"),
                partition_by=partition_by,
                model_config=model_config,
            )
        else:
            catalog.write_relation(
                target_relation, lazy_result, partition_by, model_config
            )

        return AdapterResponse(_message="OK")

    @available
    def polars_execute_sql_test(
        self,
        sql: str,
        extra_ctes: list | None = None,
        limit: int | None = None,
        fail_calc: str = "count(*)",
        warn_if: str = "!= 0",
        error_if: str = "!= 0",
        store_failures_relation: PolarsRelation | None = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        extra_ctes = extra_ctes or []
        cte_frames = self._evaluate_ctes(extra_ctes)
        stripped_sql = self._strip_all_ctes(sql, extra_ctes)

        result = self._run_sql(
            stripped_sql, eager=False, extra_frames=cte_frames or None
        )

        if limit is not None:
            result = result.limit(limit)
        frame = result.collect()

        if store_failures_relation is not None:
            self.get_storage_catalog(store_failures_relation.catalog).write_relation(
                store_failures_relation, frame, []
            )

        summary = frame.select(
            pl.sql_expr(fail_calc).alias("failures"),
            pl.sql_expr(f"{fail_calc} {warn_if}").alias("should_warn"),
            pl.sql_expr(f"{fail_calc} {error_if}").alias("should_error"),
        )
        table = table_from_data(summary.to_dicts(), summary.columns)
        return AdapterResponse(_message="OK"), table

    @available
    def execute_python_test(
        self,
        parsed_model: dict,
        compiled_code: str,
        limit: int | None = None,
        fail_calc: str = "count(*)",
        warn_if: str = "!= 0",
        error_if: str = "!= 0",
        store_failures_relation: PolarsRelation | None = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        extra_ctes = parsed_model.get("extra_ctes", [])
        cte_frames = self._evaluate_ctes(extra_ctes)
        python_code = self._strip_all_ctes(compiled_code, extra_ctes)

        result = self._run_python_model(
            python_code, cte_frames or None, allow_bool=True
        )

        if isinstance(result, bool):
            failures = 0 if result else 1
            table = table_from_data(
                [
                    {
                        "failures": failures,
                        "should_warn": not result,
                        "should_error": not result,
                    }
                ],
                ["failures", "should_warn", "should_error"],
            )
            return AdapterResponse(_message="OK"), table

        if limit is not None:
            result = result.limit(limit)
        frame = result.collect()

        if store_failures_relation is not None:
            self.get_storage_catalog(store_failures_relation.catalog).write_relation(
                store_failures_relation, frame, []
            )

        summary = frame.select(
            pl.sql_expr(fail_calc).alias("failures"),
            pl.sql_expr(f"{fail_calc} {warn_if}").alias("should_warn"),
            pl.sql_expr(f"{fail_calc} {error_if}").alias("should_error"),
        )
        table = table_from_data(summary.to_dicts(), summary.columns)
        return AdapterResponse(_message="OK"), table

    def polars_execute_snapshot(
        self,
        relation: PolarsRelation,
        sql: str,
        unique_key: str | list[str],
        strategy: str,
        updated_at: str | None,
        check_cols: str | list[str] | None,
        hard_deletes: str,
        extra_ctes: list | None,
        meta_cols: SnapshotMetaColumnNames | dict[str, str] | None = None,
        valid_to_current_expr: str | None = None,
    ) -> None:
        print("Executing snapshot")

        if isinstance(meta_cols, SnapshotMetaColumnNames):
            meta_cols = {
                key: value for key, value in meta_cols.to_dict().items() if value
            }

        meta_cols = meta_cols or {}

        ctes = extra_ctes or []
        cte_frames = self._evaluate_ctes(ctes)
        sql = self._strip_all_ctes(sql, ctes)
        source_df = self._run_sql(sql, extra_frames=cte_frames or None)

        now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

        if strategy == "timestamp":
            if updated_at is None:
                raise DbtRuntimeError(
                    "snapshot strategy 'timestamp' requires an 'updated_at' config"
                )
            source_df = source_df.with_columns(
                pl.col(updated_at).cast(pl.Datetime("us")).alias("dbt_updated_at")
            )
        else:
            source_df = source_df.with_columns(pl.lit(now).alias("dbt_updated_at"))

        source_df = _add_scd_id(source_df, unique_key)

        valid_to_sentinel: object = None
        if valid_to_current_expr:
            valid_to_sentinel = pl.select(
                pl.sql_expr(valid_to_current_expr).cast(pl.Datetime("us"))
            ).to_series()[0]

        catalog = self.get_storage_catalog(relation.catalog)
        if catalog.table_exists(relation):
            self._validate_snapshot_meta_cols(
                catalog, relation, meta_cols, hard_deletes
            )
            self._snapshot_apply_scd2(
                catalog,
                relation,
                source_df,
                unique_key,
                strategy,
                updated_at,
                check_cols,
                hard_deletes,
                now,
                meta_cols,
                valid_to_sentinel,
            )
        else:
            open_valid_to = pl.lit(valid_to_sentinel).cast(pl.Datetime("us"))
            first_df = source_df.with_columns(
                [
                    pl.col("dbt_updated_at").alias("dbt_valid_from"),
                    open_valid_to.alias("dbt_valid_to"),
                ]
            )
            if hard_deletes == "new_record":
                first_df = first_df.with_columns(pl.lit(False).alias("dbt_is_deleted"))
            if meta_cols:
                first_df = first_df.rename(
                    {k: v for k, v in meta_cols.items() if k in first_df.columns}
                )
            catalog.write_relation(relation, first_df, [])

    def _validate_snapshot_meta_cols(
        self,
        catalog: BaseCatalog,
        relation: PolarsRelation,
        meta_cols: dict[str, str],
        hard_deletes: str,
    ) -> None:
        internal = {"dbt_valid_to", "dbt_valid_from", "dbt_scd_id", "dbt_updated_at"}
        if hard_deletes == "new_record":
            internal.add("dbt_is_deleted")
        expected_external = {meta_cols.get(c, c) for c in internal}
        existing_cols = set(catalog.get_relation(relation).collect_schema().names())
        missing = expected_external - existing_cols
        if missing:
            raise DbtRuntimeError(
                "Snapshot target is missing configured columns: "
                + ", ".join(sorted(missing))
            )

    def _snapshot_apply_scd2(
        self,
        catalog: BaseCatalog,
        relation: PolarsRelation,
        source_df: pl.DataFrame,
        unique_key: str | list[str],
        strategy: str,
        updated_at: str | None,
        check_cols: str | list[str] | None,
        hard_deletes: str,
        now: datetime,
        meta_cols: dict[str, str],
        valid_to_sentinel: object,
    ) -> None:
        if hard_deletes not in ("ignore", "invalidate", "new_record"):
            raise DbtRuntimeError(
                f"dbt-polars snapshots do not support hard_deletes='{hard_deletes}'"
            )

        # Read existing snapshot; rename external column names to internal
        meta_cols_rev = {v: k for k, v in meta_cols.items()}
        existing = catalog.get_relation(relation).collect()
        if meta_cols_rev:
            existing = existing.rename(
                {e: i for e, i in meta_cols_rev.items() if e in existing.columns}
            )

        open_filter = (
            pl.col("dbt_valid_to") == valid_to_sentinel
            if valid_to_sentinel is not None
            else pl.col("dbt_valid_to").is_null()
        )
        existing_open = existing.filter(open_filter)

        # Materialise any SQL expression keys as derived columns for joining
        existing_for_join, keys = _resolve_join_keys(existing_open, unique_key)
        source_for_join, _ = _resolve_join_keys(source_df, unique_key)

        matched = existing_for_join.join(
            source_for_join, on=keys, how="inner", suffix="_new"
        )

        if strategy == "timestamp":
            changed_mask = pl.col(f"{updated_at}_new") > pl.col("dbt_updated_at")
        else:
            dbt_added = {"dbt_updated_at", "dbt_scd_id"}
            if check_cols == "all":
                raw_keys = (
                    [unique_key] if isinstance(unique_key, str) else list(unique_key)
                )
                check_col_list = [
                    c
                    for c in source_df.columns
                    if c not in raw_keys and c not in dbt_added
                ]
            else:
                check_col_list = (
                    [check_cols]
                    if isinstance(check_cols, str)
                    else list(check_cols or [])
                )
            changed_mask = pl.any_horizontal(
                [pl.col(f"{c}_new").ne_missing(pl.col(c)) for c in check_col_list]
            )

        changed = matched.filter(changed_mask)

        rows_to_close = changed.with_columns(
            [
                pl.col("dbt_updated_at_new").alias("dbt_valid_to"),
                pl.col("dbt_updated_at_new").alias("dbt_updated_at"),
            ]
        ).select(existing_open.columns)

        new_keys = source_for_join.join(existing_for_join, on=keys, how="anti")
        changed_new = source_for_join.join(changed.select(keys), on=keys, how="semi")
        base = pl.concat([new_keys, changed_new]).with_columns(
            pl.col("dbt_updated_at").alias("dbt_valid_from")
        )
        if hard_deletes == "new_record":
            base = base.with_columns(pl.lit(False).alias("dbt_is_deleted"))
        open_valid_to = pl.lit(valid_to_sentinel).cast(base.schema["dbt_valid_from"])
        rows_to_insert = base.with_columns(open_valid_to.alias("dbt_valid_to")).select(
            existing_open.columns
        )

        if hard_deletes in ("invalidate", "new_record"):
            deleted_open = existing_for_join.join(source_for_join, on=keys, how="anti")
            if not deleted_open.is_empty():
                rows_to_close = pl.concat(
                    [
                        rows_to_close,
                        deleted_open.with_columns(
                            [
                                pl.lit(now).alias("dbt_valid_to"),
                                pl.lit(now).alias("dbt_updated_at"),
                            ]
                        ).select(existing_open.columns),
                    ]
                )
                if hard_deletes == "new_record":
                    deleted_markers = deleted_open.with_columns(
                        [
                            pl.lit(now).alias("dbt_valid_from"),
                            pl.lit(now).alias("dbt_updated_at"),
                            open_valid_to.alias("dbt_valid_to"),
                            pl.lit(True).alias("dbt_is_deleted"),
                        ]
                    ).select(existing_open.columns)
                    rows_to_insert = pl.concat([rows_to_insert, deleted_markers])

        # Rename internal column names back to configured external names before writing
        scd_id_col = meta_cols.get("dbt_scd_id", "dbt_scd_id")
        if meta_cols:
            rename = {k: v for k, v in meta_cols.items()}
            rows_to_close = rows_to_close.rename(
                {k: v for k, v in rename.items() if k in rows_to_close.columns}
            )
            rows_to_insert = rows_to_insert.rename(
                {k: v for k, v in rename.items() if k in rows_to_insert.columns}
            )
        catalog.apply_snapshot_delta(
            relation, rows_to_close, rows_to_insert, scd_id_col
        )

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: int | None = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        if not fetch:
            return AdapterResponse(_message="OK"), empty_table()

        df = self._run_sql(sql)
        if limit is not None and limit >= 0:
            df = df.head(limit)
        return AdapterResponse(_message="OK"), table_from_data(
            df.to_dicts(), df.columns
        )


# may require more build out to make more user friendly to confer with team
# and community.
