from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import polars as pl
import sqlglot
import sqlglot.expressions as exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from dbt.adapters.polars.connections import PolarsConnectionManager, PolarsCredentials
from dbt.adapters.base import BaseAdapter, BaseRelation, available, Column
from dbt.adapters.base.relation import RelationType
from dbt.adapters.polars.catalogs import BaseCatalog, CATALOG_REGISTRY
from dbt.adapters.polars.relation import PolarsRelation
from dbt.adapters.contracts.connection import AdapterResponse
from dbt_common.exceptions import DbtRuntimeError, CompilationError
from dbt_common.clients.agate_helper import (
    Number,
    Integer as DbtInteger,
    empty_table,
    table_from_data,
)
from dbt.adapters.events.logging import AdapterLogger

if TYPE_CHECKING:
    import agate

_POLARS_TYPE_MAP: dict[str, pl.PolarsDataType] = {
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


def _resolve_polars_type(type_str: str) -> pl.PolarsDataType:
    key = type_str.split("(")[0].strip()
    dtype = _POLARS_TYPE_MAP.get(key) or _POLARS_TYPE_MAP.get(key.lower())
    if dtype is None:
        raise DbtRuntimeError(f"Unknown column type: '{type_str}'")
    return dtype


logger = AdapterLogger("polars")


def _parse_and_rewrite(
    sql: str,
) -> tuple[str, dict[str, PolarsRelation]]:
    """Strip catalog/schema qualifiers from three-level dbt refs.

    Every qualified table claims its bare name in the flat SQLContext namespace;
    an explicit alias claims a second, query-local name for the same table. When
    a name is claimed by more than one distinct (catalog, schema, identifier) --
    either because the same bare identifier is sourced from multiple schemas, or
    because one table's alias collides with a different table's bare name --
    the affected tables are renamed to catalog__schema__identifier to avoid
    collisions in SQLContext. Raises DbtRuntimeError when a collision cannot be
    safely resolved because the bare table name is used as a column qualifier
    (e.g. orders.id) without an alias of its own.

    Returns the rewritten SQL and a dict mapping frame_name to PolarsRelation.
    """
    ast = sqlglot.parse_one(sql)

    qualified = [n for n in ast.find_all(exp.Table) if n.name and (n.catalog or n.db)]

    # Map every name a table claims in the flat namespace (its bare identifier,
    # plus its alias if any) back to the (catalog, schema, identifier) tables
    # claiming it, so a name claimed by more than one distinct table -- via
    # either route -- is detected as colliding.
    identities_by_name: dict[str, set[tuple[str, str, str]]] = {}
    identifier_sources: dict[str, set[tuple[str, str]]] = {}
    for node in qualified:
        identity = (node.catalog, node.db, node.name)
        identities_by_name.setdefault(node.name, set()).add(identity)
        identifier_sources.setdefault(node.name, set()).add((node.catalog, node.db))
        if node.alias:
            identities_by_name.setdefault(node.alias, set()).add(identity)

    colliding = {name for name, idents in identities_by_name.items() if len(idents) > 1}

    if colliding:
        # Resolve each qualified column reference to its actual source within
        # its own scope (CTE/subquery-aware), rather than a flat textual match
        # across the whole query -- an alias in one CTE must not be confused
        # with an unrelated bare-name collision in another.
        for scope in traverse_scope(ast):
            for col in scope.columns:
                if col.table not in colliding:
                    continue
                source = scope.sources.get(col.table)
                if isinstance(source, Scope):
                    continue  # resolves to a CTE/derived table, not a physical one
                if isinstance(source, exp.Table) and source.args.get("alias"):
                    continue  # bound through its own alias; the rename below
                    # only touches the bare identifier, so this is unaffected

                # Either this is the colliding table's own bare identifier, or
                # the qualifier couldn't be resolved within this scope (e.g. a
                # correlated reference into an outer scope). Don't risk
                # silently mis-binding it -- require the user to disambiguate.
                name = source.name if isinstance(source, exp.Table) else col.table
                sources = identifier_sources.get(name, set())
                if len(sources) > 1:
                    detail = (
                        "exists in multiple schemas "
                        f"({', '.join(f'{c}.{s}' for c, s in sources)})"
                    )
                else:
                    detail = "is also used as an alias for a different table"
                raise DbtRuntimeError(
                    f"Identifier '{name}' {detail} and is used as a "
                    f"column qualifier (e.g. {name}.<col>) without a table alias. "
                    f"Add an explicit alias to each occurrence to disambiguate."
                )

    rename_map: dict[tuple[str, str, str], str] = {
        (node.catalog, node.db, node.name): (
            f"{node.catalog.strip(chr(34))}"
            f"__{node.db.strip(chr(34))}"
            f"__{node.name}"
        )
        for node in qualified
        if node.name in colliding
    }

    refs: dict[str, PolarsRelation] = {}

    def strip_qualifiers(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Table) and node.name and (node.catalog or node.db):
            raw_key = (node.catalog, node.db, node.name)
            frame_name = rename_map.get(raw_key, node.name)
            refs[frame_name] = PolarsRelation.create(
                database=node.catalog,
                schema=node.db,
                identifier=node.name,
                type=RelationType.Table,
            )
            return exp.Table(
                this=exp.Identifier(this=frame_name, quoted=False),
                alias=node.args.get("alias"),
            )
        return node

    rewritten = ast.transform(strip_qualifiers)
    return rewritten.sql(), refs


class PolarsAdapter(BaseAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = PolarsConnectionManager
    Relation = PolarsRelation
    CatalogAdapters: dict[str, BaseCatalog] = {}

    def get_catalog(self, name: Optional[str]) -> BaseCatalog:
        connection = self.connections.get_thread_connection()
        credentials: PolarsCredentials = connection.credentials

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
    def create_schema(self, relation: BaseRelation) -> None:
        self.get_catalog(relation.catalog).create_schema(relation)

    def drop_schema(self, relation: BaseRelation) -> None:
        self.get_catalog(relation.catalog).drop_schema(relation)

    def list_schemas(self, database: str) -> list[str]:
        return self.get_catalog(database).list_schemas()

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        if goal.catalog != current.catalog:
            # TODO: test this
            raise DbtRuntimeError(
                f"The provider currently doesn't support expanding column types across catalogs. {current.catalog}.{current.schema}.{current.table} to {goal.catalog}.{goal.schema}.{goal.table} "
            )

        self.get_catalog(goal.catalog).expand_column_types(goal, current)

    def get_columns_in_relation(self, relation: BaseRelation) -> list[Column]:
        catalog = self.get_catalog(relation.catalog)
        if not catalog.table_exists(relation):
            return []
        schema = catalog.get_relation(relation).collect_schema()
        return [Column(col, str(type)) for col, type in schema.items()]

    def list_relations_without_caching(
        self, schema_relation: BaseRelation
    ) -> list[BaseRelation]:
        return self.get_catalog(schema_relation.catalog).list_relations_without_caching(
            schema_relation
        )

    def rename_relation(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> None:
        if from_relation.catalog != to_relation.catalog:
            # TODO: is this relevant, does dbt support this?
            raise DbtRuntimeError(
                f"The provider currently doesn't support renaming tables across catalogs. {from_relation.catalog}.{from_relation.schema}.{from_relation.table} to {to_relation.catalog}.{to_relation.schema}.{to_relation.table} "
            )
        return self.get_catalog(from_relation.catalog).rename_relation(
            from_relation, to_relation
        )

    def drop_relation(self, relation: PolarsRelation) -> None:
        self.get_catalog(relation.catalog).drop_relation(relation)

    def truncate_relation(self, relation: BaseRelation) -> None:
        self.get_catalog(relation.catalog).truncate_relation(relation)

    # --- Type conversions ---
    @classmethod
    def convert_dbt_integer_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "Int64"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "Utf8"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "Float64" if decimals else "Int64"

    @classmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "Boolean"

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "Datetime"

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "Date"

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "Duration"

    @available
    def polars_load_csv_rows(
        self,
        relation: PolarsRelation,
        agate_table: "agate.Table",
        column_types: dict[str, str],
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

        self.get_catalog(relation.catalog).write_relation(relation, df)

    @classmethod
    def quote(cls, identifier: str) -> str:
        return f'"{identifier}"'

    def _build_sql_context(self, refs: dict[str, PolarsRelation]) -> pl.SQLContext:
        return pl.SQLContext({
            frame_name: self.get_catalog(rel.database).get_relation(rel)
            for frame_name, rel in refs.items()
        })

    def _run_sql(self, sql: str) -> pl.DataFrame:
        rewritten_sql, refs = _parse_and_rewrite(sql)
        return self._build_sql_context(refs).execute(rewritten_sql).collect()

    def _apply_schema_change(
        self,
        catalog,
        relation: PolarsRelation,
        new_data: pl.DataFrame,
        on_schema_change: str,
    ) -> tuple[pl.DataFrame | None, bool]:
        """Apply on_schema_change policy before an incremental write.

        Returns (data, allow_evolution):
          - data: the DataFrame to write, possibly column-filtered or None if
            sync_all_columns already performed a full rewrite (caller should return early)
          - allow_evolution: True when the catalog should enable schema evolution
            on the append (new columns present and policy permits them)
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
            catalog.write_relation(relation, pl.concat([existing_df, new_data]))
            return None, False

        return new_data, False

    @available
    def polars_execute_incremental_model(
        self,
        relation: PolarsRelation,
        sql: str,
        unique_key: Optional[str | list[str]],
        strategy: str,
        on_schema_change: str,
    ) -> None:
        new_data = self._run_sql(sql)
        catalog = self.get_catalog(relation.catalog)

        new_data, allow_evolution = self._apply_schema_change(
            catalog, relation, new_data, on_schema_change
        )

        if new_data is None:
            return  # sync_all_columns already performed a full rewrite

        if strategy == "append":
            catalog.append_relation(
                relation, new_data, allow_schema_evolution=allow_evolution
            )
        elif strategy == "merge":
            if not unique_key:
                raise DbtRuntimeError("'merge' strategy requires a unique_key")
            keys = [unique_key] if isinstance(unique_key, str) else unique_key
            predicate = " AND ".join(f"s.{k} = t.{k}" for k in keys)
            catalog.merge_relation(relation, new_data, predicate)
        elif strategy == "delete+insert":
            if not unique_key:
                raise DbtRuntimeError("'delete+insert' strategy requires a unique_key")
            keys = [unique_key] if isinstance(unique_key, str) else unique_key
            predicate = " AND ".join(f"s.{k} = t.{k}" for k in keys)
            catalog.delete_matched_relation(relation, new_data, predicate)
            catalog.append_relation(
                relation, new_data, allow_schema_evolution=allow_evolution
            )
        else:
            raise DbtRuntimeError(f"Unknown incremental strategy: {strategy!r}")

    @available
    def polars_execute_model(self, relation: PolarsRelation, sql: str) -> None:
        result = self._run_sql(sql)
        self.get_catalog(relation.catalog).write_relation(relation, result)

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        if not fetch:
            return AdapterResponse(_message="OK"), empty_table()

        df = self._run_sql(sql)
        if limit is not None and limit >= 0:
            df = df.head(limit)
        return AdapterResponse(_message="OK"), table_from_data(
            df.to_dicts(), df.columns
        )


# may require more build out to make more user friendly to confer with team and community.
