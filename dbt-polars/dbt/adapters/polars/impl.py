from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import polars as pl

from dbt.adapters.polars.connections import PolarsConnectionManager, PolarsCredentials
from dbt.adapters.base import BaseAdapter, BaseRelation, available, Column
from dbt.adapters.polars.catalogs import BaseCatalog, CATALOG_REGISTRY
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.clients.agate_helper import Number, Integer as DbtInteger
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


# may require more build out to make more user friendly to confer with team and community.
