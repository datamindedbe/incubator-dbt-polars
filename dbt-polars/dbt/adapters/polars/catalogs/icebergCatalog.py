from typing import Any

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

import polars as pl

logger = AdapterLogger("polars")

# Maps pyiceberg FileIO property names to Polars/object_store storage option keys.
# Populated by credential vending from REST catalogs (e.g. Unity Catalog, Polaris).
# TODO: map all variables names from https://py.iceberg.apache.org/configuration/#loading-a-custom-location-provider
_ICEBERG_TO_POLARS_STORAGE_OPTIONS: dict[str, str] = {
    "adls.account-name": "account_name",
    "adls.sas-token": "sas_token",
    "adls.client-id": "client_id",
    "adls.client-secret": "client_secret",
    "adls.tenant-id": "tenant_id",
}


def _scan_iceberg(tbl: Any) -> pl.LazyFrame:
    """Scan an iceberg table, forwarding vended credentials to Polars' storage layer."""
    io_props: dict[str, str] = getattr(getattr(tbl, "io", None), "properties", {})
    storage_options = {
        polars_key: io_props[iceberg_key]
        for iceberg_key, polars_key in _ICEBERG_TO_POLARS_STORAGE_OPTIONS.items()
        if iceberg_key in io_props
    }
    return pl.scan_iceberg(tbl, storage_options=storage_options or None)


def _apply_partial_update(
    matched_existing: pl.DataFrame,
    matched_incoming: pl.DataFrame,
    keys: list[str],
    update_cols: list[str],
) -> pl.DataFrame:
    """Return matched_existing with update_cols overwritten from matched_incoming."""
    if not update_cols:
        return matched_existing
    return (
        matched_existing.join(
            matched_incoming.select(keys + update_cols),
            on=keys,
            how="left",
            suffix="_new",
        )
        .with_columns([pl.col(f"{c}_new").alias(c) for c in update_cols])
        .select(matched_existing.columns)
    )


def _build_key_delete_filter(keys: list[str], df: pl.DataFrame):
    from pyiceberg.expressions import And, EqualTo, In, Or

    if len(keys) == 1:
        return In(keys[0], df[keys[0]].to_list())
    row_filters = [
        And(*[EqualTo(k, row[k]) for k in keys]) for row in df.select(keys).to_dicts()
    ]
    return Or(*row_filters) if len(row_filters) > 1 else row_filters[0]


class IcebergCatalogConfig(CatalogConfig):
    """Config for a pyiceberg-backed catalog.

    All profile keys beyond `name` and `type` are passed through to
    pyiceberg's load_catalog, so any backend (REST, Glue, Hive, SQL, …)
    works by supplying the properties that backend requires.
    """

    def __init__(
        self,
        *,
        name: str,
        type: str,
        pyiceberg_type: str | None = None,
        **kwargs: object,
    ) -> None:
        self.name = name
        self.type = type
        if pyiceberg_type is not None:
            kwargs = {"type": pyiceberg_type, **kwargs}
        self._catalog_properties: dict[str, object] = kwargs

    def unique_field(self) -> str:
        return str(sorted(self._catalog_properties.items()))

    def connection_keys(self) -> tuple[str, ...]:
        return ("name",) + tuple(self._catalog_properties.keys())


class IcebergCatalog(BaseCatalog):
    config: IcebergCatalogConfig

    def __init__(self, config: IcebergCatalogConfig) -> None:
        super().__init__(config)
        self._table_cache: dict[tuple[str, str], Any] = {}
        self._known_namespaces: set[str] = set()

    @property
    def _catalog(self):
        if not hasattr(self, "_catalog_instance"):
            from pyiceberg.catalog import load_catalog

            self._catalog_instance = load_catalog(
                self.config.name,
                **self.config._catalog_properties,
            )
        return self._catalog_instance

    def _load_table(self, identifier: tuple[str, str]) -> Any:
        """Return a cached Table, fetching from the catalog on first access."""
        if identifier not in self._table_cache:
            self._table_cache[identifier] = self._catalog.load_table(identifier)
        return self._table_cache[identifier]

    def _invalidate_table_cache(self, identifier: tuple[str, str]) -> None:
        self._table_cache.pop(identifier, None)

    def _id(self, relation: PolarsRelation) -> tuple[str, str]:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
        return (relation.schema, relation.identifier)

    def create_schema(self, relation: PolarsRelation) -> None:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if relation.schema in self._known_namespaces:
            return
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        logger.debug(f"Creating schema {relation.catalog}/{relation.schema}")
        try:
            self._catalog.create_namespace((relation.schema,))
        except NamespaceAlreadyExistsError:
            pass
        self._known_namespaces.add(relation.schema)

    def drop_schema(self, relation: PolarsRelation) -> None:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        logger.debug(f"Dropping schema {relation.catalog}/{relation.schema}")
        namespace = (relation.schema,)
        try:
            for table_id in self._catalog.list_tables(namespace):
                self._catalog.purge_table(table_id)
                self._invalidate_table_cache(table_id)
            self._catalog.drop_namespace(namespace)
        except Exception:
            pass
        self._known_namespaces.discard(relation.schema)

    def list_schemas(self) -> list[str]:
        return [ns[0] for ns in self._catalog.list_namespaces()]

    def table_exists(self, relation: PolarsRelation) -> bool:
        identifier = self._id(relation)
        if identifier in self._table_cache:
            return True
        return self._catalog.table_exists(identifier)

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return _scan_iceberg(self._load_table(self._id(relation)))

    def drop_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Dropping table if exists {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        identifier = self._id(relation)
        if self._catalog.table_exists(identifier):
            self._catalog.purge_table(identifier)
            self._invalidate_table_cache(identifier)

    def write_relation(self, relation: PolarsRelation, df: pl.DataFrame) -> None:
        logger.debug(
            f"Writing table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )

        identifier = self._id(relation)
        from pyiceberg.exceptions import NoSuchTableError

        try:
            self._catalog.purge_table(identifier)
            self._invalidate_table_cache(identifier)
        except NoSuchTableError:
            self.create_schema(relation)
        tbl = self._catalog.create_table(identifier, schema=df.to_arrow().schema)
        self._table_cache[identifier] = tbl
        df.write_iceberg(tbl, mode="append")

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        namespace = (schema_relation.schema,)
        from pyiceberg.exceptions import NoSuchNamespaceError

        try:
            tables = self._catalog.list_tables(namespace)
        except NoSuchNamespaceError:
            return []
        return [
            schema_relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=table_id[-1],
                type=RelationType.Table,
                catalog=schema_relation.catalog,
            )
            for table_id in tables
        ]

    def truncate_relation(self, relation: PolarsRelation) -> None:

        logger.debug(
            f"Truncating table {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        self._load_table(self._id(relation)).delete()

    def append_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None:
        tbl = self._load_table(self._id(relation))
        if allow_schema_evolution:
            with tbl.update_schema() as update:
                update.union_by_name(df.to_arrow().schema)

        df.write_iceberg(tbl, mode="append")

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None = None,
        incremental_predicates: list[str] | None = None,
        allow_schema_evolution: bool = False,
    ) -> None:
        missing_keys = [k for k in keys if k not in df.columns]
        if missing_keys:
            raise DbtRuntimeError(
                f"Unique key column(s) not found in model: {', '.join(missing_keys)}"
            )

        if incremental_predicates:
            raise NotImplementedError(
                "merge_relation with incremental_predicates is not supported "
                + "for IcebergCatalog"
            )
        keys = list(dict.fromkeys(keys))  # deduplicate, preserve order
        tbl = self._load_table(self._id(relation))
        if allow_schema_evolution:
            with tbl.update_schema() as update:
                update.union_by_name(df.to_arrow().schema)
        if except_cols is None:
            if not df.is_empty():
                tbl.delete(_build_key_delete_filter(keys, df))
                df.write_iceberg(tbl, mode="append")
            return

        except_set = set(except_cols)
        update_cols = [c for c in df.columns if c not in keys and c not in except_set]

        matched_existing = self._scan_existing_matched_rows(tbl, df, keys)
        incoming_new_rows = df.join(matched_existing.select(keys), on=keys, how="anti")

        if not matched_existing.is_empty():
            matched_incoming = df.join(
                matched_existing.select(keys), on=keys, how="semi"
            )
            updated_rows = _apply_partial_update(
                matched_existing, matched_incoming, keys, update_cols
            )
            tbl.delete(_build_key_delete_filter(keys, matched_existing))
        else:
            updated_rows = pl.DataFrame(schema=matched_existing.schema)

        rows_to_append = pl.concat([updated_rows, incoming_new_rows])
        if not rows_to_append.is_empty():
            rows_to_append.write_iceberg(tbl, mode="append")

    def _scan_existing_matched_rows(
        self, tbl, df: pl.DataFrame, keys: list[str]
    ) -> pl.DataFrame:
        return (
            _scan_iceberg(tbl)
            .filter(*[pl.col(k).is_in(df[k].to_list()) for k in keys])
            .join(df.lazy().select(keys), on=keys, how="semi")
            .collect()
        )

    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
    ) -> None:
        if incremental_predicates:
            raise NotImplementedError(
                "delete_matched_relation with incremental_predicates "
                + "is not supported for IcebergCatalog"
            )
        tbl = self._load_table(self._id(relation))
        tbl.delete(_build_key_delete_filter(keys, df))

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        tbl = self._load_table(self._id(relation))
        with tbl.transaction() as tx:
            tx.set_properties(comment=comment)

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        tbl = self._load_table(self._id(relation))
        return tbl.properties.get("comment")

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        tbl = self._load_table(self._id(relation))
        with tbl.update_schema() as update:
            for column, comment in comments.items():
                update.update_column(column, doc=comment)

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        tbl = self._load_table(self._id(relation))
        return {
            field.name: field.doc
            for field in tbl.schema().fields
            if field.doc is not None
        }
