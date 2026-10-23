from __future__ import annotations

import itertools
import re
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, overload

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

import polars as pl
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.catalogs.iceberg_write_utils import (
    _append_in_single_snapshot,
    _replace_in_single_snapshot,
    _sync_partition_spec,
    _sync_schema,
    _write_data_files,
    delete_rows_by_keys,
    overwrite_keys_and_append,
)
from dbt.adapters.polars.relation import PolarsRelation
from dbt.adapters.polars.utils import resolve_relative_path

if TYPE_CHECKING:
    from pyiceberg.table import Table

logger = AdapterLogger("polars")

COMMIT_ATTEMPTS = 4

_UNSIGNED_TO_SIGNED: dict[type, type[pl.DataType]] = {
    pl.UInt8: pl.Int8,
    pl.UInt16: pl.Int16,
    pl.UInt32: pl.Int32,
    pl.UInt64: pl.Int64,
}


@overload
def _cast_unsigned_to_signed(data: pl.DataFrame) -> pl.DataFrame: ...


@overload
def _cast_unsigned_to_signed(data: pl.LazyFrame) -> pl.LazyFrame: ...


def _cast_unsigned_to_signed(
    data: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    # Iceberg has no unsigned integer types; cast to the corresponding signed type
    schema = data.collect_schema() if isinstance(data, pl.LazyFrame) else data.schema
    casts = [
        pl.col(name).cast(_UNSIGNED_TO_SIGNED[type(dtype)])
        for name, dtype in schema.items()
        if type(dtype) in _UNSIGNED_TO_SIGNED
    ]
    return data.with_columns(casts) if casts else data


# Maps pyiceberg FileIO property names to Polars/object_store storage option keys.
# Populated by credential vending from REST catalogs (e.g. Unity Catalog, Polaris).
# TODO: map all variables names from https://py.iceberg.apache.org/configuration/#loading-a-custom-location-provider
_ICEBERG_TO_POLARS_STORAGE_OPTIONS: dict[str, str] = {
    "adls.account-name": "account_name",
    "adls.sas-token": "sas_token",
    "adls.client-id": "client_id",
    "adls.client-secret": "client_secret",
    "adls.tenant-id": "tenant_id",
    "s3.access-key-id": "aws_access_key_id",
    "s3.secret-access-key": "aws_secret_access_key",
    "s3.session-token": "aws_session_token",
    "s3.region": "aws_region",
}

# Matches adls.sas-token.<account>.dfs.core.windows.net (not the -expires-at-ms variant)
_VENDED_SAS_RE = re.compile(r"^adls\.sas-token\.(.+)\.dfs\.core\.windows\.net$")


def _extract_vended_sas(io_props: dict, opts: dict) -> None:
    if "sas_token" in opts:
        return
    for key, value in io_props.items():
        m = _VENDED_SAS_RE.match(key)
        if m:
            opts["sas_token"] = value
            opts.setdefault("account_name", m.group(1))
            break


def _extract_vended_s3_region(io_props: dict, opts: dict) -> None:
    # pyiceberg's generic client.region only applies when the credentials are S3
    if "aws_access_key_id" in opts and "client.region" in io_props:
        opts.setdefault("aws_region", io_props["client.region"])


_SPECIAL_EXTRACTORS = [_extract_vended_sas, _extract_vended_s3_region]


def _storage_options(tbl: Table) -> dict[str, str] | None:
    io_props: dict[str, str] = getattr(getattr(tbl, "io", None), "properties", {})
    opts = {
        polars_key: io_props[iceberg_key]
        for iceberg_key, polars_key in _ICEBERG_TO_POLARS_STORAGE_OPTIONS.items()
        if iceberg_key in io_props
    }
    for extractor in _SPECIAL_EXTRACTORS:
        extractor(io_props, opts)
    return opts or None


def _scan_iceberg(tbl: Table) -> pl.LazyFrame:
    return pl.scan_iceberg(tbl, storage_options=_storage_options(tbl))


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


SECRET_PROPERTY_MARKERS = ("token", "secret", "password", "credential", "key")


def is_secret_property(name: str) -> bool:
    return any(marker in name.lower() for marker in SECRET_PROPERTY_MARKERS)


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
        schema: str,
        pyiceberg_type: str | None = None,
        **kwargs: object,
    ) -> None:

        self.name = name
        self.type = type
        self.schema = schema
        if pyiceberg_type is not None:
            kwargs = {"type": pyiceberg_type, **kwargs}
        self._catalog_properties: dict[str, object] = kwargs

    def visible_properties(self) -> dict[str, object]:
        return {
            key: value
            for key, value in self._catalog_properties.items()
            if not is_secret_property(key)
        }

    def unique_field(self) -> str:
        return str(sorted(self.visible_properties().items()))

    def connection_keys(self) -> tuple[str, ...]:
        keys = [
            "pyiceberg_type" if key == "type" else key
            for key in self.visible_properties()
        ]
        return ("name", *keys)


def _resolve_relative_uri(uri: str, prefix: str, project_root: str) -> str:
    if not uri.startswith(prefix):
        return uri
    raw_path = uri[len(prefix) :]
    if raw_path.startswith("/"):
        return uri
    return prefix + str(resolve_relative_path(raw_path, project_root))


class IcebergCatalog(BaseCatalog):
    config: IcebergCatalogConfig

    def __init__(self, config: IcebergCatalogConfig, project_root: str) -> None:
        super().__init__(config)
        properties = dict(config._catalog_properties)
        for key, value in properties.items():
            if not isinstance(value, str):
                continue
            for prefix in ("file://", "sqlite:///"):
                if value.startswith(prefix):
                    properties[key] = _resolve_relative_uri(value, prefix, project_root)
                    break
        self._catalog_properties = properties
        self._table_cache: dict[tuple[str, str], Any] = {}
        self._known_namespaces: set[str] = set()
        self._catalog_lock = threading.Lock()

    @property
    def _catalog(self):
        if not hasattr(self, "_catalog_instance"):
            with self._catalog_lock:
                if not hasattr(self, "_catalog_instance"):
                    from pyiceberg.catalog import load_catalog

                    self._catalog_instance = load_catalog(
                        self.config.name,
                        **self._catalog_properties,
                    )
        return self._catalog_instance

    def _load_table(self, identifier: tuple[str, str]) -> Table:
        """Return a cached Table, fetching from the catalog on first access."""
        if identifier not in self._table_cache:
            self._table_cache[identifier] = self._catalog.load_table(identifier)
        return self._table_cache[identifier]

    def _invalidate_table_cache(self, identifier: tuple[str, str]) -> None:
        self._table_cache.pop(identifier, None)

    def _commit(
        self, identifier: tuple[str, str], write: Callable[[Table], None]
    ) -> None:
        """Run `write`, which commits one change to the table, with retries.

        Some REST catalogs (e.g. Snowflake Horizon) reject a commit, or can't
        tell whether it was applied, right after another commit on the table.
        A commit with an unknown result counts as applied when the table's
        current snapshot is a new one on top of the snapshot it started from;
        otherwise, like a rejected commit, it's retried on a reloaded table.
        """
        from pyiceberg.exceptions import (
            CommitFailedException,
            CommitStateUnknownException,
        )

        for attempt in range(1, COMMIT_ATTEMPTS + 1):
            start = self._load_table(identifier).metadata.current_snapshot_id
            try:
                write(self._load_table(identifier))
                return
            except CommitFailedException:
                if attempt == COMMIT_ATTEMPTS:
                    raise
            except CommitStateUnknownException:
                self._invalidate_table_cache(identifier)
                current = self._load_table(identifier).current_snapshot()
                if (
                    current is not None
                    and current.snapshot_id != start
                    and current.parent_snapshot_id == start
                ):
                    return
                if attempt == COMMIT_ATTEMPTS:
                    raise
            logger.debug(f"Retrying commit to {identifier} (attempt {attempt + 1})")
            self._invalidate_table_cache(identifier)
            time.sleep(2**attempt)

    def catalog_name(self, name: str) -> str:
        """The name of a dbt schema or table in the Iceberg catalog."""
        return name

    def dbt_name(self, name: str) -> str:
        """The dbt name of a schema or table listed by the Iceberg catalog."""
        return name

    def _id(self, relation: PolarsRelation) -> tuple[str, str]:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
        return (
            self.catalog_name(relation.schema),
            self.catalog_name(relation.identifier),
        )

    def create_schema(self, relation: PolarsRelation) -> None:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        namespace = self.catalog_name(relation.schema)
        if namespace in self._known_namespaces:
            return
        from pyiceberg.exceptions import NamespaceAlreadyExistsError

        logger.debug(f"Creating schema {relation.catalog}/{namespace}")
        try:
            self._catalog.create_namespace((namespace,))
        except NamespaceAlreadyExistsError:
            pass
        self._known_namespaces.add(namespace)

    def drop_schema(self, relation: PolarsRelation) -> None:
        from pyiceberg.exceptions import NoSuchNamespaceError

        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        namespace = self.catalog_name(relation.schema)
        logger.debug(f"Dropping schema {relation.catalog}/{namespace}")
        try:
            for table_id in self._catalog.list_tables((namespace,)):
                self._catalog.purge_table(table_id)
                self._invalidate_table_cache(table_id)
            self._catalog.drop_namespace((namespace,))
        except NoSuchNamespaceError:
            pass
        self._known_namespaces.discard(namespace)

    def list_schemas(self) -> list[str]:
        return [self.dbt_name(ns[0]) for ns in self._catalog.list_namespaces()]

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

    def get_partition_columns(self, relation: PolarsRelation) -> list[str]:
        tbl = self._load_table(self._id(relation))
        return [f.name for f in tbl.spec().fields]

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        logger.debug(
            f"Writing table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        data = _cast_unsigned_to_signed(data)
        arrow_schema = (
            data.collect_schema().to_arrow()
            if isinstance(data, pl.LazyFrame)
            else data.to_arrow().schema
        )
        missing = [c for c in partition_by if c not in arrow_schema.names]
        if missing:
            raise DbtRuntimeError(
                f"partition_by column(s) not found in model: {', '.join(missing)}"
            )

        if not relation.identifier:
            raise DbtRuntimeError("relation identifier cannot be empty")

        target_id = self._id(relation)

        self.create_schema(relation)

        if self._catalog.table_exists(target_id):
            # Single commit: old snapshots remain until snapshot expiry,
            # unlike the previous purge-and-rename.
            def replace(tbl: Table) -> None:
                with tbl.transaction() as transaction:
                    _sync_schema(transaction, tbl.schema(), arrow_schema)
                    _sync_partition_spec(transaction, partition_by)
                    _replace_in_single_snapshot(transaction, tbl.io, data, model_config)

            self._commit(target_id, replace)
        else:
            # Not create_table_transaction: Databricks Unity Catalog does not vend
            # storage credentials on a stage-create response, so writes to the
            # staged table fail. Create first (creates an empty, visible table),
            # then write all data in one commit.
            self._table_cache[target_id] = self._catalog.create_table(
                target_id, schema=arrow_schema
            )

            def append(tbl: Table) -> None:
                with tbl.transaction() as transaction:
                    _sync_partition_spec(transaction, partition_by)
                    _append_in_single_snapshot(transaction, tbl.io, data, model_config)

            self._commit(target_id, append)

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        namespace = (self.catalog_name(schema_relation.schema),)
        from pyiceberg.exceptions import NoSuchNamespaceError

        try:
            tables = self._catalog.list_tables(namespace)
        except NoSuchNamespaceError:
            return []
        return [
            schema_relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=self.dbt_name(table_id[-1]),
                type=RelationType.Table,
                catalog=schema_relation.catalog,
            )
            for table_id in tables
        ]

    def truncate_relation(
        self, relation: PolarsRelation, model_config: dict | None = None
    ) -> None:

        logger.debug(
            f"Truncating table {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        self._commit(self._id(relation), lambda tbl: tbl.delete())

    def append_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        data = _cast_unsigned_to_signed(data)

        def append(tbl: Table) -> None:
            with tbl.transaction() as transaction:
                if allow_schema_evolution:
                    arrow_schema = (
                        data.collect_schema().to_arrow()
                        if isinstance(data, pl.LazyFrame)
                        else data.to_arrow().schema
                    )
                    with transaction.update_schema() as update:
                        update.union_by_name(arrow_schema)
                _append_in_single_snapshot(transaction, tbl.io, data, model_config)

        self._commit(self._id(relation), append)

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None = None,
        incremental_predicates: list[str] | None = None,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        df = _cast_unsigned_to_signed(df)
        missing_keys = [k for k in keys if k not in df.columns]
        if missing_keys:
            raise DbtRuntimeError(
                f"Unique key column(s) not found in model: {', '.join(missing_keys)}"
            )

        if incremental_predicates:
            raise DbtRuntimeError(
                "IcebergCatalog does not support incremental_predicates for merge"
            )
        keys = list(dict.fromkeys(keys))  # deduplicate, preserve order
        identifier = self._id(relation)
        if allow_schema_evolution:

            def evolve_schema(tbl: Table) -> None:
                with tbl.update_schema() as update:
                    update.union_by_name(df.to_arrow().schema)

            self._commit(identifier, evolve_schema)
        if df.is_empty():
            return
        if except_cols is None:

            def merge(tbl: Table) -> None:
                with tbl.transaction() as transaction:
                    overwrite_keys_and_append(tbl, transaction, tbl.io, keys, df, df)

            self._commit(identifier, merge)
            self._invalidate_table_cache(identifier)
            return

        except_set = set(except_cols)
        update_cols = [c for c in df.columns if c not in keys and c not in except_set]

        def merge_partially(tbl: Table) -> None:
            counter = itertools.count(0)
            with tbl.transaction() as transaction:
                with transaction.update_snapshot().overwrite() as snapshot_writer:
                    matched_existing = delete_rows_by_keys(
                        tbl, transaction, snapshot_writer, tbl.io, keys, df, counter
                    )
                    matched_keys = matched_existing.select(keys)
                    updated_rows = _apply_partial_update(
                        matched_existing,
                        df.join(matched_keys, on=keys, how="semi"),
                        keys,
                        update_cols,
                    )
                    incoming_new_rows = df.join(matched_keys, on=keys, how="anti")
                    _write_data_files(
                        snapshot_writer,
                        transaction,
                        tbl.io,
                        pl.concat([updated_rows, incoming_new_rows]),
                        {},
                        counter=counter,
                    )

        self._commit(identifier, merge_partially)
        self._invalidate_table_cache(identifier)

    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
        model_config: dict | None = None,
    ) -> None:
        if incremental_predicates:
            raise DbtRuntimeError(
                "IcebergCatalog does not support incremental_predicates"
            )
        if df.is_empty():
            return

        def delete_matched(tbl: Table) -> None:
            with tbl.transaction() as transaction:
                overwrite_keys_and_append(
                    tbl, transaction, tbl.io, keys, df, df.clear()
                )

        self._commit(self._id(relation), delete_matched)
        self._invalidate_table_cache(self._id(relation))

    def apply_snapshot_delta(
        self,
        relation: PolarsRelation,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str = "dbt_scd_id",
        model_config: dict | None = None,
    ) -> None:
        if rows_to_close.is_empty() and rows_to_insert.is_empty():
            return

        all_new = _cast_unsigned_to_signed(pl.concat([rows_to_close, rows_to_insert]))

        def apply_delta(tbl: Table) -> None:
            with tbl.transaction() as transaction:
                if rows_to_close.is_empty():
                    _append_in_single_snapshot(transaction, tbl.io, rows_to_insert, {})
                else:
                    overwrite_keys_and_append(
                        tbl, transaction, tbl.io, [scd_id_col], rows_to_close, all_new
                    )

        self._commit(self._id(relation), apply_delta)
        self._invalidate_table_cache(self._id(relation))

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        # Skipped when unchanged, e.g. after a table was replaced in place
        if self._load_table(self._id(relation)).properties.get("comment") == comment:
            return

        def set_comment(tbl: Table) -> None:
            with tbl.transaction() as tx:
                tx.set_properties(comment=comment)

        self._commit(self._id(relation), set_comment)

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        tbl = self._load_table(self._id(relation))
        return tbl.properties.get("comment")

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        current = {
            field.name: field.doc
            for field in self._load_table(self._id(relation)).schema().fields
        }
        changed = {
            column: comment
            for column, comment in comments.items()
            if current.get(column) != comment
        }
        if not changed:
            return

        def set_comments(tbl: Table) -> None:
            with tbl.update_schema() as update:
                for column, comment in changed.items():
                    update.update_column(column, doc=comment)

        self._commit(self._id(relation), set_comments)

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        tbl = self._load_table(self._id(relation))
        return {
            field.name: field.doc
            for field in tbl.schema().fields
            if field.doc is not None
        }
