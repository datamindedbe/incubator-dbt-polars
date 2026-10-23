import threading

import polars as pl
from dbt.adapters.polars.catalogs import BaseCatalog, PolarsRelation, file_format_of
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_databricks.config import (
    SUPPORTED_TABLE_FORMATS,
    DatabricksCatalogConfig,
)
from dbt_polars_catalog_databricks.delta import DatabricksDeltaCatalog
from dbt_polars_catalog_databricks.unity_catalog import (
    UnityCatalogClient,
    UnityCatalogTable,
)


class DatabricksCatalog(BaseCatalog):
    """
    Integrates with unity catalog through the Rest API. Data operations are send to the
    delta catalog (storageCatalog) or iceberg catalog based on the table format.
    """

    config: DatabricksCatalogConfig
    model_config_keys = frozenset({"table_format"})

    def __init__(self, config: DatabricksCatalogConfig, project_root: str) -> None:
        super().__init__(config)
        self.unity_catalog = UnityCatalogClient(config)
        self.delta_catalog = DatabricksDeltaCatalog(config, self.unity_catalog)
        self.loaded_iceberg_catalog = None
        self.iceberg_catalog_lock = threading.Lock()

    @property
    def iceberg_catalog(self):
        if self.loaded_iceberg_catalog is None:
            with self.iceberg_catalog_lock:
                if self.loaded_iceberg_catalog is None:
                    self.loaded_iceberg_catalog = self.create_iceberg_catalog()
        return self.loaded_iceberg_catalog

    def create_iceberg_catalog(self):
        try:
            from dbt_polars_catalog_databricks.iceberg import DatabricksIcebergCatalog
        except ModuleNotFoundError as exc:
            if not (exc.name or "").startswith("pyiceberg"):
                raise
            raise DbtRuntimeError(
                "Iceberg tables in the databricks catalog require pyiceberg. "
                "Install it with: pip install 'dbt-polars[databricks,iceberg]'"
            ) from exc
        return DatabricksIcebergCatalog(
            self.config, self.unity_catalog.workspace_client
        )

    def catalog_for_format(self, table_format: str) -> BaseCatalog:
        if table_format == "iceberg":
            return self.iceberg_catalog
        return self.delta_catalog

    def requested_format(self, model_config: dict) -> str:
        table_format = (
            model_config.get("table_format") or self.config.default_table_format
        )
        if table_format not in SUPPORTED_TABLE_FORMATS:
            raise DbtRuntimeError(
                f"table_format '{table_format}' is not supported by the databricks "
                f"catalog, use one of: {', '.join(SUPPORTED_TABLE_FORMATS)}."
            )
        return table_format

    def existing_table(self, relation: PolarsRelation) -> UnityCatalogTable:
        table = self.unity_catalog.registered_table(relation)
        if table is None:
            raise DbtRuntimeError(f"Table {relation} does not exist")
        return table

    def catalog_for_existing(self, relation: PolarsRelation) -> BaseCatalog:
        """The catalog for the format `relation` has in Unity Catalog."""
        return self.catalog_for_format(self.existing_table(relation).table_format)

    def validate_incremental_relation(
        self, relation: PolarsRelation, model_config: dict
    ) -> None:
        table_format = self.existing_table(relation).table_format
        requested = self.requested_format(model_config)
        if requested != table_format:
            raise DbtRuntimeError(
                f"{relation} is a {table_format} table, but its table_format "
                f"is {requested}. Rebuild it with --full-refresh."
            )

    # ── schema management and listing ──────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        self.unity_catalog.create_schema(relation)

    def drop_schema(self, relation: PolarsRelation) -> None:
        self.unity_catalog.drop_schema(relation, self.drop_table)

    def list_schemas(self) -> list[str]:
        return self.unity_catalog.list_schemas()

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        return self.unity_catalog.list_relations(schema_relation)

    def table_exists(self, relation: PolarsRelation) -> bool:
        return self.unity_catalog.registered_table(relation) is not None

    # ── drop ─────────────────────────────────────────────────────────────────────

    def drop_relation(self, relation: PolarsRelation) -> None:
        table = self.unity_catalog.registered_table(relation)
        if table is not None:
            self.drop_table(table)

    def drop_table(self, table: UnityCatalogTable) -> None:
        if table.table_format != "iceberg":
            self.delta_catalog.drop_table(table)
            return
        # Unity Catalog removes a managed table's data itself.
        self.unity_catalog.delete_table(table.full_name)
        if self.loaded_iceberg_catalog is not None:
            _, schema, identifier = table.full_name.split(".")
            self.loaded_iceberg_catalog._invalidate_table_cache((schema, identifier))

    # ── data ───────────────────────────────────────────────────────────────────

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        if file_format_of(relation) != "delta":
            raise DbtRuntimeError(
                "file_format is not a supported config for the databricks "
                f"catalog (got {file_format_of(relation)!r} for {relation}) - "
                "use table_format to choose between delta and iceberg."
            )
        model_config = model_config or {}
        table_format = self.requested_format(model_config)
        existing = self.unity_catalog.registered_table(relation)
        if existing is not None and existing.table_format != table_format:
            self.drop_table(existing)
            existing = None

        self.create_schema(relation)
        self.catalog_for_format(table_format).write_relation(
            relation, data, partition_by, model_config
        )
        if table_format == "iceberg" and existing is None:
            # Created through Iceberg REST, so Unity Catalog's info isn't cached yet
            self.unity_catalog.forget_table(
                self.unity_catalog.table_full_name(relation)
            )

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return self.catalog_for_existing(relation).get_relation(relation)

    def get_partition_columns(self, relation: PolarsRelation) -> list[str]:
        return self.catalog_for_existing(relation).get_partition_columns(relation)

    def truncate_relation(
        self, relation: PolarsRelation, model_config: dict | None = None
    ) -> None:
        self.catalog_for_existing(relation).truncate_relation(relation, model_config)

    def append_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        self.catalog_for_existing(relation).append_relation(
            relation, data, allow_schema_evolution, model_config
        )

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
        self.catalog_for_existing(relation).merge_relation(
            relation,
            df,
            keys,
            except_cols,
            incremental_predicates,
            allow_schema_evolution,
            model_config,
        )

    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
        model_config: dict | None = None,
    ) -> None:
        self.catalog_for_existing(relation).delete_matched_relation(
            relation, df, keys, incremental_predicates, model_config
        )

    def apply_snapshot_delta(
        self,
        relation: PolarsRelation,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str = "dbt_scd_id",
        model_config: dict | None = None,
    ) -> None:
        self.catalog_for_existing(relation).apply_snapshot_delta(
            relation, rows_to_close, rows_to_insert, scd_id_col, model_config
        )

    # ── comments ─────────────────────────────────────────────────────────────────

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        # The table's own metadata is what docs generate reads back; Unity
        # Catalog's comment fields are only updated with persist_docs_http_path.
        self.catalog_for_existing(relation).set_relation_comment(relation, comment)
        self.unity_catalog.set_relation_comment(relation, comment)

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        self.catalog_for_existing(relation).set_column_comments(relation, comments)
        self.unity_catalog.set_column_comments(relation, comments)

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        return self.catalog_for_existing(relation).get_relation_comment(relation)

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        return self.catalog_for_existing(relation).get_column_comments(relation)
