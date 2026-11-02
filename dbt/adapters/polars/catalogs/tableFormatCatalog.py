import threading
from abc import abstractmethod

from dbt_common.exceptions import DbtRuntimeError

import polars as pl
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.relation import PolarsRelation

SUPPORTED_TABLE_FORMATS = ("delta", "iceberg")


class TableFormatCatalog(BaseCatalog):
    """A catalog holding Delta and Iceberg tables side by side.

    Table data goes to the Delta or Iceberg catalog: the model's `table_format`
    decides for new tables, the format the table already has for existing ones.
    The Iceberg catalog, and with it pyiceberg, is only loaded once an Iceberg
    table is used. Subclasses implement schemas, listing, dropping and how the
    format of an existing table is found.
    """

    model_config_keys = frozenset({"table_format"})
    # The pip extras that install pyiceberg for this catalog, e.g. "databricks,iceberg"
    iceberg_extras: str = "iceberg"

    def __init__(
        self,
        config: CatalogConfig,
        delta_catalog: BaseCatalog,
        default_table_format: str,
    ) -> None:
        super().__init__(config)
        self.delta_catalog = delta_catalog
        self.default_table_format = default_table_format
        self.loaded_iceberg_catalog: BaseCatalog | None = None
        self.iceberg_catalog_lock = threading.Lock()

    # ── hooks ──────────────────────────────────────────────────────────────────

    @abstractmethod
    def existing_table_format(self, relation: PolarsRelation) -> str | None:
        """The format of the existing table, None when there is no table."""

    @abstractmethod
    def drop_existing_table(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def load_iceberg_catalog(self) -> BaseCatalog:
        """Import the Iceberg catalog module here, so pyiceberg loads lazily."""

    def after_create(self, relation: PolarsRelation, table_format: str) -> None:
        """Called after write_relation created a new table."""

    def push_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        pass

    def push_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        pass

    # ── format routing ─────────────────────────────────────────────────────────

    @property
    def iceberg_catalog(self) -> BaseCatalog:
        if self.loaded_iceberg_catalog is None:
            with self.iceberg_catalog_lock:
                if self.loaded_iceberg_catalog is None:
                    self.loaded_iceberg_catalog = self.create_iceberg_catalog()
        return self.loaded_iceberg_catalog

    def create_iceberg_catalog(self) -> BaseCatalog:
        try:
            return self.load_iceberg_catalog()
        except ModuleNotFoundError as exc:
            if not (exc.name or "").startswith("pyiceberg"):
                raise
            raise DbtRuntimeError(
                f"Iceberg tables in the {self.config.type} catalog require "
                "pyiceberg. Install it with: "
                f"pip install 'dbt-polars[{self.iceberg_extras}]'"
            ) from exc

    def forget_iceberg_table(self, relation: PolarsRelation) -> None:
        if self.loaded_iceberg_catalog is not None and relation.identifier:
            self.loaded_iceberg_catalog._invalidate_table_cache(  # type: ignore[attr-defined]
                (relation.schema, relation.identifier)
            )

    def catalog_for_format(self, table_format: str) -> BaseCatalog:
        if table_format == "iceberg":
            return self.iceberg_catalog
        return self.delta_catalog

    def requested_format(self, model_config: dict) -> str:
        table_format = model_config.get("table_format") or self.default_table_format
        if table_format not in SUPPORTED_TABLE_FORMATS:
            raise DbtRuntimeError(
                f"table_format '{table_format}' is not supported by the "
                f"{self.config.type} catalog, use one of: "
                f"{', '.join(SUPPORTED_TABLE_FORMATS)}."
            )
        return table_format

    def existing_format(self, relation: PolarsRelation) -> str:
        table_format = self.existing_table_format(relation)
        if table_format is None:
            raise DbtRuntimeError(f"Table {relation} does not exist")
        return table_format

    def catalog_for_existing(self, relation: PolarsRelation) -> BaseCatalog:
        return self.catalog_for_format(self.existing_format(relation))

    def validate_incremental_relation(
        self, relation: PolarsRelation, model_config: dict
    ) -> None:
        if relation.file_format is not None:
            raise self.file_format_not_supported()
        table_format = self.existing_format(relation)
        requested = self.requested_format(model_config)
        if requested != table_format:
            raise DbtRuntimeError(
                f"{relation} is a {table_format} table, but its table_format "
                f"is {requested}. Rebuild it with --full-refresh."
            )

    # ── existence and drop ─────────────────────────────────────────────────────

    def table_exists(self, relation: PolarsRelation) -> bool:
        return self.existing_table_format(relation) is not None

    def drop_relation(self, relation: PolarsRelation) -> None:
        if self.existing_table_format(relation) is not None:
            self.drop_existing_table(relation)

    # ── data ───────────────────────────────────────────────────────────────────

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        if relation.file_format is not None:
            raise self.file_format_not_supported()
        model_config = model_config or {}
        table_format = self.requested_format(model_config)
        existing_format = self.existing_table_format(relation)
        if existing_format is not None and existing_format != table_format:
            self.drop_existing_table(relation)
            existing_format = None

        self.create_schema(relation)
        self.catalog_for_format(table_format).write_relation(
            relation, data, partition_by, model_config
        )
        if existing_format is None:
            self.after_create(relation, table_format)

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
        if relation.file_format is not None:
            raise self.file_format_not_supported()
        self.catalog_for_existing(relation).apply_snapshot_delta(
            relation, rows_to_close, rows_to_insert, scd_id_col, model_config
        )

    # ── comments ───────────────────────────────────────────────────────────────

    def set_comments(
        self,
        relation: PolarsRelation,
        relation_comment: str | None,
        column_comments: dict[str, str],
    ) -> None:
        self.catalog_for_existing(relation).set_comments(
            relation, relation_comment, column_comments
        )
        if relation_comment is not None:
            self.push_relation_comment(relation, relation_comment)
        if column_comments:
            self.push_column_comments(relation, column_comments)

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        self.set_comments(relation, comment, {})

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        self.set_comments(relation, None, comments)

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        return self.catalog_for_existing(relation).get_relation_comment(relation)

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        return self.catalog_for_existing(relation).get_column_comments(relation)
