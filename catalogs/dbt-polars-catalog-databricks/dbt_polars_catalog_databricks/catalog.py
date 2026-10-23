from dbt.adapters.polars.catalogs import (
    BaseCatalog,
    PolarsRelation,
    TableFormatCatalog,
)

from dbt_polars_catalog_databricks.config import DatabricksCatalogConfig
from dbt_polars_catalog_databricks.delta import DatabricksDeltaCatalog
from dbt_polars_catalog_databricks.unity_catalog import (
    UnityCatalogClient,
    UnityCatalogTable,
)


class DatabricksCatalog(TableFormatCatalog):
    """
    Integrates with unity catalog through the Rest API. Data operations are send to the
    delta catalog (storageCatalog) or iceberg catalog based on the table format.
    """

    config: DatabricksCatalogConfig
    delta_catalog: DatabricksDeltaCatalog
    iceberg_extras = "databricks,iceberg"

    def __init__(self, config: DatabricksCatalogConfig, project_root: str) -> None:
        self.unity_catalog = UnityCatalogClient(config)
        super().__init__(
            config,
            DatabricksDeltaCatalog(config, self.unity_catalog),
            config.default_table_format,
        )

    def load_iceberg_catalog(self) -> BaseCatalog:
        from dbt_polars_catalog_databricks.iceberg import DatabricksIcebergCatalog

        return DatabricksIcebergCatalog(
            self.config, self.unity_catalog.workspace_client
        )

    def existing_table_format(self, relation: PolarsRelation) -> str | None:
        table = self.unity_catalog.registered_table(relation)
        return table.table_format if table is not None else None

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

    # ── drop ─────────────────────────────────────────────────────────────────────

    def drop_existing_table(self, relation: PolarsRelation) -> None:
        table = self.unity_catalog.registered_table(relation)
        if table is not None:
            self.drop_table(table)

    def drop_table(self, table: UnityCatalogTable) -> None:
        if table.table_format != "iceberg":
            self.delta_catalog.drop_table(table)
            return
        # Unity Catalog removes a managed table's data itself.
        self.unity_catalog.delete_table(table.full_name)
        _, schema, identifier = table.full_name.split(".")
        self.forget_iceberg_table(
            PolarsRelation.create(schema=schema, identifier=identifier)
        )

    # ── hooks ────────────────────────────────────────────────────────────────────

    def after_write(
        self, relation: PolarsRelation, table_format: str, created: bool
    ) -> None:
        if table_format == "iceberg" and created:
            # Created through Iceberg REST, so Unity Catalog's info isn't cached yet
            self.unity_catalog.forget_table(
                self.unity_catalog.table_full_name(relation)
            )

    def push_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        # Unity Catalog's comment fields are only updated with persist_docs_http_path.
        self.unity_catalog.set_relation_comment(relation, comment)

    def push_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        self.unity_catalog.set_column_comments(relation, comments)
