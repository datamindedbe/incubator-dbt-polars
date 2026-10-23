from dbt.adapters.polars.catalogs import (
    BaseCatalog,
    PolarsRelation,
    TableFormatCatalog,
)

from dbt_polars_catalog_fabric.config import FabricCatalogConfig
from dbt_polars_catalog_fabric.delta import FabricDeltaCatalog
from dbt_polars_catalog_fabric.onelake import OneLakeClient


class FabricCatalog(TableFormatCatalog):
    """Delta and Iceberg tables in a schema-enabled Fabric Lakehouse.

    Schemas, listing and drops are OneLake directory operations. Delta tables
    are written straight to OneLake, Iceberg tables through OneLake's Iceberg
    REST catalog. No Fabric compute (Spark or SQL) is used.
    """

    config: FabricCatalogConfig
    delta_catalog: FabricDeltaCatalog
    iceberg_extras = "fabric,iceberg"

    def __init__(self, config: FabricCatalogConfig, project_root: str) -> None:
        self.onelake = OneLakeClient(config)
        super().__init__(
            config,
            FabricDeltaCatalog(config, self.onelake),
            config.default_table_format,
        )

    def load_iceberg_catalog(self) -> BaseCatalog:
        from dbt_polars_catalog_fabric.iceberg import FabricIcebergCatalog

        return FabricIcebergCatalog(self.config, self.onelake)

    def existing_table_format(self, relation: PolarsRelation) -> str | None:
        return self.onelake.table_format(relation)

    def drop_existing_table(self, relation: PolarsRelation) -> None:
        self.onelake.drop_table(relation)
        self.forget_iceberg_table(relation)

    def after_write(
        self, relation: PolarsRelation, table_format: str, created: bool
    ) -> None:
        self.onelake.remember_table(relation, table_format)

    # ── schema management and listing ──────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        self.onelake.create_schema(relation)

    def drop_schema(self, relation: PolarsRelation) -> None:
        self.onelake.drop_schema(relation)
        if self.loaded_iceberg_catalog is not None:
            self.loaded_iceberg_catalog._table_cache.clear()  # type: ignore[attr-defined]

    def list_schemas(self) -> list[str]:
        return self.onelake.list_schemas()

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        return self.onelake.list_relations(schema_relation)
