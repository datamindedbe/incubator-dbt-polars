from dbt.adapters.polars.catalogs import PolarsRelation, StorageCatalog

from dbt_polars_catalog_fabric.config import FabricCatalogConfig
from dbt_polars_catalog_fabric.onelake import OneLakeClient


class FabricDeltaCatalog(StorageCatalog):
    """Writes Delta tables to `Tables/<schema>/<table>` of the Lakehouse."""

    config: FabricCatalogConfig

    def __init__(self, config: FabricCatalogConfig, onelake: OneLakeClient) -> None:
        super().__init__(config)
        self.onelake = onelake

    def get_uri(self, relation: PolarsRelation) -> str:
        return self.onelake.table_uri(relation)

    def get_storage_options(self, uri: str, read_only: bool = False) -> dict[str, str]:
        return {**self.onelake.storage_options(), "timeout": "120s"}

    # ── schema management and listing ──────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        self.onelake.create_schema(relation)

    def drop_schema(self, relation: PolarsRelation) -> None:
        self.onelake.drop_schema(relation)

    def list_schemas(self) -> list[str]:
        return self.onelake.list_schemas()

    def table_exists(self, relation: PolarsRelation) -> bool:
        return self.onelake.table_format(relation) == "delta"

    def drop_relation(self, relation: PolarsRelation) -> None:
        self.onelake.drop_table(relation)

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        return self.onelake.list_relations(schema_relation)
