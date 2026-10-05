from dbt_polars_catalog_fabric.catalog import FabricCatalog
from dbt_polars_catalog_fabric.config import FabricCatalogConfig

config_class = FabricCatalogConfig
create_catalog = FabricCatalog

__all__ = [
    "FabricCatalog",
    "FabricCatalogConfig",
    "config_class",
    "create_catalog",
]
