from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.catalogs.icebergCatalog import (
    IcebergCatalog,
    IcebergCatalogConfig,
)
from dbt.adapters.polars.catalogs.localCatalog import LocalCatalog, LocalCatalogConfig

CATALOG_CONFIG_REGISTRY: dict[str, type[CatalogConfig]] = {
    "local": LocalCatalogConfig,
    "iceberg": IcebergCatalogConfig,
}
CATALOG_REGISTRY: dict[str, type[BaseCatalog]] = {
    "local": LocalCatalog,
    "iceberg": IcebergCatalog,
}
