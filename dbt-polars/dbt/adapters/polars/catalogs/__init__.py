from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.catalogs.localCatalog import LocalCatalogConfig, LocalCatalog

CATALOG_CONFIG_REGISTRY: dict[str, CatalogConfig] = {"local": LocalCatalogConfig}
CATALOG_REGISTRY: dict[str, BaseCatalog] = {"local": LocalCatalog}
