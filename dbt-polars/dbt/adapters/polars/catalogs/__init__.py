from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.catalogs.localCatalog import LocalCatalog, LocalCatalogConfig
from dbt.adapters.polars.catalogs.s3Catalog import S3Catalog, S3CatalogConfig

CATALOG_CONFIG_REGISTRY: dict[str, type[CatalogConfig]] = {
    "local": LocalCatalogConfig,
    "s3": S3CatalogConfig,
}
CATALOG_REGISTRY: dict[str, type[BaseCatalog]] = {
    "local": LocalCatalog,
    "s3": S3Catalog,
}

__all__ = [
    "BaseCatalog",
    "CatalogConfig",
    "LocalCatalog",
    "LocalCatalogConfig",
    "S3Catalog",
    "S3CatalogConfig",
    "CATALOG_CONFIG_REGISTRY",
    "CATALOG_REGISTRY",
]
