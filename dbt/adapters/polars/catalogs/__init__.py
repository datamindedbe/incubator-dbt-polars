from dbt.adapters.polars.catalogs.azureCatalog import (
    AzureBlobStorageCatalog,
    AzureBlobStorageCatalogConfig,
)
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.catalogs.icebergCatalog import (
    IcebergCatalog,
    IcebergCatalogConfig,
)
from dbt.adapters.polars.catalogs.localCatalog import LocalCatalog, LocalCatalogConfig
from dbt.adapters.polars.catalogs.s3Catalog import AWSS3Catalog, S3CatalogConfig

CATALOG_CONFIG_REGISTRY: dict[str, type[CatalogConfig]] = {
    "local": LocalCatalogConfig,
    "iceberg": IcebergCatalogConfig,
    "azure": AzureBlobStorageCatalogConfig,
    "s3": S3CatalogConfig,
}
CATALOG_REGISTRY: dict[str, type[BaseCatalog]] = {
    "local": LocalCatalog,
    "iceberg": IcebergCatalog,
    "azure": AzureBlobStorageCatalog,
    "s3": AWSS3Catalog,
}
