from dbt_polars_catalog_azure.catalog import (
    AzureBlobStorageCatalog,
    AzureBlobStorageCatalogConfig,
)

config_class = AzureBlobStorageCatalogConfig
create_catalog = AzureBlobStorageCatalog

__all__ = [
    "AzureBlobStorageCatalog",
    "AzureBlobStorageCatalogConfig",
    "config_class",
    "create_catalog",
]
