from dbt_polars_catalog_local.catalog import LocalCatalog, LocalCatalogConfig

config_class = LocalCatalogConfig
create_catalog = LocalCatalog

__all__ = ["LocalCatalog", "LocalCatalogConfig", "config_class", "create_catalog"]
