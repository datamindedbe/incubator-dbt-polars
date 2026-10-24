from dbt_polars_catalog_snowflake.catalog import (
    SnowflakeCatalog,
    SnowflakeCatalogConfig,
)

config_class = SnowflakeCatalogConfig
create_catalog = SnowflakeCatalog

__all__ = [
    "SnowflakeCatalog",
    "SnowflakeCatalogConfig",
    "config_class",
    "create_catalog",
]
