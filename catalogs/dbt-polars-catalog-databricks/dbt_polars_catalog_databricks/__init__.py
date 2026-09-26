from dbt.adapters.polars.catalogs import BaseCatalog

from dbt_polars_catalog_databricks.config import DatabricksCatalogConfig
from dbt_polars_catalog_databricks.delta import DatabricksDeltaCatalog

CATALOG_BY_TABLE_FORMAT: dict[str, type[BaseCatalog]] = {
    "delta": DatabricksDeltaCatalog,
}

config_class = DatabricksCatalogConfig


def create_catalog(config: DatabricksCatalogConfig, project_root: str) -> BaseCatalog:
    return CATALOG_BY_TABLE_FORMAT[config.table_format](config, project_root)


__all__ = [
    "CATALOG_BY_TABLE_FORMAT",
    "DatabricksCatalogConfig",
    "DatabricksDeltaCatalog",
    "config_class",
    "create_catalog",
]
