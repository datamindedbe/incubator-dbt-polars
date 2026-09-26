from dbt_polars_catalog_s3.catalog import AWSS3Catalog, S3CatalogConfig

config_class = S3CatalogConfig
create_catalog = AWSS3Catalog

__all__ = ["AWSS3Catalog", "S3CatalogConfig", "config_class", "create_catalog"]
