# Reference implementation used by the template's CI in place of the generated stubs
from dataclasses import dataclass

from dbt_polars_catalog_local import LocalCatalog, LocalCatalogConfig


@dataclass
class SampleCatalogConfig(LocalCatalogConfig):
    pass


class SampleCatalog(LocalCatalog):
    pass
