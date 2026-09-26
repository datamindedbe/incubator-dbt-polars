import re
from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from importlib import import_module

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.catalogs.azureCatalog import (
    AzureBlobStorageCatalog,
    AzureBlobStorageCatalogConfig,
)
from dbt.adapters.polars.catalogs.baseCatalog import (
    BaseCatalog,
    CatalogConfig,
    get_write_options,
)
from dbt.adapters.polars.catalogs.databricksCatalog import (
    DatabricksCatalog,
    DatabricksCatalogConfig,
)
from dbt.adapters.polars.catalogs.formats import FILE_FORMATS
from dbt.adapters.polars.catalogs.icebergCatalog import (
    IcebergCatalog,
    IcebergCatalogConfig,
)
from dbt.adapters.polars.catalogs.localCatalog import LocalCatalog, LocalCatalogConfig
from dbt.adapters.polars.catalogs.s3Catalog import AWSS3Catalog, S3CatalogConfig
from dbt.adapters.polars.catalogs.storageCatalog import StorageCatalog, file_format_of
from dbt.adapters.polars.relation import PolarsRelation
from dbt.adapters.polars.utils import resolve_relative_path

__all__ = [
    "FILE_FORMATS",
    "BaseCatalog",
    "CatalogConfig",
    "CatalogPlugin",
    "PolarsRelation",
    "StorageCatalog",
    "create_catalog",
    "file_format_of",
    "get_write_options",
    "resolve_catalog_plugin",
    "resolve_relative_path",
]

EXTENSION_MODULE_PREFIX = "dbt_polars_catalog_"
CATALOG_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
CATALOG_TYPES_INSTALLED_VIA_EXTRA = frozenset({"s3", "azure", "databricks"})


@dataclass(frozen=True)
class CatalogPlugin:
    config_class: type[CatalogConfig]
    create_catalog: Callable[..., BaseCatalog]


BUILTIN_CATALOGS: dict[str, CatalogPlugin] = {
    "local": CatalogPlugin(LocalCatalogConfig, LocalCatalog),
    "iceberg": CatalogPlugin(IcebergCatalogConfig, IcebergCatalog),
    "azure": CatalogPlugin(AzureBlobStorageCatalogConfig, AzureBlobStorageCatalog),
    "s3": CatalogPlugin(S3CatalogConfig, AWSS3Catalog),
    "databricks": CatalogPlugin(DatabricksCatalogConfig, DatabricksCatalog),
}


def missing_catalog_package_message(catalog_type: str) -> str:
    if catalog_type in CATALOG_TYPES_INSTALLED_VIA_EXTRA:
        install_hint = f"pip install 'dbt-polars[{catalog_type}]'"
    else:
        distribution = "dbt-polars-catalog-" + catalog_type.replace("_", "-")
        install_hint = f"pip install {distribution}"
    return (
        f"Unknown catalog type '{catalog_type}'. Built-in types: "
        f"{', '.join(sorted(BUILTIN_CATALOGS))}. To use an extension catalog, "
        f"install its package: {install_hint}"
    )


@cache
def resolve_catalog_plugin(catalog_type: str) -> CatalogPlugin:
    """Find the catalog implementation for a profile's `type`.

    Built-in types resolve directly. Any other type `x` is loaded from the module
    `dbt_polars_catalog_x`, which must expose `config_class` and `create_catalog`.
    """
    if catalog_type in BUILTIN_CATALOGS:
        return BUILTIN_CATALOGS[catalog_type]
    if not CATALOG_TYPE_PATTERN.match(catalog_type):
        raise DbtRuntimeError(
            f"Invalid catalog type '{catalog_type}': must match "
            f"{CATALOG_TYPE_PATTERN.pattern}"
        )

    module_name = EXTENSION_MODULE_PREFIX + catalog_type
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise
        raise DbtRuntimeError(missing_catalog_package_message(catalog_type)) from exc

    config_class = getattr(module, "config_class", None)
    create_catalog = getattr(module, "create_catalog", None)
    if not (isinstance(config_class, type) and issubclass(config_class, CatalogConfig)):
        raise DbtRuntimeError(
            f"Catalog module '{module_name}' must define `config_class`, "
            "a subclass of CatalogConfig"
        )
    if not callable(create_catalog):
        raise DbtRuntimeError(
            f"Catalog module '{module_name}' must define a callable `create_catalog`"
        )
    return CatalogPlugin(config_class, create_catalog)


def create_catalog(config: CatalogConfig, project_root: str) -> BaseCatalog:
    catalog = resolve_catalog_plugin(config.type).create_catalog(config, project_root)
    if not isinstance(catalog, BaseCatalog):
        raise DbtRuntimeError(
            f"create_catalog for catalog type '{config.type}' returned "
            f"{type(catalog).__name__}, expected a BaseCatalog subclass"
        )
    return catalog
