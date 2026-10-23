"""Managed Iceberg tables through Unity Catalog's Iceberg REST endpoint.

Imports pyiceberg, so only import this module once an Iceberg table is used.
"""

from dbt.adapters.polars.catalogs import IcebergCatalog, IcebergCatalogConfig
from pyiceberg.catalog.rest.auth import AuthManager

from dbt_polars_catalog_databricks.config import DatabricksCatalogConfig


class DatabricksSdkAuthManager(AuthManager):
    """Authenticates pyiceberg with the databricks-sdk credentials of the catalog.

    The sdk refreshes expiring tokens (OAuth, Azure) itself.
    """

    def __init__(self, sdk_config) -> None:
        self.sdk_config = sdk_config

    def auth_header(self) -> str:
        return self.sdk_config.authenticate()["Authorization"]


class DatabricksIcebergCatalog(IcebergCatalog):
    """Table operations only: DatabricksCatalog handles schemas and listing."""

    def __init__(self, config: DatabricksCatalogConfig, workspace_client) -> None:
        sdk_config = workspace_client.config
        workspace_id_header = (
            {"header.X-Databricks-Workspace-Id": sdk_config.workspace_id}
            if sdk_config.workspace_id
            else {}
        )
        super().__init__(
            IcebergCatalogConfig(
                name=config.name,
                type="iceberg",
                schema=config.schema,
                pyiceberg_type="rest",
                uri=f"{sdk_config.host.rstrip('/')}/api/2.1/unity-catalog/iceberg-rest",
                warehouse=config.catalog_name,
                auth={
                    "type": "custom",
                    "impl": f"{__name__}.{DatabricksSdkAuthManager.__name__}",
                    "custom": {"sdk_config": sdk_config},
                },
                **workspace_id_header,
            ),
            project_root="",
        )
