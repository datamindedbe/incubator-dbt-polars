"""Native Iceberg tables through OneLake's Iceberg REST endpoint.

Imports pyiceberg, so only import this module once an Iceberg table is used.
"""

import polars as pl
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential
from dbt.adapters.polars.catalogs import (
    IcebergCatalog,
    IcebergCatalogConfig,
    PolarsRelation,
)
from pyiceberg.catalog.rest.auth import AuthManager

from dbt_polars_catalog_fabric.config import FabricCatalogConfig
from dbt_polars_catalog_fabric.onelake import (
    ONELAKE_BLOB_HOST,
    ONELAKE_ICEBERG_URI,
    OneLakeClient,
)


class OneLakeAuthManager(AuthManager):
    """Authenticates pyiceberg with the Azure credential of the catalog."""

    def __init__(self, onelake: OneLakeClient) -> None:
        self.onelake = onelake

    def auth_header(self) -> str:
        return f"Bearer {self.onelake.storage_token()}"


class FabricIcebergCatalog(IcebergCatalog):
    """Table operations only: FabricCatalog handles schemas, listing and drops."""

    def __init__(self, config: FabricCatalogConfig, onelake: OneLakeClient) -> None:
        self.onelake = onelake
        super().__init__(
            IcebergCatalogConfig(
                name=config.name,
                type="iceberg",
                schema=config.schema,
                pyiceberg_type="rest",
                uri=ONELAKE_ICEBERG_URI,
                warehouse=onelake.iceberg_warehouse(),
                auth={
                    "type": "custom",
                    "impl": f"{__name__}.{OneLakeAuthManager.__name__}",
                    "custom": {"onelake": onelake},
                },
                # Without the host, pyiceberg's ADLS client targets
                # onelake.blob.core.windows.net, an unrelated storage account.
                **{
                    "adls.account-name": "onelake",
                    "adls.account-host": ONELAKE_BLOB_HOST,
                    "adls.credential": AsyncDefaultAzureCredential(
                        **config.credential_kwargs
                    ),
                    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                },
            ),
            project_root="",
        )

    def create_schema(self, relation: PolarsRelation) -> None:
        # OneLake's Iceberg catalog can't create namespaces; schemas are directories
        self.onelake.create_schema(relation)

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return pl.scan_iceberg(
            self._load_table(self._id(relation)),
            storage_options={
                "bearer_token": self.onelake.storage_token(),
                "use_fabric_endpoint": "true",
            },
        )
