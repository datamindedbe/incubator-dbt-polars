from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import CatalogConfig
from dbt.adapters.polars.catalogs.formats import FILE_FORMATS
from dbt.adapters.polars.catalogs.storageCatalog import StorageCatalog
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable

logger = AdapterLogger("polars")

_FILE_FORMATS = frozenset(FILE_FORMATS)


class AzureBlobStorageCatalogConfig(CatalogConfig):
    """Profile config for an Azure Blob Storage catalog.

    Auth priority: azure_storage_token > sas_token > account_key > DefaultAzureCredential.
    All remaining kwargs are forwarded to DefaultAzureCredential, enabling fields
    like managed_identity_client_id or exclude_cli_credential.

    When schemas_as_containers=True the dbt schema maps to an Azure container and
    the container field is not used. list_schemas returns the containers in the
    storage account.
    """

    def __init__(
        self,
        name: str,
        type: str,
        account_name: str,
        container: str = "",
        prefix: str = "",
        schemas_as_containers: bool = False,
        azure_storage_token: str | None = None,
        sas_token: str | None = None,
        account_key: str | None = None,
        **dac_kwargs: object,
    ) -> None:
        self.name = name
        self.type = type
        self.account_name = account_name
        self.container = container
        self.prefix = prefix.strip("/")
        self.schemas_as_containers = schemas_as_containers
        self.azure_storage_token = azure_storage_token
        self.sas_token = sas_token
        self.account_key = account_key
        self.dac_kwargs = dac_kwargs

    def unique_field(self) -> str:
        if self.schemas_as_containers:
            return f"{self.account_name}/{self.prefix}"
        return f"{self.account_name}/{self.container}/{self.prefix}"

    def connection_keys(self) -> tuple[str, ...]:
        if self.schemas_as_containers:
            return ("name", "account_name", "prefix")
        return ("name", "account_name", "container", "prefix")


# TODO: investigate better caching of token & blobserviceclient
class AzureBlobStorageCatalog(StorageCatalog):
    config: AzureBlobStorageCatalogConfig

    def __init__(
        self, config: AzureBlobStorageCatalogConfig, project_root: str
    ) -> None:
        try:
            import azure.identity  # noqa: F401
            import azure.storage.blob  # noqa: F401
        except ImportError as exc:
            raise DbtRuntimeError(
                "The azure extra is required for AzureBlobStorageCatalog. "
                "Install it with: pip install 'dbt-polars[azure]'"
            ) from exc
        super().__init__(config, project_root)
        self._cached_token = None  # AccessToken | None
        self._service_client = None  # BlobServiceClient | None

    # ── URI and credential helpers ────────────────────────────────────────────

    def _blob_prefix(self, *parts: str) -> str:
        """Join the catalog prefix with additional path parts."""
        components = [self.config.prefix] if self.config.prefix else []
        components.extend(p.strip("/") for p in parts if p)
        return "/".join(components)

    def _container_name(self, schema: str) -> str:
        """Return the container to use for a given schema."""
        return schema if self.config.schemas_as_containers else self.config.container

    def _object_path(self, schema: str, *parts: str) -> str:
        """Return the blob path within the container for the given schema + parts."""
        if self.config.schemas_as_containers:
            return self._blob_prefix(*parts)
        return self._blob_prefix(schema, *parts)

    def _account_url(self) -> str:
        return f"https://{self.config.account_name}.blob.core.windows.net"

    def _get_uri(self, relation: PolarsRelation) -> str:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        container = self._container_name(schema)
        path = self._object_path(schema, identifier)
        if relation.file_format != "delta":
            path = f"{path}.{relation.file_format}"
        return f"az://{container}/{path}"

    def _get_storage_options(self, _uri: str) -> dict[str, str]:
        opts: dict[str, str] = {"account_name": self.config.account_name}
        if self.config.azure_storage_token:
            opts["bearer_token"] = self.config.azure_storage_token
        elif self.config.sas_token:
            opts["sas_token"] = self.config.sas_token
        elif self.config.account_key:
            opts["account_key"] = self.config.account_key
        else:
            import time

            from azure.identity import DefaultAzureCredential

            if (
                self._cached_token is None
                or self._cached_token.expires_on < time.time() + 300
            ):
                self._cached_token = DefaultAzureCredential(
                    **self.config.dac_kwargs
                ).get_token("https://storage.azure.com/.default")
            opts["bearer_token"] = self._cached_token.token
        return opts

    def _remove_files_with_prefix(self, container: str, prefix: str) -> None:
        """Delete all blobs under prefix in container, deepest-first for HNS compatibility."""
        client = self._get_container_client(container)
        blobs = sorted(
            client.list_blobs(name_starts_with=prefix),
            key=lambda b: b.name.rstrip("/").count("/"),
            reverse=True,
        )
        for blob in blobs:
            client.delete_blob(blob.name)
        # In HNS accounts the directory entry persists after its contents are deleted.
        try:
            client.delete_blob(prefix.rstrip("/"))
        except Exception:
            pass

    def _get_service_client(self):
        if self._service_client is not None:
            return self._service_client

        from azure.storage.blob import BlobServiceClient

        url = self._account_url()
        if self.config.sas_token:
            service = BlobServiceClient(f"{url}?{self.config.sas_token}")
        elif self.config.account_key:
            service = BlobServiceClient(url, credential=self.config.account_key)
        elif self.config.azure_storage_token:
            import time

            from azure.core.credentials import AccessToken

            token_str = self.config.azure_storage_token

            class _StaticToken:
                def get_token(self, *_args, **_kwargs):
                    return AccessToken(token_str, int(time.time()) + 3600)

            service = BlobServiceClient(url, credential=_StaticToken())
        else:
            from azure.identity import DefaultAzureCredential

            service = BlobServiceClient(
                url, credential=DefaultAzureCredential(**self.config.dac_kwargs)
            )
        self._service_client = service
        return self._service_client

    def _get_container_client(self, container: str | None = None):
        name = container or self.config.container
        return self._get_service_client().get_container_client(name)

    # ── schema management ─────────────────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        if self.config.schemas_as_containers:
            logger.debug(f"create_schema: creating container {schema}")
            self._get_container_client(schema).create_container()
        else:
            logger.debug(
                f"create_schema (no-op for Azure): {relation.catalog}/{schema}"
            )

    def drop_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        container = self._container_name(schema)
        prefix = self._object_path(schema) + "/"
        logger.debug(f"Dropping schema {relation.catalog}/{schema}")
        self._remove_files_with_prefix(container, prefix)

    def list_schemas(self) -> list[str]:
        if self.config.schemas_as_containers:
            return [c["name"] for c in self._get_service_client().list_containers()]
        base = (self.config.prefix + "/") if self.config.prefix else ""
        schemas: set[str] = set()
        for item in self._get_container_client().walk_blobs(
            name_starts_with=base, delimiter="/"
        ):
            if hasattr(item, "prefix"):
                schema = item.name.rstrip("/").split("/")[-1]
                schemas.add(schema)
        return list(schemas)

    def table_exists(self, relation: PolarsRelation) -> bool:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        container = self._container_name(schema)
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            return DeltaTable.is_deltatable(uri, storage_options=opts)
        blob_path = f"{self._object_path(schema, identifier)}.{relation.file_format}"
        return self._get_container_client(container).get_blob_client(blob_path).exists()

    def drop_relation(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        container = self._container_name(schema)
        logger.debug(
            f"Dropping table if exists {relation.catalog}/{schema}/{identifier}"
        )
        client = self._get_container_client(container)
        if relation.file_format == "delta":
            self._remove_files_with_prefix(
                container, self._object_path(schema, identifier) + "/"
            )
        else:
            blob_path = (
                f"{self._object_path(schema, identifier)}.{relation.file_format}"
            )
            blob = client.get_blob_client(blob_path)
            if blob.exists():
                blob.delete_blob()

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        schema = schema_relation.schema or ""
        container = self._container_name(schema)
        prefix = self._object_path(schema) + "/"
        client = self._get_container_client(container)
        relations: list[PolarsRelation] = []

        for item in client.walk_blobs(name_starts_with=prefix, delimiter="/"):
            if hasattr(item, "prefix"):
                dir_name = item.name.rstrip("/").split("/")[-1]
                uri = f"az://{container}/{self._object_path(schema, dir_name)}"
                if DeltaTable.is_deltatable(
                    uri, storage_options=self._get_storage_options(uri)
                ):
                    relations.append(
                        schema_relation.create(
                            database=schema_relation.database,
                            schema=schema,
                            identifier=dir_name,
                            type=RelationType.Table,
                            catalog=schema_relation.catalog,
                            file_format="delta",
                        )
                    )
            else:
                filename = item.name.split("/")[-1]
                stem, _, ext = filename.rpartition(".")
                if ext in _FILE_FORMATS:
                    relations.append(
                        schema_relation.create(
                            database=schema_relation.database,
                            schema=schema,
                            identifier=stem,
                            type=RelationType.Table,
                            catalog=schema_relation.catalog,
                            file_format=ext,
                        )
                    )

        return relations
