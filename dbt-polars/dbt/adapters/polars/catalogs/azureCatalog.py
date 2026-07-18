from typing import TYPE_CHECKING

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import CatalogConfig
from dbt.adapters.polars.catalogs.formats import FILE_FORMATS
from dbt.adapters.polars.catalogs.storageCatalog import StorageCatalog
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable

if TYPE_CHECKING:
    from azure.core.credentials import AccessToken

logger = AdapterLogger("polars")

_FILE_FORMATS = frozenset(FILE_FORMATS)


class AzureBlobStorageCatalogConfig(CatalogConfig):
    def __init__(
        self,
        name: str,
        type: str,
        account_name: str,
        container: str = "",
        prefix: str = "",
        schemas_as_containers: bool = False,
        credentials: dict | None = None,
    ) -> None:
        self.name = name
        self.type = type
        self.account_name = account_name
        self.container = container
        self.prefix = prefix.strip("/")
        self.schemas_as_containers = schemas_as_containers
        self.credentials = credentials or {}

    def unique_field(self) -> str:
        if self.schemas_as_containers:
            return f"{self.account_name}/{self.prefix}"
        return f"{self.account_name}/{self.container}/{self.prefix}"

    def connection_keys(self) -> tuple[str, ...]:
        if self.schemas_as_containers:
            return ("name", "account_name", "prefix")
        return ("name", "account_name", "container", "prefix")


class _StaticTokenCredential:
    def __init__(self, token: str) -> None:
        self._token = token

    def get_token(self, *_args, **_kwargs):
        import time

        from azure.core.credentials import AccessToken

        return AccessToken(self._token, int(time.time()) + 3600)


class AzureBlobStorageCatalog(StorageCatalog):
    config: AzureBlobStorageCatalogConfig

    def __init__(
        self, config: AzureBlobStorageCatalogConfig, project_root: str
    ) -> None:
        try:
            import azure.identity  # noqa: F401
            import azure.storage.filedatalake  # noqa: F401
        except ImportError as exc:
            raise DbtRuntimeError(
                "The azure extra is required for AzureBlobStorageCatalog. "
                "Install it with: pip install 'dbt-polars[azure]'"
            ) from exc
        super().__init__(config, project_root)
        self._cached_token: AccessToken | None = None  # for _get_storage_options only
        self._service_client = None  # DataLakeServiceClient | None

    # ── URI and credential helpers ────────────────────────────────────────────

    def _blob_prefix(self, *parts: str) -> str:
        """Join the catalog prefix with additional path parts."""
        components = [self.config.prefix] if self.config.prefix else []
        components.extend(p.strip("/") for p in parts if p)
        return "/".join(components)

    def _container_name(self, schema: str) -> str:
        """Return the file system (container) to use for a given schema."""
        return schema if self.config.schemas_as_containers else self.config.container

    def _object_path(self, schema: str, *parts: str) -> str:
        """Return the path within the file system for the given schema + parts."""
        if self.config.schemas_as_containers:
            return self._blob_prefix(*parts)
        return self._blob_prefix(schema, *parts)

    def _dfs_url(self) -> str:
        return f"https://{self.config.account_name}.dfs.core.windows.net"

    def _get_uri(self, relation: PolarsRelation) -> str:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        container = self._container_name(schema)
        path = self._object_path(schema, identifier)
        if relation.file_format != "delta":
            path = f"{path}.{relation.file_format}"
        return f"az://{container}/{path}"

    _DELEGATED_CRED_KEYS = (
        "bearer_token",
        "account_key",
        "sas_token",
        "client_secret",
        "federated_token_file",
        "use_azure_cli",
        "use_managed_identity",
    )

    def _get_storage_options(self, _uri: str) -> dict[str, str]:
        opts: dict[str, str] = {
            "account_name": self.config.account_name,
            "timeout": "120s",
        }
        creds = self.config.credentials
        if any(k in creds for k in self._DELEGATED_CRED_KEYS):
            # object_store handles all of these natively (managed identity via
            # IMDS fallback, so the flag itself is stripped)
            opts.update(
                {k: str(v) for k, v in creds.items() if k != "use_managed_identity"}
            )
        else:
            # object_store has no DefaultAzureCredential equivalent; mint a token
            opts["bearer_token"] = self._default_chain_token()
        return opts

    def _default_chain_token(self) -> str:
        import time

        from azure.identity import DefaultAzureCredential

        if (
            self._cached_token is None
            or self._cached_token.expires_on < time.time() + 300
        ):
            self._cached_token = DefaultAzureCredential(
                **self.config.credentials
            ).get_token("https://storage.azure.com/.default")
        return self._cached_token.token

    def _get_service_client(self):
        if self._service_client is not None:
            return self._service_client

        from azure.storage.filedatalake import DataLakeServiceClient

        url = self._dfs_url()
        creds = self.config.credentials
        if "bearer_token" in creds:
            service = DataLakeServiceClient(
                url, credential=_StaticTokenCredential(creds["bearer_token"])
            )
        elif "account_key" in creds:
            service = DataLakeServiceClient(url, credential=creds["account_key"])
        elif "sas_token" in creds:
            service = DataLakeServiceClient(f"{url}?{creds['sas_token']}")
        elif "client_secret" in creds:
            from azure.identity import ClientSecretCredential

            service = DataLakeServiceClient(
                url,
                credential=ClientSecretCredential(
                    creds["tenant_id"], creds["client_id"], creds["client_secret"]
                ),
            )
        elif "federated_token_file" in creds:
            from azure.identity import WorkloadIdentityCredential

            service = DataLakeServiceClient(
                url,
                credential=WorkloadIdentityCredential(
                    tenant_id=creds["tenant_id"],
                    client_id=creds["client_id"],
                    token_file_path=creds["federated_token_file"],
                ),
            )
        elif creds.get("use_azure_cli"):
            from azure.identity import AzureCliCredential

            service = DataLakeServiceClient(url, credential=AzureCliCredential())
        elif creds.get("use_managed_identity"):
            from azure.identity import ManagedIdentityCredential

            identity_config = {}
            if "object_id" in creds:
                identity_config["object_id"] = creds["object_id"]
            if "msi_resource_id" in creds:
                identity_config["msi_res_id"] = creds["msi_resource_id"]
            service = DataLakeServiceClient(
                url,
                credential=ManagedIdentityCredential(
                    client_id=creds.get("client_id"),
                    identity_config=identity_config or None,
                ),
            )
        else:
            from azure.identity import DefaultAzureCredential

            service = DataLakeServiceClient(
                url, credential=DefaultAzureCredential(**creds)
            )
        self._service_client = service
        return self._service_client

    def _get_file_system_client(self, file_system: str | None = None):
        name = file_system or self.config.container
        return self._get_service_client().get_file_system_client(name)

    def _remove_directory(self, file_system: str, path: str) -> None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._get_file_system_client(file_system).get_directory_client(
                path
            ).delete_directory()
        except ResourceNotFoundError:
            pass

    # ── schema management ─────────────────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        if self.config.schemas_as_containers:
            from azure.core.exceptions import ResourceExistsError

            try:
                self._get_service_client().create_file_system(schema)
            except ResourceExistsError:
                pass
            logger.debug(f"create_schema: created file system {schema}")
        else:
            path = self._object_path(schema)
            logger.debug(f"create_schema: creating directory {path}")
            self._get_file_system_client().create_directory(path)

    def drop_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        file_system = self._container_name(schema)
        path = self._object_path(schema)
        logger.debug(f"Dropping schema {relation.catalog}/{schema}")
        self._remove_directory(file_system, path)

    def list_schemas(self) -> list[str]:
        if self.config.schemas_as_containers:
            return [fs.name for fs in self._get_service_client().list_file_systems()]
        from azure.core.exceptions import ResourceNotFoundError

        base = self.config.prefix or ""
        try:
            return [
                p.name.split("/")[-1]
                for p in self._get_file_system_client().get_paths(
                    path=base, recursive=False
                )
                if p.is_directory
            ]
        except ResourceNotFoundError:
            return []

    def table_exists(self, relation: PolarsRelation) -> bool:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            return DeltaTable.is_deltatable(uri, storage_options=opts)
        file_system = self._container_name(schema)
        file_path = f"{self._object_path(schema, identifier)}.{relation.file_format}"
        return (
            self._get_file_system_client(file_system)
            .get_file_client(file_path)
            .exists()
        )

    def drop_relation(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        file_system = self._container_name(schema)
        logger.debug(
            f"Dropping table if exists {relation.catalog}/{schema}/{identifier}"
        )
        if relation.file_format == "delta":
            self._remove_directory(file_system, self._object_path(schema, identifier))
        else:
            from azure.core.exceptions import ResourceNotFoundError

            file_path = (
                f"{self._object_path(schema, identifier)}.{relation.file_format}"
            )
            try:
                self._get_file_system_client(file_system).get_file_client(
                    file_path
                ).delete_file()
            except ResourceNotFoundError:
                pass

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        from azure.core.exceptions import ResourceNotFoundError

        schema = schema_relation.schema or ""
        file_system = self._container_name(schema)
        prefix = self._object_path(schema)
        relations: list[PolarsRelation] = []

        try:
            paths = list(
                self._get_file_system_client(file_system).get_paths(
                    path=prefix, recursive=False
                )
            )
        except ResourceNotFoundError:
            return []

        for path in paths:
            name = path.name.split("/")[-1]
            if path.is_directory:
                uri = f"az://{file_system}/{path.name}"
                if DeltaTable.is_deltatable(
                    uri, storage_options=self._get_storage_options(uri)
                ):
                    relations.append(
                        schema_relation.create(
                            database=schema_relation.database,
                            schema=schema,
                            identifier=name,
                            type=RelationType.Table,
                            catalog=schema_relation.catalog,
                            file_format="delta",
                        )
                    )
            else:
                stem, _, ext = name.rpartition(".")
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
