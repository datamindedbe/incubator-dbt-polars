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
        credentials: dict | None = None,
    ) -> None:
        self.name = name
        self.type = type
        self.account_name = account_name
        self.container = container
        self.prefix = prefix.strip("/")
        self.credentials = credentials or {}

    def unique_field(self) -> str:
        return f"{self.account_name}/{self.container}/{self.prefix}"

    def connection_keys(self) -> tuple[str, ...]:
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
        self._cached_token: AccessToken | None = None
        self._service_client = None  # DataLakeServiceClient | None
        self._dac = None  # DefaultAzureCredential | None

    # ── URI and credential helpers ────────────────────────────────────────────

    def _blob_prefix(self, *parts: str) -> str:
        """Join the catalog prefix with additional path parts."""
        components = [self.config.prefix] if self.config.prefix else []
        components.extend(p.strip("/") for p in parts if p)
        return "/".join(components)

    def _object_path(self, schema: str, *parts: str) -> str:
        return self._blob_prefix(schema, *parts)

    def _dfs_url(self) -> str:
        return f"https://{self.config.account_name}.dfs.core.windows.net"

    def _get_uri(self, relation: PolarsRelation) -> str:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        container = self.config.container
        path = self._object_path(schema, identifier)
        if relation.file_format != "delta":
            path = f"{path}.{relation.file_format}"
        return f"az://{container}/{path}"

    _DELEGATED_CRED_KEYS = ("bearer_token", "account_key", "sas_token")

    def _get_storage_options(self, _uri: str) -> dict[str, str]:
        base: dict[str, str] = {
            "account_name": self.config.account_name,
            "timeout": "120s",
        }
        creds = self.config.credentials

        override = creds.get("storage_options")
        if override is not None:
            return {**base, **override}

        direct = {k: str(v) for k, v in creds.items() if k in self._DELEGATED_CRED_KEYS}
        if direct:
            return {**base, **direct}

        base["bearer_token"] = self._default_chain_token()
        return base

    def _get_dac(self):
        if self._dac is None:
            from azure.identity import DefaultAzureCredential

            dac_kwargs = {
                k: v
                for k, v in self.config.credentials.items()
                if k != "storage_options"
            }
            self._dac = DefaultAzureCredential(**dac_kwargs)
        return self._dac

    def _default_chain_token(self) -> str:
        import time

        if (
            self._cached_token is None
            or self._cached_token.expires_on < time.time() + 300
        ):
            self._cached_token = self._get_dac().get_token(
                "https://storage.azure.com/.default"
            )
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
        else:
            service = DataLakeServiceClient(url, credential=self._get_dac())
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
        path = self._object_path(schema)
        logger.debug(f"create_schema: creating directory {path}")
        self._get_file_system_client().create_directory(path)

    def drop_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        path = self._object_path(schema)
        logger.debug(f"Dropping schema {relation.catalog}/{schema}")
        self._remove_directory(self.config.container, path)

    def list_schemas(self) -> list[str]:
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
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            return DeltaTable.is_deltatable(uri, storage_options=opts)
        file_path = uri.removeprefix(f"az://{self.config.container}/")
        return (
            self._get_file_system_client(self.config.container)
            .get_file_client(file_path)
            .exists()
        )

    def drop_relation(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        file_system = self.config.container
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
        file_system = self.config.container
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
