import re
import time

import polars as pl
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs import PolarsRelation, StorageCatalog
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_databricks.config import DatabricksCatalogConfig
from dbt_polars_catalog_databricks.databricks_types import polars_dtype_to_uc_type
from dbt_polars_catalog_databricks.unity_catalog import (
    UnityCatalogClient,
    UnityCatalogTable,
    is_already_exists_error,
)

logger = AdapterLogger("polars")

CREDENTIAL_RESPONSE_FIELDS = (
    "aws_temp_credentials",
    "azure_user_delegation_sas",
    "azure_aad",
    "gcp_oauth_token",
    "r2_temp_credentials",
)

AZURE_URI = re.compile(
    r"abfss://(?P<container>[^@/]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net"
    r"/(?P<path>.+?)/?"
)


def map_vended_credentials(response: dict) -> dict[str, str]:
    if response.get("aws_temp_credentials"):
        c = response["aws_temp_credentials"]
        return {
            "AWS_ACCESS_KEY_ID": c["access_key_id"],
            "AWS_SECRET_ACCESS_KEY": c["secret_access_key"],
            "AWS_SESSION_TOKEN": c["session_token"],
        }
    if response.get("azure_user_delegation_sas"):
        return {"sas_token": response["azure_user_delegation_sas"]["sas_token"]}
    if response.get("azure_aad"):
        return {"bearer_token": response["azure_aad"]["aad_token"]}
    if response.get("gcp_oauth_token"):
        return {"bearer_token": response["gcp_oauth_token"]["oauth_token"]}
    if response.get("r2_temp_credentials"):
        c = response["r2_temp_credentials"]
        return {
            "AWS_ACCESS_KEY_ID": c["access_key_id"],
            "AWS_SECRET_ACCESS_KEY": c["secret_access_key"],
            "AWS_SESSION_TOKEN": c["session_token"],
        }
    raise DbtRuntimeError(
        "Unity Catalog returned a temporary credential response with none of "
        f"the expected fields ({', '.join(CREDENTIAL_RESPONSE_FIELDS)})."
    )


class StaticTokenCredential:
    """An Azure credential for a vended Microsoft Entra ID token."""

    def __init__(self, token: str) -> None:
        self.token = token

    def get_token(self, *args, **kwargs):
        from azure.core.credentials import AccessToken

        return AccessToken(self.token, int(time.time()) + 3600)


def delete_azure_directory(uri: str, opts: dict[str, str]) -> None:
    """Delete an ADLS Gen2 directory with everything in it, with vended credentials."""
    try:
        from azure.core.exceptions import ResourceNotFoundError
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError as exc:
        raise DbtRuntimeError(
            "Deleting storage on Azure needs the azure extra: "
            "pip install 'dbt-polars-catalog-databricks[azure]'"
        ) from exc

    match = AZURE_URI.fullmatch(uri)
    if match is None:
        raise DbtRuntimeError(f"Not an ADLS Gen2 location: {uri}")
    url = f"https://{match['account']}.dfs.core.windows.net"
    if "sas_token" in opts:
        service = DataLakeServiceClient(f"{url}?{opts['sas_token'].lstrip('?')}")
    else:
        service = DataLakeServiceClient(
            url, credential=StaticTokenCredential(opts["bearer_token"])
        )
    directory = service.get_file_system_client(match["container"]).get_directory_client(
        match["path"]
    )
    try:
        directory.delete_directory()
    except ResourceNotFoundError:
        pass


class DatabricksDeltaCatalog(StorageCatalog):
    """Registers external Delta tables in Unity Catalog via the REST API.

    No Databricks compute/SQL warehouse is ever used: schema/table management and
    credential vending go straight to the Unity Catalog REST API.
    """

    config: DatabricksCatalogConfig

    def __init__(
        self, config: DatabricksCatalogConfig, unity_catalog: UnityCatalogClient
    ) -> None:
        super().__init__(config)
        self.unity_catalog = unity_catalog
        self.catalog_root: str | None = None
        self.uris_pending_creation: set[str] = set()
        # (uri or table_id, operation) -> (storage_options, expiry)
        self.path_credential_cache: dict[
            tuple[str, str], tuple[dict[str, str], float]
        ] = {}
        self.table_credential_cache: dict[
            tuple[str, str], tuple[dict[str, str], float]
        ] = {}

    # ── storage locations ──────────────────────────────────────────────────────

    def catalog_storage_root(self) -> str:
        if self.catalog_root is not None:
            return self.catalog_root
        info = self.unity_catalog.request(
            "GET", f"/api/2.1/unity-catalog/catalogs/{self.config.catalog_name}"
        )
        root = (info.get("storage_root") or "").rstrip("/")
        if not root:
            raise DbtRuntimeError(
                f"Catalog {self.config.catalog_name} has no storage location in "
                "Unity Catalog. Configure a managed location on it (e.g. "
                "`CREATE CATALOG ... MANAGED LOCATION '...'`) before writing "
                "external tables through it."
            )
        self.catalog_root = root
        return root

    def schema_storage_root(self, schema: str) -> str:
        return f"{self.catalog_storage_root()}/{schema}"

    def get_uri(self, relation: PolarsRelation) -> str:
        table = self.unity_catalog.registered_table(relation)
        if table is not None:
            return table.storage_location
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        return f"{self.schema_storage_root(relation.schema)}/{relation.identifier}"

    # ── credential vending ───────────────────────────────────────────────────────

    def get_storage_options(
        self, uri: str, read_only: bool = False
    ) -> dict[str, str] | None:
        table_id = self.unity_catalog.storage_uri_to_table_id.get(uri)
        if table_id is not None:
            return self.vend_table_credentials(
                table_id, "READ" if read_only else "READ_WRITE"
            )
        if uri in self.uris_pending_creation:
            return self.vend_path_credentials(uri, "PATH_CREATE_TABLE")
        logger.debug(
            f"No cached table_id for {uri}; falling back to path-based credentials."
        )
        return self.vend_path_credentials(
            uri, "PATH_READ" if read_only else "PATH_READ_WRITE"
        )

    def vend_credentials(
        self,
        cache: dict[tuple[str, str], tuple[dict[str, str], float]],
        cache_key: tuple[str, str],
        endpoint: str,
        body: dict,
    ) -> dict[str, str]:
        cached = self.valid_cached_credentials(cache, cache_key)
        if cached is not None:
            return cached

        now = time.time()
        response = self.unity_catalog.request("POST", endpoint, body=body)
        opts = map_vended_credentials(response)
        expiry = (
            response["expiration_time"] / 1000.0
            if response.get("expiration_time")
            else now + 3600
        )
        cache[cache_key] = (opts, expiry)
        return opts

    def valid_cached_credentials(
        self,
        cache: dict[tuple[str, str], tuple[dict[str, str], float]],
        cache_key: tuple[str, str],
    ) -> dict[str, str] | None:
        cached = cache.get(cache_key)
        if cached is not None and cached[1] > time.time() + 300:
            return cached[0]
        return None

    def vend_table_credentials(self, table_id: str, operation: str) -> dict[str, str]:
        if operation == "READ":
            read_write = self.valid_cached_credentials(
                self.table_credential_cache, (table_id, "READ_WRITE")
            )
            if read_write is not None:
                return read_write
        return self.vend_credentials(
            self.table_credential_cache,
            (table_id, operation),
            "/api/2.0/unity-catalog/temporary-table-credentials",
            {"table_id": table_id, "operation": operation},
        )

    def vend_path_credentials(self, uri: str, operation: str) -> dict[str, str]:
        return self.vend_credentials(
            self.path_credential_cache,
            (uri, operation),
            "/api/2.0/unity-catalog/temporary-path-credentials",
            {"url": uri, "operation": operation},
        )

    # ── schema management and listing ──────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        self.unity_catalog.create_schema(relation)

    def drop_schema(self, relation: PolarsRelation) -> None:
        self.unity_catalog.drop_schema(relation, self.drop_table)

    def list_schemas(self) -> list[str]:
        return self.unity_catalog.list_schemas()

    def table_exists(self, relation: PolarsRelation) -> bool:
        return self.unity_catalog.registered_table(relation) is not None

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        return self.unity_catalog.list_relations(schema_relation)

    # ── drop ─────────────────────────────────────────────────────────────────────

    def drop_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Dropping table if exists {self.unity_catalog.table_full_name(relation)}"
        )
        table = self.unity_catalog.registered_table(relation)
        if table is not None:
            self.drop_table(table)

    def drop_table(self, table: UnityCatalogTable) -> None:
        if table.managed:
            # Unity Catalog removes a managed table's data itself
            self.unity_catalog.delete_table(table.full_name)
            return
        opts = self.vend_table_credentials(table.table_id, "READ_WRITE")
        self.unity_catalog.delete_table(table.full_name)
        self.table_credential_cache.pop((table.table_id, "READ"), None)
        self.table_credential_cache.pop((table.table_id, "READ_WRITE"), None)
        self.delete_storage(table.storage_location, opts)

    def delete_storage(self, uri: str, opts: dict[str, str]) -> None:
        from deltalake import DeltaTable

        try:
            if uri.startswith("abfss://"):
                # vacuum's batch delete is refused for the table-scoped SAS
                delete_azure_directory(uri, opts)
                return
            if not DeltaTable.is_deltatable(uri, storage_options=opts):
                return
            dt = DeltaTable(uri, storage_options=opts)
            dt.delete()
            # Leaves the _delta_log behind: vacuum never removes it
            dt.vacuum(
                retention_hours=0,
                enforce_retention_duration=False,
                dry_run=False,
                full=True,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Could not clean up storage at {uri}: {exc}")

    # ── write / register ─────────────────────────────────────────────────────────

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        self.create_schema(relation)

        already_registered = self.table_exists(relation)
        uri = self.get_uri(relation)

        if already_registered:
            super().write_relation(relation, data, partition_by, model_config)
            return

        self.uris_pending_creation.add(uri)
        try:
            super().write_relation(relation, data, partition_by, model_config)
        finally:
            self.uris_pending_creation.discard(uri)

        self.register_table(relation, uri, data, partition_by)

    def register_table(
        self,
        relation: PolarsRelation,
        uri: str,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
    ) -> None:
        from databricks.sdk.errors import DatabricksError

        full = self.unity_catalog.table_full_name(relation)
        schema = (
            data.collect_schema() if isinstance(data, pl.LazyFrame) else data.schema
        )
        columns = []
        for i, (name, dtype) in enumerate(schema.items()):
            type_name, type_text, type_json = polars_dtype_to_uc_type(dtype)
            column = {
                "name": name,
                "type_name": type_name,
                "type_text": type_text,
                "type_json": type_json,
                "position": i,
                "nullable": True,
            }
            if name in partition_by:
                column["partition_index"] = partition_by.index(name)
            columns.append(column)

        body = {
            "name": relation.identifier,
            "catalog_name": self.config.catalog_name,
            "schema_name": relation.schema,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": uri,
            "columns": columns,
        }

        try:
            info = self.unity_catalog.request(
                "POST", "/api/2.1/unity-catalog/tables", body=body
            )
        except DatabricksError as exc:
            if not is_already_exists_error(exc, "TABLE_ALREADY_EXISTS"):
                raise
            info = self.unity_catalog.request(
                "GET", f"/api/2.1/unity-catalog/tables/{full}"
            )

        self.unity_catalog.remember_table(
            {
                "full_name": full,
                **info,
                "storage_location": info.get("storage_location") or uri,
            }
        )
