import threading
import time

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

import polars as pl
from dbt.adapters.polars.catalogs.baseCatalog import CatalogConfig
from dbt.adapters.polars.catalogs.databricks_types import polars_dtype_to_uc_type
from dbt.adapters.polars.catalogs.storageCatalog import StorageCatalog
from dbt.adapters.polars.relation import PolarsRelation

logger = AdapterLogger("polars")

# Credential-vending response field name -> polars/deltalake storage_options mapper.
# Key names chosen to match what AzureBlobStorageCatalog already uses for the same
# concepts (sas_token/bearer_token), for consistency within this repo.
CREDENTIAL_RESPONSE_FIELDS = (
    "aws_temp_credentials",
    "azure_user_delegation_sas",
    "azure_aad",
    "gcp_oauth_token",
    "r2_temp_credentials",
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


def is_already_exists_error(exc: Exception, error_code: str) -> bool:
    """True if `exc` represents Unity Catalog rejecting a duplicate create.

    Unity Catalog isn't consistent about the HTTP status for this across
    resource types: schema creation returns 400 with `error_code=
    SCHEMA_ALREADY_EXISTS` (confirmed against a live workspace), while other
    endpoints may use the dedicated 409 `AlreadyExists` exception - check both.
    """
    from databricks.sdk.errors import AlreadyExists

    if isinstance(exc, AlreadyExists):
        return True
    return getattr(exc, "error_code", None) == error_code


class DatabricksCatalogConfig(CatalogConfig):
    """Config for the Databricks/Unity Catalog backend.

    `catalog_name` is the only required dbt-polars-specific field (which Unity
    Catalog catalog to register tables in - schema storage locations are read
    from Unity Catalog itself, not configured here). Every other keyword is
    passed straight through to `databricks.sdk.Config`, so any auth method it
    supports (PAT, OAuth M2M, Azure client-secret, external-browser, azure-cli,
    databricks-cli profile, ...) works by supplying the matching kwargs.

    `persist_docs_http_path`: docs (table/column comments from persist_docs)
    are always written to the Delta table's own metadata. If a SQL warehouse's
    HTTP path (e.g. `/sql/1.0/warehouses/<warehouse_id>`, as shown on the
    warehouse's Connection Details page) is also set here, they're additionally
    written into Unity Catalog's native comment fields via `COMMENT ON TABLE`/
    `ALTER TABLE ... ALTER COLUMN ... COMMENT` - the only way to set those,
    since Unity Catalog's REST API has no endpoint for it at all. Left unset,
    docs never reach Unity Catalog itself.
    """

    def __init__(
        self,
        *,
        name: str,
        type: str,
        schema: str,
        catalog_name: str,
        host: str | None = None,
        persist_docs_http_path: str | None = None,
        **kwargs: object,
    ) -> None:
        self.name = name
        self.type = type
        self.schema = schema
        self.catalog_name = catalog_name
        self.persist_docs_http_path = persist_docs_http_path
        self.sdk_kwargs: dict[str, object] = {
            **({"host": host} if host else {}),
            **kwargs,
        }

    def unique_field(self) -> str:
        return f"{self.catalog_name}/{self.schema}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "catalog_name", "host", "persist_docs_http_path")


class DatabricksCatalog(StorageCatalog):
    """Registers external Delta tables in Unity Catalog via the REST API.

    No Databricks compute/SQL warehouse is ever used: schema/table management and
    credential vending go straight to the Unity Catalog REST API (authenticated via
    databricks-sdk's `WorkspaceClient`, called through its low-level `api_client`
    rather than its per-resource wrappers, since those don't always expose every
    documented request/response field - e.g. table creation's `comment`).

    Data reads/writes use Unity-Catalog-vended credentials: table-based (by
    table_id) for any table already registered (works for managed and external
    tables alike, gated by `EXTERNAL USE SCHEMA`), and path-based only to
    bootstrap the very first write of a brand-new table that has no table_id yet
    (gated by `EXTERNAL USE LOCATION`).
    """

    config: DatabricksCatalogConfig

    def __init__(self, config: DatabricksCatalogConfig, project_root: str) -> None:
        try:
            from databricks.sdk import WorkspaceClient  # noqa: F401
        except ImportError as exc:
            raise DbtRuntimeError(
                "The databricks extra is required for DatabricksCatalog. "
                "Install it with: pip install 'dbt-polars[databricks]'"
            ) from exc
        super().__init__(config)
        self.client = None
        self.lock = threading.Lock()
        self.registered_tables: set[str] = set()
        self.known_schemas: set[str] = set()
        self.catalog_root: str | None = None
        # full_name -> (table_id, storage_location)
        self.table_metadata: dict[str, tuple[str, str]] = {}
        # storage uri -> table_id, so _get_storage_options(uri) can find the table
        self.storage_uri_to_table_id: dict[str, str] = {}
        self.uris_pending_creation: set[str] = set()
        self.path_credential_cache: dict[str, tuple[dict[str, str], float]] = {}
        self.table_credential_cache: dict[str, tuple[dict[str, str], float]] = {}

    @property
    def workspace_client(self):
        if self.client is None:
            with self.lock:
                if self.client is None:
                    from databricks.sdk import WorkspaceClient

                    self.client = WorkspaceClient(**self.config.sdk_kwargs)
        return self.client

    # ── raw Unity Catalog REST access ───────────────────────────────────────────

    def unity_catalog_request(
        self,
        method: str,
        path: str,
        *,
        query: dict | None = None,
        body: dict | None = None,
    ) -> dict:
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        workspace_id = self.workspace_client.config.workspace_id
        if workspace_id:
            headers["X-Databricks-Workspace-Id"] = workspace_id
        return self.workspace_client.api_client.do(
            method, path, query=query, body=body, headers=headers
        )

    def unity_catalog_pages(
        self, path: str, *, query: dict | None = None, items_key: str
    ):
        """Yield items across every page of a paginated Unity Catalog list endpoint."""
        query = dict(query or {})
        query.setdefault("max_results", 0)
        while True:
            response = self.unity_catalog_request("GET", path, query=query)
            yield from response.get(items_key, [])
            next_page_token = response.get("next_page_token")
            if not next_page_token:
                return
            query["page_token"] = next_page_token

    # ── naming helpers ─────────────────────────────────────────────────────────

    def table_full_name(self, relation: PolarsRelation) -> str:
        return f"{self.config.catalog_name}.{relation.schema}.{relation.identifier}"

    def schema_full_name(self, schema: str) -> str:
        return f"{self.config.catalog_name}.{schema}"

    def catalog_storage_root(self) -> str:
        if self.catalog_root is not None:
            return self.catalog_root
        info = self.unity_catalog_request(
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
        # Computed directly from the catalog's own storage_root rather than
        # setting/reading one on the schema itself - doing that would require
        # the CREATE MANAGED STORAGE privilege on top of EXTERNAL_USE_LOCATION/
        # CREATE_EXTERNAL_TABLE, which this catalog otherwise never needs.
        return f"{self.catalog_storage_root()}/{schema}"

    def remember_table(
        self, full_name: str, table_id: str, storage_location: str
    ) -> None:
        self.table_metadata[full_name] = (table_id, storage_location)
        self.storage_uri_to_table_id[storage_location] = table_id
        self.registered_tables.add(full_name)

    def registered_table_info(self, relation: PolarsRelation) -> tuple[str, str] | None:
        """Return (table_id, storage_location) for an already-registered table."""
        full = self.table_full_name(relation)
        if full in self.table_metadata:
            return self.table_metadata[full]
        from databricks.sdk.errors import NotFound

        try:
            info = self.unity_catalog_request(
                "GET", f"/api/2.1/unity-catalog/tables/{full}"
            )
        except NotFound:
            return None
        self.remember_table(full, info["table_id"], info["storage_location"])
        return self.table_metadata[full]

    def _get_uri(self, relation: PolarsRelation) -> str:
        info = self.registered_table_info(relation)
        if info is not None:
            return info[1]
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        return f"{self.schema_storage_root(relation.schema)}/{relation.identifier}"

    # ── credential vending ───────────────────────────────────────────────────────

    def _get_storage_options(self, uri: str) -> dict[str, str] | None:
        table_id = self.storage_uri_to_table_id.get(uri)
        if table_id is not None:
            return self.vend_table_credentials(table_id)
        if uri in self.uris_pending_creation:
            return self.vend_path_credentials(uri, "PATH_CREATE_TABLE")
        logger.debug(
            f"No cached table_id for {uri}; falling back to path-based credentials."
        )
        return self.vend_path_credentials(uri, "PATH_READ_WRITE")

    def vend_table_credentials(self, table_id: str) -> dict[str, str]:
        cached = self.table_credential_cache.get(table_id)
        now = time.time()
        if cached is not None and cached[1] > now + 300:
            return cached[0]

        response = self.unity_catalog_request(
            "POST",
            "/api/2.0/unity-catalog/temporary-table-credentials",
            body={"table_id": table_id, "operation": "READ_WRITE"},
        )
        opts = map_vended_credentials(response)
        expiry = (
            response["expiration_time"] / 1000.0
            if response.get("expiration_time")
            else now + 3600
        )
        self.table_credential_cache[table_id] = (opts, expiry)
        return opts

    def vend_path_credentials(self, uri: str, operation: str) -> dict[str, str]:
        cached = self.path_credential_cache.get(uri)
        now = time.time()
        if cached is not None and cached[1] > now + 300:
            return cached[0]

        response = self.unity_catalog_request(
            "POST",
            "/api/2.0/unity-catalog/temporary-path-credentials",
            body={"url": uri, "operation": operation},
        )
        opts = map_vended_credentials(response)
        expiry = (
            response["expiration_time"] / 1000.0
            if response.get("expiration_time")
            else now + 3600
        )
        self.path_credential_cache[uri] = (opts, expiry)
        return opts

    # ── schema management ────────────────────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema
        if schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if schema in self.known_schemas:
            return
        from databricks.sdk.errors import DatabricksError

        logger.debug(f"Creating schema {relation.catalog}/{schema}")
        try:
            self.unity_catalog_request(
                "POST",
                "/api/2.1/unity-catalog/schemas",
                body={"name": schema, "catalog_name": self.config.catalog_name},
            )
        except DatabricksError as exc:
            if not is_already_exists_error(exc, "SCHEMA_ALREADY_EXISTS"):
                raise
        self.known_schemas.add(schema)

    def drop_schema(self, relation: PolarsRelation) -> None:
        from databricks.sdk.errors import NotFound

        schema = relation.schema
        if schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        logger.debug(f"Dropping schema {relation.catalog}/{schema}")
        try:
            tables = list(
                self.unity_catalog_pages(
                    "/api/2.1/unity-catalog/tables",
                    query={
                        "catalog_name": self.config.catalog_name,
                        "schema_name": schema,
                    },
                    items_key="tables",
                )
            )
            for t in tables:
                self.drop_registered_table(
                    t["full_name"], t["table_id"], t["storage_location"]
                )
            self.unity_catalog_request(
                "DELETE",
                f"/api/2.1/unity-catalog/schemas/{self.schema_full_name(schema)}",
                query={"force": True},
            )
        except NotFound:
            pass
        self.known_schemas.discard(schema)

    def list_schemas(self) -> list[str]:
        return [
            s["name"]
            for s in self.unity_catalog_pages(
                "/api/2.1/unity-catalog/schemas",
                query={"catalog_name": self.config.catalog_name},
                items_key="schemas",
            )
        ]

    # ── table existence / listing ───────────────────────────────────────────────

    def table_exists(self, relation: PolarsRelation) -> bool:
        return self.registered_table_info(relation) is not None

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        from databricks.sdk.errors import NotFound

        try:
            tables = list(
                self.unity_catalog_pages(
                    "/api/2.1/unity-catalog/tables",
                    query={
                        "catalog_name": self.config.catalog_name,
                        "schema_name": schema_relation.schema,
                    },
                    items_key="tables",
                )
            )
        except NotFound:
            return []

        result = []
        for t in tables:
            self.remember_table(t["full_name"], t["table_id"], t["storage_location"])
            result.append(
                schema_relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=t["name"],
                    type=RelationType.Table,
                    catalog=schema_relation.catalog,
                    file_format="delta",
                )
            )
        return result

    # ── drop ─────────────────────────────────────────────────────────────────────

    def drop_relation(self, relation: PolarsRelation) -> None:
        full = self.table_full_name(relation)
        info = self.registered_table_info(relation)
        logger.debug(f"Dropping table if exists {full}")
        if info is None:
            return
        table_id, uri = info
        self.drop_registered_table(full, table_id, uri)

    def drop_registered_table(self, full_name: str, table_id: str, uri: str) -> None:
        from databricks.sdk.errors import NotFound

        opts = self.vend_table_credentials(table_id)
        try:
            self.unity_catalog_request(
                "DELETE", f"/api/2.1/unity-catalog/tables/{full_name}"
            )
        except NotFound:
            pass
        self.registered_tables.discard(full_name)
        self.table_metadata.pop(full_name, None)
        self.storage_uri_to_table_id.pop(uri, None)
        self.table_credential_cache.pop(table_id, None)
        self.delete_storage(uri, opts)

    def delete_storage(self, uri: str, opts: dict[str, str]) -> None:
        from deltalake import DeltaTable

        try:
            if DeltaTable.is_deltatable(uri, storage_options=opts):
                DeltaTable(uri, storage_options=opts).delete()
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Could not clean up storage at {uri}: {exc}")

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
        uri = self._get_uri(relation)

        if already_registered:
            super().write_relation(relation, data, partition_by, model_config)
            return

        self.uris_pending_creation.add(uri)
        try:
            super().write_relation(relation, data, partition_by, model_config)
        finally:
            self.uris_pending_creation.discard(uri)

        self.register_table(relation, uri, data)

    def register_table(
        self,
        relation: PolarsRelation,
        uri: str,
        data: pl.DataFrame | pl.LazyFrame,
    ) -> None:
        from databricks.sdk.errors import DatabricksError

        full = self.table_full_name(relation)
        schema = (
            data.collect_schema() if isinstance(data, pl.LazyFrame) else data.schema
        )
        columns = []
        for i, (name, dtype) in enumerate(schema.items()):
            type_name, type_text, type_json = polars_dtype_to_uc_type(dtype)
            columns.append(
                {
                    "name": name,
                    "type_name": type_name,
                    "type_text": type_text,
                    "type_json": type_json,
                    "position": i,
                    "nullable": True,
                }
            )

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
            info = self.unity_catalog_request(
                "POST", "/api/2.1/unity-catalog/tables", body=body
            )
        except DatabricksError as exc:
            if not is_already_exists_error(exc, "TABLE_ALREADY_EXISTS"):
                raise
            info = self.unity_catalog_request(
                "GET", f"/api/2.1/unity-catalog/tables/{full}"
            )

        self.remember_table(full, info["table_id"], info.get("storage_location") or uri)

    # ── comments ─────────────────────────────────────────────────────────────────

    def persist_docs_warehouse_id(self) -> str:
        http_path = (self.config.persist_docs_http_path or "").rstrip("/")
        warehouse_id = http_path.rsplit("/", 1)[-1] if http_path else ""
        if not warehouse_id:
            raise DbtRuntimeError(
                f"Catalog {self.config.name!r} has an invalid "
                f"persist_docs_http_path {self.config.persist_docs_http_path!r}; "
                "expected something like /sql/1.0/warehouses/<warehouse_id>."
            )
        return warehouse_id

    def run_sql_statement(self, statement: str) -> None:
        from databricks.sdk.service.sql import StatementState

        warehouse_id = self.persist_docs_warehouse_id()
        response = self.workspace_client.statement_execution.execute_statement(
            statement=statement, warehouse_id=warehouse_id, wait_timeout="30s"
        )
        if response.status.state != StatementState.SUCCEEDED:
            error = response.status.error
            raise DbtRuntimeError(
                f"Failed to run SQL on warehouse {warehouse_id}: "
                f"{error.message if error else response.status.state}"
            )

    def current_unity_catalog_docs(
        self, full_name: str
    ) -> tuple[str | None, dict[str, str]]:
        info = self.unity_catalog_request(
            "GET", f"/api/2.1/unity-catalog/tables/{full_name}"
        )
        comment = info.get("comment")
        column_comments = {
            c["name"]: c["comment"] for c in info.get("columns", []) if c.get("comment")
        }
        return comment, column_comments

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        # Always keep the Delta table's own metadata current — it's freely
        # updatable and is what dbt's own `docs generate`/catalog.json reads back
        # (see get_relation_comment/get_column_comments, inherited unchanged below).
        super().set_relation_comment(relation, comment)

        if not self.config.persist_docs_http_path:
            return
        full = self.table_full_name(relation)
        current_comment, _ = self.current_unity_catalog_docs(full)
        if (current_comment or "") == (comment or ""):
            return
        escaped = comment.replace("'", "''")
        self.run_sql_statement(f"COMMENT ON TABLE {full} IS '{escaped}'")

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        super().set_column_comments(relation, comments)

        if not self.config.persist_docs_http_path:
            return
        full = self.table_full_name(relation)
        _, current_columns = self.current_unity_catalog_docs(full)
        diffs = {
            col: desired
            for col, desired in comments.items()
            if (current_columns.get(col) or "") != (desired or "")
        }
        for col, comment in diffs.items():
            escaped = comment.replace("'", "''")
            self.run_sql_statement(
                f"ALTER TABLE {full} ALTER COLUMN {col} COMMENT '{escaped}'"
            )
