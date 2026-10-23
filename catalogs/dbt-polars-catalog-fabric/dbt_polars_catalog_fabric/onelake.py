from __future__ import annotations

import json
import re
import threading
import time
from typing import TYPE_CHECKING

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_fabric.config import FabricCatalogConfig

if TYPE_CHECKING:
    from azure.core.credentials import AccessToken

logger = AdapterLogger("polars")

ONELAKE_DFS_HOST = "onelake.dfs.fabric.microsoft.com"
ONELAKE_BLOB_HOST = "onelake.blob.fabric.microsoft.com"
ONELAKE_ICEBERG_URI = "https://onelake.table.fabric.microsoft.com/iceberg"
STORAGE_TOKEN_SCOPE = "https://storage.azure.com/.default"
DEFAULT_SCHEMA = "dbo"
GUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def lakehouse_item_path(lakehouse: str) -> str:
    """OneLake addresses an item by GUID, or by `<display name>.Lakehouse`."""
    if GUID_PATTERN.match(lakehouse):
        return lakehouse
    return f"{lakehouse}.Lakehouse"


def is_missing_path(exc: Exception) -> bool:
    """True only for a path that really doesn't exist.

    OneLake also answers 404 when e.g. the Fabric capacity is paused
    (error code `NotFound`), which must not be mistaken for an absent table.
    """
    return getattr(exc, "error_code", None) == "PathNotFound"


def onelake_error(exc: Exception) -> DbtRuntimeError:
    return DbtRuntimeError(f"OneLake request failed: {exc}")


def schema_of(relation: PolarsRelation) -> str:
    if relation.schema is None:
        raise DbtRuntimeError(f"Relation {relation} is missing a schema")
    return relation.schema


def identifier_of(relation: PolarsRelation) -> str:
    if relation.identifier is None:
        raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
    return relation.identifier


class OneLakeClient:
    """Lakehouse schemas and tables as OneLake directories.

    Fabric discovers tables under `Tables/<schema>/<table>` by itself, so there
    is no registration step. Every call authenticates as the principal resolved
    by DefaultAzureCredential.
    """

    def __init__(self, config: FabricCatalogConfig) -> None:
        self.config = config
        self.lock = threading.RLock()
        self.credential = None
        self.file_system = None
        self.access_token: AccessToken | None = None
        self.known_schemas: set[str] = set()
        # (schema, table) -> "delta" | "iceberg"
        self.table_formats: dict[tuple[str, str], str] = {}

    # ── auth and clients ────────────────────────────────────────────────────────

    def azure_credential(self):
        with self.lock:
            if self.credential is None:
                from azure.identity import DefaultAzureCredential

                self.credential = DefaultAzureCredential(
                    **self.config.credential_kwargs
                )
            return self.credential

    def storage_token(self) -> str:
        with self.lock:
            if (
                self.access_token is None
                or self.access_token.expires_on < time.time() + 300
            ):
                self.access_token = self.azure_credential().get_token(
                    STORAGE_TOKEN_SCOPE
                )
            return self.access_token.token

    def file_system_client(self):
        with self.lock:
            if self.file_system is None:
                from azure.storage.filedatalake import DataLakeServiceClient

                self.file_system = DataLakeServiceClient(
                    f"https://{ONELAKE_DFS_HOST}", credential=self.azure_credential()
                ).get_file_system_client(self.config.workspace)
            return self.file_system

    # ── paths ───────────────────────────────────────────────────────────────────

    def tables_path(self, *parts: str) -> str:
        return "/".join([lakehouse_item_path(self.config.lakehouse), "Tables", *parts])

    def uri_for_path(self, path: str) -> str:
        return f"abfss://{self.config.workspace}@{ONELAKE_DFS_HOST}/{path}"

    def table_uri(self, relation: PolarsRelation) -> str:
        return self.uri_for_path(
            self.tables_path(schema_of(relation), identifier_of(relation))
        )

    def iceberg_warehouse(self) -> str:
        return f"{self.config.workspace}/{lakehouse_item_path(self.config.lakehouse)}"

    def child_names(self, path: str) -> list[str] | None:
        """Names of the entries directly under `path`, None if it doesn't exist."""
        from azure.core.exceptions import ResourceNotFoundError

        try:
            paths = list(
                self.file_system_client().get_paths(path=path, recursive=False)
            )
        except ResourceNotFoundError as exc:
            if not is_missing_path(exc):
                raise onelake_error(exc) from exc
            return None
        return [p.name.rsplit("/", 1)[-1] for p in paths if p.is_directory]

    def remove_directory(self, path: str) -> None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self.file_system_client().get_directory_client(path).delete_directory()
        except ResourceNotFoundError as exc:
            if not is_missing_path(exc):
                raise onelake_error(exc) from exc

    # ── tables ──────────────────────────────────────────────────────────────────

    def table_format(self, relation: PolarsRelation) -> str | None:
        """`delta`, `iceberg`, or None when the directory holds no table."""
        key = (schema_of(relation), identifier_of(relation))
        if key in self.table_formats:
            return self.table_formats[key]
        children = self.child_names(self.tables_path(*key)) or []
        # Fabric adds a Delta log to Iceberg tables and can add Iceberg metadata
        # to Delta tables, so a Delta log alone doesn't decide the format.
        if "_delta_log" in children:
            table_format = self.delta_log_source_format(relation)
        elif "metadata" in children:
            table_format = "iceberg"
        else:
            return None
        self.table_formats[key] = table_format
        return table_format

    def delta_log_source_format(self, relation: PolarsRelation) -> str:
        """`iceberg` when Fabric generated the Delta log for an Iceberg table."""
        from deltalake import DeltaTable

        configuration = (
            DeltaTable(
                self.table_uri(relation),
                storage_options={
                    "bearer_token": self.storage_token(),
                    "use_fabric_endpoint": "true",
                },
            )
            .metadata()
            .configuration
        )
        xtable_metadata = json.loads(configuration.get("XTABLE_METADATA") or "{}")
        if xtable_metadata.get("sourceTableFormat") == "ICEBERG":
            return "iceberg"
        return "delta"

    def remember_table(self, relation: PolarsRelation, table_format: str) -> None:
        self.table_formats[(schema_of(relation), identifier_of(relation))] = (
            table_format
        )

    def drop_table(self, relation: PolarsRelation) -> None:
        schema, identifier = schema_of(relation), identifier_of(relation)
        logger.debug(
            f"Dropping table if exists {relation.catalog}/{schema}/{identifier}"
        )
        # Also removes an Iceberg table from OneLake's Iceberg catalog
        self.remove_directory(self.tables_path(schema, identifier))
        self.table_formats.pop((schema, identifier), None)

    def list_relations(self, schema_relation: PolarsRelation) -> list[PolarsRelation]:
        schema = schema_of(schema_relation)
        return [
            schema_relation.create(
                database=schema_relation.database,
                schema=schema,
                identifier=name,
                type=RelationType.Table,
                catalog=schema_relation.catalog,
            )
            for name in self.child_names(self.tables_path(schema)) or []
        ]

    # ── schema management ───────────────────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        schema = schema_of(relation)
        if schema in self.known_schemas:
            return
        from azure.core.exceptions import ResourceNotFoundError

        logger.debug(f"Creating schema {relation.catalog}/{schema}")
        try:
            self.file_system_client().create_directory(self.tables_path(schema))
        except ResourceNotFoundError as exc:
            raise onelake_error(exc) from exc
        self.known_schemas.add(schema)

    def drop_schema(self, relation: PolarsRelation) -> None:
        schema = schema_of(relation)
        if schema == DEFAULT_SCHEMA:
            raise DbtRuntimeError(
                f"Schema '{DEFAULT_SCHEMA}' is the Lakehouse default schema and "
                "can't be dropped."
            )
        logger.debug(f"Dropping schema {relation.catalog}/{schema}")
        self.remove_directory(self.tables_path(schema))
        self.known_schemas.discard(schema)
        for key in [key for key in self.table_formats if key[0] == schema]:
            del self.table_formats[key]

    def list_schemas(self) -> list[str]:
        names = self.child_names(self.tables_path())
        if names is None:
            raise DbtRuntimeError(
                f"Lakehouse {self.config.lakehouse} in workspace "
                f"{self.config.workspace} has no Tables folder."
            )
        return names
