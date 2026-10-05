import re
import threading
import time

import polars as pl
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs import (
    PolarsRelation,
    StorageCatalog,
    file_format_of,
)
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable

from dbt_polars_catalog_fabric.config import FabricCatalogConfig

logger = AdapterLogger("polars")

ONELAKE_DFS_HOST = "onelake.dfs.fabric.microsoft.com"
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


class FabricCatalog(StorageCatalog):
    """Writes Delta tables into a schema-enabled Fabric Lakehouse via OneLake.

    Fabric discovers Delta tables under `Tables/<schema>/<table>` by itself, so
    there is no registration step: schemas and tables are plain OneLake
    directories, managed through the ADLS Gen2-compatible OneLake endpoint.
    Every call authenticates as the principal resolved by
    DefaultAzureCredential.
    """

    config: FabricCatalogConfig

    def __init__(self, config: FabricCatalogConfig, project_root: str) -> None:
        super().__init__(config)
        self.lock = threading.RLock()
        self.credential = None
        self.file_system = None
        self.access_token = None
        self.known_schemas: set[str] = set()

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

    def get_uri(self, relation: PolarsRelation) -> str:
        if relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
        return self.uri_for_path(
            self.tables_path(schema_of(relation), relation.identifier)
        )

    def get_storage_options(self, uri: str, read_only: bool = False) -> dict[str, str]:
        return {
            "bearer_token": self.storage_token(),
            "use_fabric_endpoint": "true",
            "timeout": "120s",
        }

    def remove_directory(self, path: str) -> None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self.file_system_client().get_directory_client(path).delete_directory()
        except ResourceNotFoundError as exc:
            if not is_missing_path(exc):
                raise onelake_error(exc) from exc

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

    def list_schemas(self) -> list[str]:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            paths = list(
                self.file_system_client().get_paths(
                    path=self.tables_path(), recursive=False
                )
            )
        except ResourceNotFoundError as exc:
            raise onelake_error(exc) from exc
        return [path.name.rsplit("/", 1)[-1] for path in paths if path.is_directory]

    # ── tables ──────────────────────────────────────────────────────────────────

    def table_exists(self, relation: PolarsRelation) -> bool:
        uri = self.get_uri(relation)
        return DeltaTable.is_deltatable(
            uri, storage_options=self.get_storage_options(uri, read_only=True)
        )

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        from azure.core.exceptions import ResourceNotFoundError

        schema = schema_of(schema_relation)
        try:
            paths = list(
                self.file_system_client().get_paths(
                    path=self.tables_path(schema), recursive=False
                )
            )
        except ResourceNotFoundError as exc:
            if not is_missing_path(exc):
                raise onelake_error(exc) from exc
            return []

        relations = []
        for path in paths:
            if not path.is_directory:
                continue
            uri = self.uri_for_path(path.name)
            if not DeltaTable.is_deltatable(
                uri, storage_options=self.get_storage_options(uri, read_only=True)
            ):
                continue
            relations.append(
                schema_relation.create(
                    database=schema_relation.database,
                    schema=schema,
                    identifier=path.name.rsplit("/", 1)[-1],
                    type=RelationType.Table,
                    catalog=schema_relation.catalog,
                )
            )
        return relations

    def drop_relation(self, relation: PolarsRelation) -> None:
        schema = schema_of(relation)
        identifier = relation.identifier or ""
        logger.debug(
            f"Dropping table if exists {relation.catalog}/{schema}/{identifier}"
        )
        self.remove_directory(self.tables_path(schema, identifier))

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        if file_format_of(relation) != "delta":
            raise DbtRuntimeError(
                "file_format is not a supported config for the fabric catalog "
                f"(got {file_format_of(relation)!r} for {relation}) - Lakehouse "
                "tables are always written as Delta."
            )
        self.create_schema(relation)
        super().write_relation(relation, data, partition_by, model_config)
