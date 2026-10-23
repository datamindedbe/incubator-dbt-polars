from __future__ import annotations

import json
import re
import threading
import time
from typing import TYPE_CHECKING

import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from dbt_polars_catalog_fabric.config import FabricCatalogConfig

if TYPE_CHECKING:
    from azure.core.credentials import AccessToken

logger = AdapterLogger("polars")

ONELAKE_DFS_HOST = "onelake.dfs.fabric.microsoft.com"
ONELAKE_BLOB_HOST = "onelake.blob.fabric.microsoft.com"
ONELAKE_ICEBERG_URI = "https://onelake.table.fabric.microsoft.com/iceberg"
ONELAKE_DELTA_URI = "https://onelake.table.fabric.microsoft.com/delta"
STORAGE_TOKEN_SCOPE = "https://storage.azure.com/.default"
DEFAULT_SCHEMA = "dbo"
METADATA_API_ATTEMPTS = 4
RETRIED_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
GUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def lakehouse_item_path(lakehouse: str) -> str:
    """OneLake addresses an item by GUID, or by `<display name>.Lakehouse`."""
    if GUID_PATTERN.match(lakehouse):
        return lakehouse
    return f"{lakehouse}.Lakehouse"


def is_missing_path(error_code: str | None) -> bool:
    """True only for a path that really doesn't exist.

    OneLake also answers 404 when e.g. the Fabric capacity is paused
    (error code `NotFound`), which must not be mistaken for an absent table.
    """
    return error_code == "PathNotFound"


def schema_of(relation: PolarsRelation) -> str:
    if relation.schema is None:
        raise DbtRuntimeError(f"Relation {relation} is missing a schema")
    return relation.schema


def identifier_of(relation: PolarsRelation) -> str:
    if relation.identifier is None:
        raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
    return relation.identifier


class OneLakeClient:
    """Lakehouse schemas and tables in OneLake.

    Fabric discovers tables under `Tables/<schema>/<table>` by itself, so there
    is no registration step: schemas and tables are created and dropped as
    OneLake directories, and listed through OneLake's Delta metadata API. Every
    call authenticates as the principal resolved by DefaultAzureCredential.
    """

    def __init__(self, config: FabricCatalogConfig) -> None:
        self.config = config
        self.lock = threading.RLock()
        self.credential: DefaultAzureCredential | None = None
        self.file_system = None
        self.access_token: AccessToken | None = None
        self.known_schemas: set[str] = set()
        # (schema, table) -> "delta" | "iceberg"
        self.table_formats: dict[tuple[str, str], str] = {}

    # ── auth and clients ────────────────────────────────────────────────────────

    def azure_credential(self) -> DefaultAzureCredential:
        with self.lock:
            if self.credential is None:
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
                self.file_system = DataLakeServiceClient(
                    f"https://{ONELAKE_DFS_HOST}", credential=self.azure_credential()
                ).get_file_system_client(self.config.workspace)
            return self.file_system

    def storage_options(self) -> dict[str, str]:
        return {"bearer_token": self.storage_token(), "use_fabric_endpoint": "true"}

    # ── paths ───────────────────────────────────────────────────────────────────

    def tables_path(self, *parts: str) -> str:
        return "/".join([lakehouse_item_path(self.config.lakehouse), "Tables", *parts])

    def table_uri(self, relation: PolarsRelation) -> str:
        path = self.tables_path(schema_of(relation), identifier_of(relation))
        return f"abfss://{self.config.workspace}@{ONELAKE_DFS_HOST}/{path}"

    def remove_directory(self, path: str) -> None:
        try:
            self.file_system_client().get_directory_client(path).delete_directory()
        except ResourceNotFoundError as exc:
            if not is_missing_path(getattr(exc, "error_code", None)):
                raise DbtRuntimeError(f"OneLake request failed: {exc}") from exc

    # ── Delta metadata API ──────────────────────────────────────────────────────

    def get_with_retries(self, url: str, params: dict) -> requests.Response:
        """GET that retries timeouts, connection errors and temporary server errors."""
        attempt = 1
        while True:
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers={"Authorization": f"Bearer {self.storage_token()}"},
                    timeout=60,
                )
                if (
                    response.status_code not in RETRIED_STATUS_CODES
                    or attempt == METADATA_API_ATTEMPTS
                ):
                    return response
            except (requests.Timeout, requests.ConnectionError):
                if attempt == METADATA_API_ATTEMPTS:
                    raise
            time.sleep(2**attempt)
            attempt += 1

    def list_metadata(self, endpoint: str, items_key: str, **params) -> list[dict]:
        """All items of a Delta metadata API list call, [] for a missing path."""
        item = lakehouse_item_path(self.config.lakehouse)
        url = (
            f"{ONELAKE_DELTA_URI}/{self.config.workspace}/{item}"
            f"/api/2.1/unity-catalog/{endpoint}"
        )
        params = {"catalog_name": item, **params}
        items: list[dict] = []
        while True:
            response = self.get_with_retries(url, params)
            if response.status_code == 404 and is_missing_path(
                response.json().get("error", {}).get("code")
            ):
                return []
            if not response.ok:
                raise DbtRuntimeError(
                    f"OneLake request failed: {response.status_code} {response.text}"
                )
            body = response.json()
            items.extend(body.get(items_key) or [])
            if not body.get("next_page_token"):
                return items
            params["page_token"] = body["next_page_token"]

    # ── tables ──────────────────────────────────────────────────────────────────

    def table_format(self, relation: PolarsRelation) -> str | None:
        """`delta`, `iceberg`, or None when there is no table.

        Every table has a Delta log: Fabric generates one for Iceberg tables,
        marking Iceberg as the source format in `XTABLE_METADATA`.
        """
        key = (schema_of(relation), identifier_of(relation))
        if key in self.table_formats:
            return self.table_formats[key]
        try:
            delta_table = DeltaTable(
                self.table_uri(relation), storage_options=self.storage_options()
            )
        except TableNotFoundError:
            return None
        xtable_metadata = json.loads(
            delta_table.metadata().configuration.get("XTABLE_METADATA") or "{}"
        )
        table_format = (
            "iceberg"
            if xtable_metadata.get("sourceTableFormat") == "ICEBERG"
            else "delta"
        )
        self.table_formats[key] = table_format
        return table_format

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
                identifier=table["name"],
                type=RelationType.Table,
                catalog=schema_relation.catalog,
            )
            for table in self.list_metadata("tables", "tables", schema_name=schema)
        ]

    # ── schema management ───────────────────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        schema = schema_of(relation)
        if schema in self.known_schemas:
            return
        logger.debug(f"Creating schema {relation.catalog}/{schema}")
        try:
            self.file_system_client().create_directory(self.tables_path(schema))
        except ResourceNotFoundError as exc:
            raise DbtRuntimeError(f"OneLake request failed: {exc}") from exc
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
        return [schema["name"] for schema in self.list_metadata("schemas", "schemas")]
