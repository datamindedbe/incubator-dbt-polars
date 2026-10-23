import threading
from collections.abc import Callable
from dataclasses import dataclass

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_databricks.config import DatabricksCatalogConfig

logger = AdapterLogger("polars")


@dataclass(frozen=True)
class UnityCatalogTable:
    full_name: str
    table_id: str
    storage_location: str
    table_format: str
    managed: bool


def table_format_of(info: dict) -> str:
    """The format dbt-polars reads and writes the table in.

    Tables created through Unity Catalog's Iceberg REST endpoint can be managed
    Delta tables with Iceberg metadata (UniForm). Managed tables can only be
    written through Iceberg REST, so these count as Iceberg.
    """
    table_format = (info.get("data_source_format") or "DELTA").lower()
    enabled_formats = (info.get("properties") or {}).get(
        "delta.universalFormat.enabledFormats", ""
    )
    if (
        table_format == "delta"
        and info.get("table_type") == "MANAGED"
        and "iceberg" in enabled_formats.split(",")
    ):
        return "iceberg"
    return table_format


def table_from_response(info: dict) -> UnityCatalogTable:
    return UnityCatalogTable(
        full_name=info["full_name"],
        table_id=info["table_id"],
        storage_location=info.get("storage_location") or "",
        table_format=table_format_of(info),
        managed=info.get("table_type") == "MANAGED",
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


def quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def quoted_full_name(catalog_name: str, schema: str, identifier: str) -> str:
    return ".".join(quote_identifier(p) for p in (catalog_name, schema, identifier))


def escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def comment_on_table_sql(quoted_name: str, comment: str) -> str:
    return f"COMMENT ON TABLE {quoted_name} IS '{escape_sql_string(comment)}'"


def alter_column_comments_sql(quoted_name: str, comments: dict[str, str]) -> str:
    column_changes = ", ".join(
        f"{quote_identifier(column)} COMMENT '{escape_sql_string(comment)}'"
        for column, comment in comments.items()
    )
    return f"ALTER TABLE {quoted_name} ALTER COLUMN {column_changes}"


class UnityCatalogClient:
    """Schema and table registry access through the Unity Catalog REST API.

    Calls go through databricks-sdk's `WorkspaceClient`, via its low-level
    `api_client` rather than its per-resource wrappers, since those don't always
    expose every documented request/response field - e.g. table creation's
    `comment`. Shared by the Delta and Iceberg table catalogs.
    """

    def __init__(self, config: DatabricksCatalogConfig) -> None:
        self.config = config
        self.client = None
        self.lock = threading.Lock()
        self.known_schemas: set[str] = set()
        self.tables: dict[str, UnityCatalogTable] = {}
        # full_name confirmed not registered yet, so a fresh write_relation
        # doesn't re-check via REST 2-3x over before it registers the table
        self.confirmed_not_registered: set[str] = set()
        # storage uri -> table_id, so credential vending can find the table
        self.storage_uri_to_table_id: dict[str, str] = {}

    @property
    def workspace_client(self):
        if self.client is None:
            with self.lock:
                if self.client is None:
                    from databricks.sdk import WorkspaceClient

                    self.client = WorkspaceClient(**self.config.sdk_kwargs)
        return self.client

    # ── raw REST access ────────────────────────────────────────────────────────

    def request(
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

    def pages(self, path: str, *, query: dict | None = None, items_key: str):
        """Yield items across every page of a paginated Unity Catalog list endpoint."""
        query = dict(query or {})
        query.setdefault("max_results", 0)
        while True:
            response = self.request("GET", path, query=query)
            yield from response.get(items_key, [])
            next_page_token = response.get("next_page_token")
            if not next_page_token:
                return
            query["page_token"] = next_page_token

    # ── naming helpers ─────────────────────────────────────────────────────────

    def table_full_name(self, relation: PolarsRelation) -> str:
        return f"{self.config.catalog_name}.{relation.schema}.{relation.identifier}"

    def quoted_full_name(self, relation: PolarsRelation) -> str:
        if relation.schema is None or relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema/identifier")
        return quoted_full_name(
            self.config.catalog_name, relation.schema, relation.identifier
        )

    def schema_full_name(self, schema: str) -> str:
        return f"{self.config.catalog_name}.{schema}"

    # ── table registry ─────────────────────────────────────────────────────────

    def remember_table(self, info: dict) -> UnityCatalogTable:
        table = table_from_response(info)
        self.tables[table.full_name] = table
        self.storage_uri_to_table_id[table.storage_location] = table.table_id
        self.confirmed_not_registered.discard(table.full_name)
        return table

    def forget_table(self, full_name: str) -> None:
        table = self.tables.pop(full_name, None)
        if table is not None:
            self.storage_uri_to_table_id.pop(table.storage_location, None)
        self.confirmed_not_registered.discard(full_name)

    def registered_table(self, relation: PolarsRelation) -> UnityCatalogTable | None:
        full = self.table_full_name(relation)
        if full in self.tables:
            return self.tables[full]
        if full in self.confirmed_not_registered:
            return None
        from databricks.sdk.errors import NotFound

        try:
            info = self.request("GET", f"/api/2.1/unity-catalog/tables/{full}")
        except NotFound:
            self.confirmed_not_registered.add(full)
            return None
        return self.remember_table(info)

    def list_tables(self, schema: str) -> list[UnityCatalogTable]:
        from databricks.sdk.errors import NotFound

        try:
            tables = list(
                self.pages(
                    "/api/2.1/unity-catalog/tables",
                    query={
                        "catalog_name": self.config.catalog_name,
                        "schema_name": schema,
                    },
                    items_key="tables",
                )
            )
        except NotFound:
            return []
        return [self.remember_table(t) for t in tables]

    def list_relations(self, schema_relation: PolarsRelation) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        return [
            schema_relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=table.full_name.rsplit(".", 1)[-1],
                type=RelationType.Table,
                catalog=schema_relation.catalog,
            )
            for table in self.list_tables(schema_relation.schema)
        ]

    def delete_table(self, full_name: str) -> None:
        from databricks.sdk.errors import NotFound

        try:
            self.request("DELETE", f"/api/2.1/unity-catalog/tables/{full_name}")
        except NotFound:
            pass
        self.forget_table(full_name)
        self.confirmed_not_registered.add(full_name)

    # ── schema management ──────────────────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema
        if schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if schema in self.known_schemas:
            return
        from databricks.sdk.errors import DatabricksError

        logger.debug(f"Creating schema {relation.catalog}/{schema}")
        try:
            self.request(
                "POST",
                "/api/2.1/unity-catalog/schemas",
                body={"name": schema, "catalog_name": self.config.catalog_name},
            )
        except DatabricksError as exc:
            # A concurrent creator (another thread in this run, or an entirely
            # different process) may have won the race using an error shape
            # is_already_exists_error doesn't recognize - confirm via a fresh
            # GET rather than assuming failure.
            if not is_already_exists_error(
                exc, "SCHEMA_ALREADY_EXISTS"
            ) and not self.schema_exists_now(schema):
                raise
        self.known_schemas.add(schema)

    def schema_exists_now(self, schema: str) -> bool:
        from databricks.sdk.errors import NotFound

        try:
            self.request(
                "GET", f"/api/2.1/unity-catalog/schemas/{self.schema_full_name(schema)}"
            )
            return True
        except NotFound:
            return False

    def drop_schema(
        self,
        relation: PolarsRelation,
        drop_table: Callable[[UnityCatalogTable], None],
    ) -> None:
        from databricks.sdk.errors import NotFound

        schema = relation.schema
        if schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        logger.debug(f"Dropping schema {relation.catalog}/{schema}")
        for table in self.list_tables(schema):
            drop_table(table)
        try:
            self.request(
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
            for s in self.pages(
                "/api/2.1/unity-catalog/schemas",
                query={"catalog_name": self.config.catalog_name},
                items_key="schemas",
            )
        ]

    # ── Unity Catalog comments ─────────────────────────────────────────────────

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

    def current_docs(self, full_name: str) -> tuple[str | None, dict[str, str]]:
        info = self.request("GET", f"/api/2.1/unity-catalog/tables/{full_name}")
        comment = info.get("comment")
        column_comments = {
            c["name"]: c["comment"] for c in info.get("columns", []) if c.get("comment")
        }
        return comment, column_comments

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        """Writes the comment into Unity Catalog, if persist_docs_http_path is set.

        Unity Catalog's REST API has no endpoint for comments, so this runs SQL.
        """
        if not self.config.persist_docs_http_path:
            return
        current_comment, _ = self.current_docs(self.table_full_name(relation))
        if (current_comment or "") == (comment or ""):
            return
        self.run_sql_statement(
            comment_on_table_sql(self.quoted_full_name(relation), comment)
        )

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        if not self.config.persist_docs_http_path:
            return
        _, current_columns = self.current_docs(self.table_full_name(relation))
        diffs = {
            col: desired
            for col, desired in comments.items()
            if (current_columns.get(col) or "") != (desired or "")
        }
        if not diffs:
            return
        self.run_sql_statement(
            alter_column_comments_sql(self.quoted_full_name(relation), diffs)
        )
