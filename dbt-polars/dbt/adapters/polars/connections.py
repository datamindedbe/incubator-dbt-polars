from contextlib import contextmanager
from dataclasses import dataclass
from typing import ClassVar, List, Optional, Tuple

import agate

from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState, Credentials
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("polars")


@dataclass
class PolarsCredentials(Credentials):
    """
    Profile credentials for dbt-polars.

    Local target (default):
        type: polars
        path: /path/to/catalog
        database: local
        schema: main

    Databricks Unity Catalog target:
        type: polars
        host: https://adb-<id>.azuredatabricks.net
        catalog: my_catalog
        database: my_catalog
        schema: my_schema
        client_id: "<service-principal-application-id>"
        client_secret: "<oauth-secret>"  # OAuth M2M secret for the service principal
    """

    # local
    path: str = ""

    # databricks unity catalog
    host: str = ""
    catalog: str = ""
    client_id: str = ""             # Databricks service principal application ID
    client_secret: str = ""         # OAuth secret for the service principal

    _ALIASES: ClassVar[dict] = {}

    @property
    def type(self) -> str:
        return "polars"

    @property
    def unique_field(self) -> str:
        return self.host or self.path

    def _connection_keys(self) -> Tuple[str, ...]:
        if self.host:
            return ("host", "catalog", "client_id", "database", "schema")
        return ("path", "database", "schema")


class PolarsConnectionHandle:
    """Carries the catalog base path for the local filesystem target."""

    def __init__(self, path: str) -> None:
        self.path = path

    def close(self) -> None:
        pass


class DatabricksConnectionHandle:
    """Carries a live Databricks API client for the Unity Catalog target."""

    def __init__(self, client: "Any", catalog: str, schema: str) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = schema

    def close(self) -> None:
        pass


class PolarsConnectionManager(BaseConnectionManager):
    TYPE = "polars"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection
        creds = connection.credentials
        if creds.host:
            from dbt.adapters.polars._databricks import DatabricksClient, get_oauth_token
            token = get_oauth_token(creds.host, creds.client_id, creds.client_secret)
            client = DatabricksClient(creds.host, token)
            connection.handle = DatabricksConnectionHandle(
                client=client,
                catalog=creds.catalog or creds.database,
                schema=creds.schema,
            )
        else:
            connection.handle = PolarsConnectionHandle(creds.path)
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        return AdapterResponse(_message="OK")

    def cancel(self, connection: Connection) -> None:
        pass

    @classmethod
    def cancel_open(cls) -> List[str]:
        return []

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        if not fetch:
            return AdapterResponse(_message="OK"), agate.Table([])

        import re as _re
        from dbt.adapters.polars._catalog import build_sql_context, cte_names, strip_qualifiers

        cleaned = strip_qualifiers(sql)
        exclude = cte_names(cleaned)
        needed = set(_re.findall(r'"(\w+)"', cleaned)) - exclude

        handle = self.get_thread_connection().handle
        if isinstance(handle, DatabricksConnectionHandle):
            from dbt.adapters.polars._databricks import build_uc_sql_context
            ctx = build_uc_sql_context(handle.client, handle.catalog, handle.schema, exclude=exclude, needed=needed)
        else:
            ctx = build_sql_context(handle.path, exclude=exclude)

        df = ctx.execute(cleaned, eager=True)
        if limit is not None:
            df = df.head(limit)

        return AdapterResponse(_message="OK"), agate.Table(
            df.rows(), column_names=df.columns
        )

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc
