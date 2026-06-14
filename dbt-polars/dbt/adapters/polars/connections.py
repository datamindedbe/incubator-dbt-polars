from __future__ import annotations

import abc
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import ClassVar, Dict, Optional, Any, TYPE_CHECKING

from dbt.adapters.base import BaseConnectionManager
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError, DbtDatabaseError
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.contracts.connection import (
    ConnectionState,
    Credentials,
    AdapterResponse,
)
from dbt.adapters.polars.catalogs import CatalogConfig, CATALOG_CONFIG_REGISTRY
from dbt_common.clients.agate_helper import empty_table

if TYPE_CHECKING:
    import agate

logger = AdapterLogger("polars")


@dataclass
class PolarsCredentials(Credentials):
    database: str = ""
    schema: str = ""
    catalogs: list[Dict[str, Any]] = field(default_factory=list)

    _ALIASES: ClassVar[Dict[str, str]] = {}

    @property
    def type(self) -> str:
        return "polars"

    @property
    def unique_field(self) -> str:
        # TODO
        return "todo"
        # return self._catalog_config.unique_field()

    def _connection_keys(self) -> tuple:
        return ("database", "schema", "catalogs")

    @property
    def catalog_configs(self) -> dict[str, CatalogConfig]:
        return self._catalog_configs

    def __post_init__(self) -> None:
        self._catalog_configs = {
            c["name"]: self._parse_catalog(c) for c in self.catalogs
        }

        if not self.database:
            self.database = self.catalogs[0]["name"]
            logger.info(
                f"No default catalog specified via 'database', using '{self.database}'."
            )
        elif self.database not in self._catalog_configs:
            raise DbtRuntimeError(
                f"Default catalog '{self.database}' not found in catalogs"
            )

    def _parse_catalog(self, entry: Dict[str, Any]) -> CatalogConfig:
        catalog_type = entry.get("type")

        if catalog_type not in CATALOG_CONFIG_REGISTRY:
            raise DbtRuntimeError(f"Unknown catalog type: {catalog_type}")

        return CATALOG_CONFIG_REGISTRY.get(catalog_type)(
            **{k: v for k, v in entry.items()}
        )


class PolarsHandle:
    """A stub handle for the Polars adapter (no real DB connection)."""

    def close(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return PolarsCursor()


class PolarsCursor:
    """A stub cursor for the Polars adapter."""

    def __init__(self):
        self.description = []
        self._rows = []

    def execute(self, sql: str, bindings: Optional[Any] = None):
        pass

    def fetchone(self):
        return None

    def fetchall(self):
        return []

    def fetchmany(self, size: int):
        return []


class PolarsConnectionManager(BaseConnectionManager):
    TYPE: str = "polars"

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            logger.debug(f"An exception occurred while executing query:\n{sql}\n")
            raise DbtDatabaseError(str(e)) from e

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            return connection

        connection.handle = PolarsHandle()
        connection.state = ConnectionState.OPEN
        return connection

    def begin(self) -> None:
        connection = self.get_thread_connection()
        connection.transaction_open = True

    def commit(self) -> None:
        connection = self.get_thread_connection()
        connection.transaction_open = False

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        return AdapterResponse(_message="OK"), empty_table()

    @classmethod
    def get_response(cls, cursor):
        pass

    def cancel(self, connection):
        pass

    def cancel_open(self) -> Optional[list[str]]:
        return []
