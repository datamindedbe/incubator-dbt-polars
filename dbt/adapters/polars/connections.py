from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from dbt.adapters.base.connections import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    ConnectionState,
    Credentials,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.clients.agate_helper import empty_table
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.polars.catalogs import CATALOG_CONFIG_REGISTRY, CatalogConfig

if TYPE_CHECKING:
    import agate

logger = AdapterLogger("polars")


@dataclass
class PolarsCredentials(Credentials):
    database: str = ""
    schema: str = ""
    catalogs: list[dict[str, Any]] = field(default_factory=list)

    _ALIASES: ClassVar[dict[str, str]] = {"catalog": "database"}

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
    def catalog(self) -> str:
        return self.database

    @property
    def catalog_configs(self) -> dict[str, CatalogConfig]:
        return self._catalog_configs

    @classmethod
    def __pre_deserialize__(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Fold single-catalog shorthand fields into a one-entry `catalogs` list."""
        data = dict(data)

        if "catalogs" in data:
            if data.get("schema"):
                raise DbtRuntimeError(
                    "Profile sets 'catalogs' explicitly, so the top-level 'schema' "
                    "must be removed - set 'schema' on each catalog entry instead."
                )
            if data.get("catalog_type"):
                raise DbtRuntimeError(
                    "Profile sets both 'catalogs' and 'catalog_type' - 'catalog_type' "
                    "is only used for the single-catalog shorthand. Move its value "
                    "into the appropriate catalog entry's 'type'."
                )
            return data

        catalog_type = data.pop("catalog_type", None)
        if catalog_type is None:
            raise DbtRuntimeError(
                "Profile must define either 'catalogs' (multi-catalog setup) or "
                "'catalog_type' plus the catalog's own options (single-catalog "
                "shorthand)."
            )
        name = data.get("database") or "default"
        reserved = {"database", "schema"}
        entry = {"name": name, "type": catalog_type}
        for key in list(data):
            if key not in reserved:
                entry[key] = data.pop(key)
        data["catalogs"] = [entry]
        return data

    def __post_init__(self) -> None:
        for entry in self.catalogs:
            if not entry.get("schema"):
                if not self.schema:
                    raise DbtRuntimeError(
                        f"Catalog '{entry.get('name')}' has no schema, and no "
                        "top-level schema was set on the profile."
                    )
                entry["schema"] = self.schema

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

        self.schema = self._catalog_configs[self.database].schema

    def _parse_catalog(self, entry: dict[str, Any]) -> CatalogConfig:
        catalog_type = entry.get("type")

        if catalog_type not in CATALOG_CONFIG_REGISTRY:
            raise DbtRuntimeError(f"Unknown catalog type: {catalog_type}")

        config_cls = CATALOG_CONFIG_REGISTRY[catalog_type]
        return config_cls(**{k: v for k, v in entry.items()})


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

    def execute(self, sql: str, bindings: Any | None = None):
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
        limit: int | None = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        return AdapterResponse(_message="OK"), empty_table()

    @classmethod
    def get_response(cls, cursor):
        pass

    def cancel(self, connection):
        pass

    def cancel_open(self) -> list[str] | None:
        return []
