from __future__ import annotations

from abc import ABC, abstractmethod

from dbt.adapters.polars.relation import TableFormat
from dbt.adapters.polars.types import TableColumnName

import polars as pl

# ---------------------------------------------------------------------------
# Base format class
# ---------------------------------------------------------------------------


class BaseFormat(ABC):
    """ABC for table-format implementations.

    Subclasses implement the seven abstract data-I/O methods.  Comment methods
    default to no-ops and are overridden by formats with native metadata storage
    (e.g. ``DeltaLakeFormat`` uses the transaction log).

    Third-party formats do not need to inherit from this class.
    """

    #: Corresponding ``TableFormat`` enum value — must be set on every subclass.
    table_format: TableFormat

    def __init__(self, storage_options: dict[str, str] | None = None) -> None:
        self._storage_options = storage_options or None

    # ------------------------------------------------------------------
    # Data I/O — abstract; every subclass must implement these
    # ------------------------------------------------------------------

    @abstractmethod
    def is_table(self, uri: str) -> bool:
        """Return ``True`` if *uri* points to a valid table of this format."""
        ...

    @abstractmethod
    def scan(self, uri: str) -> pl.LazyFrame:
        """Return a lazy scan of the table at *uri*."""
        ...

    @abstractmethod
    def write(self, uri: str, df: pl.DataFrame) -> None:
        """Overwrite the table at *uri* with *df*."""
        ...

    @abstractmethod
    def truncate(self, uri: str) -> None:
        """Delete all rows from the table at *uri*, preserving the schema."""
        ...

    @abstractmethod
    def append(
        self,
        uri: str,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None:
        """Append *df* to the table at *uri*."""
        ...

    @abstractmethod
    def merge(
        self,
        uri: str,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[TableColumnName] | None = None,
    ) -> None:
        """Upsert *df* into the table at *uri* matched by *predicate*."""
        ...

    @abstractmethod
    def delete_matched(
        self,
        uri: str,
        df: pl.DataFrame,
        predicate: str,
    ) -> None:
        """Delete rows at *uri* that match rows in *df* via *predicate*."""
        ...

    # ------------------------------------------------------------------
    # Comments — Null Object defaults so BaseCatalog can call these
    # unconditionally on any format.  Formats with native metadata storage
    # override them (DeltaLakeFormat uses the transaction log).
    # ------------------------------------------------------------------

    def set_table_comment(self, uri: str, comment: str) -> None:
        pass

    def get_table_comment(self, uri: str) -> str | None:
        return None

    def set_column_comments(
        self, uri: str, comments: dict[TableColumnName, str]
    ) -> None:
        pass

    def get_column_comments(self, uri: str) -> dict[TableColumnName, str]:
        return {}
