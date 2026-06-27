from __future__ import annotations

import time
from abc import abstractmethod

from dbt.adapters.polars.formats.baseFormat import BaseFormat
from dbt.adapters.polars.types import TableColumnName
from dbt_common.exceptions import DbtRuntimeError

import polars as pl


class FlatFileFormat(BaseFormat):
    """Base class for flat-file table formats (Parquet, CSV, …).

    A "table" is a directory (or object-storage prefix) whose contents are
    files sharing a single extension.  Subclasses define:

    * ``table_format`` — the ``TableFormat`` enum value
    * ``_extension``   — file extension without the leading dot (e.g. ``"parquet"``)
    * ``_scan_files``  — how to lazy-scan a glob of that extension
    * ``_write_file``  — how to write a single file; receives the resolved
                         ``storage_options`` dict (or ``None``) so that
                         subclasses can forward it to cloud-aware writers

    Everything else — ``is_table``, ``scan``, ``write``, ``truncate``,
    ``append``, unsupported ``merge``/``delete_matched``, and comment no-ops
    — is implemented here once for all subclasses.

    Cloud writes are supported for subclasses that forward *opts* to a
    polars ``storage_options``-aware writer (e.g. ``df.write_parquet`` and
    ``df.write_csv`` both accept ``storage_options`` in polars ≥ 1.0).
    """

    _extension: str  # e.g. "parquet" or "csv"

    # ------------------------------------------------------------------
    # Subclass contract
    # ------------------------------------------------------------------

    @abstractmethod
    def _scan_files(self, glob: str, opts: dict[str, str] | None) -> pl.LazyFrame:
        """Return a lazy scan of *glob* using this format's reader."""
        ...

    @abstractmethod
    def _write_file(
        self, path: str, df: pl.DataFrame, opts: dict[str, str] | None
    ) -> None:
        """Write *df* to *path* using this format's writer.

        *opts* is the resolved ``storage_options`` dict (or ``None``).
        Implementations must forward it to their cloud-aware writer so that
        the same format class works on local paths, S3, GCS, and Azure.
        """
        ...

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _glob(self, uri: str) -> str:
        return f"{uri.rstrip('/')}/*.{self._extension}"

    def _data_file(self, uri: str) -> str:
        return f"{uri.rstrip('/')}/data.{self._extension}"

    def _part_file(self, uri: str) -> str:
        return f"{uri.rstrip('/')}/part-{time.time_ns()}.{self._extension}"

    # ------------------------------------------------------------------
    # BaseFormat implementation
    # ------------------------------------------------------------------

    def is_table(self, uri: str) -> bool:
        try:
            self.scan(uri).limit(0).collect()
            return True
        except Exception:
            return False

    def scan(self, uri: str) -> pl.LazyFrame:
        return self._scan_files(self._glob(uri), self._storage_options)

    def write(self, uri: str, df: pl.DataFrame) -> None:
        self._write_file(self._data_file(uri), df, self._storage_options)

    def truncate(self, uri: str) -> None:
        schema = self.scan(uri).limit(0).collect().schema
        self._write_file(
            self._data_file(uri), pl.DataFrame(schema=schema), self._storage_options
        )

    def append(
        self,
        uri: str,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None:
        self._write_file(self._part_file(uri), df, self._storage_options)

    def merge(
        self,
        uri: str,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[TableColumnName] | None = None,
    ) -> None:
        raise DbtRuntimeError(
            f"{type(self).__name__} does not support atomic merge operations.  "
            "Parquet and CSV are immutable file formats — individual files cannot "
            "be updated in place, so a merge would require a non-atomic "
            "read-modify-write that leaves the table inconsistent on failure.  "
            "Use DeltaLakeFormat (table_format: delta) for upsert workloads; "
            "Delta Lake adds a transaction log on top of Parquet that makes "
            "merge/delete operations safe."
        )

    def delete_matched(
        self,
        uri: str,
        df: pl.DataFrame,
        predicate: str,
    ) -> None:
        raise DbtRuntimeError(
            f"{type(self).__name__} does not support atomic delete_matched. "
            "See merge() for the reasoning. "
            "Use DeltaLakeFormat (table_format: delta) for delete workloads."
        )
