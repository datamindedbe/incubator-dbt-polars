from __future__ import annotations

from dbt.adapters.polars.formats.flatfile.flatfileFormat import FlatFileFormat
from dbt.adapters.polars.relation import TableFormat

import polars as pl


class ParquetFormat(FlatFileFormat):
    """Parquet table format backed by polars.

    A "table" is a directory (or object-storage prefix) containing
    ``*.parquet`` files.  The same instance works across all storage backends;
    cloud reads and writes rely on polars' built-in ``storage_options`` support
    (backed by ``object_store`` / ``fsspec`` depending on the polars build).

    See ``FlatFileFormat`` for the full contract and limitations.
    """

    table_format = TableFormat.parquet
    _extension = "parquet"

    def _scan_files(self, glob: str, opts: dict[str, str] | None) -> pl.LazyFrame:
        return pl.scan_parquet(glob, storage_options=opts)

    def _write_file(
        self, path: str, df: pl.DataFrame, opts: dict[str, str] | None
    ) -> None:
        df.write_parquet(path, storage_options=opts)
