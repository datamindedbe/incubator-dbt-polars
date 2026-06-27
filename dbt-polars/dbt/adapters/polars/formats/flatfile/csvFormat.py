from __future__ import annotations

from dbt.adapters.polars.formats.flatfile.flatfileFormat import FlatFileFormat
from dbt.adapters.polars.relation import TableFormat

import polars as pl


class CsvFormat(FlatFileFormat):
    """CSV table format backed by polars.

    A "table" is a directory (or object-storage prefix) containing
    ``*.csv`` files.  The same instance works across all storage backends;
    cloud reads and writes rely on polars' built-in ``storage_options`` support.

    Each file is expected to have a header row; schema is inferred from the
    first file polars encounters.  Note: CSV has no embedded type metadata —
    after a ``truncate()`` (header-only file), numeric columns are re-inferred
    as ``String`` until rows with data are present again.

    See ``FlatFileFormat`` for the full contract and limitations.
    """

    table_format = TableFormat.csv
    _extension = "csv"

    def _scan_files(self, glob: str, opts: dict[str, str] | None) -> pl.LazyFrame:
        return pl.scan_csv(glob, storage_options=opts)

    def _write_file(
        self, path: str, df: pl.DataFrame, opts: dict[str, str] | None
    ) -> None:
        df.write_csv(path, storage_options=opts)
