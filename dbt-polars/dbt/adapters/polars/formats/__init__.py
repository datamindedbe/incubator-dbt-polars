from typing import Literal

from dbt.adapters.polars.formats.baseFormat import (
    BaseFormat,
)
from dbt.adapters.polars.formats.flatfile.csvFormat import CsvFormat
from dbt.adapters.polars.formats.flatfile.flatfileFormat import FlatFileFormat
from dbt.adapters.polars.formats.flatfile.parquetFormat import ParquetFormat
from dbt.adapters.polars.formats.table.deltaFormat import DeltaLakeFormat

# String-literal type for FORMAT_REGISTRY keys; use for format-name config fields:
#   table_format: TableFormatStr = "delta"
TableFormatStr = Literal["delta", "parquet", "csv"]

FORMAT_REGISTRY: dict[str, type[BaseFormat]] = {
    "delta": DeltaLakeFormat,
    "parquet": ParquetFormat,
    "csv": CsvFormat,
}

__all__ = [
    "BaseFormat",
    "CsvFormat",
    "DeltaLakeFormat",
    "FlatFileFormat",
    "ParquetFormat",
    "TableFormatStr",
    "FORMAT_REGISTRY",
]
