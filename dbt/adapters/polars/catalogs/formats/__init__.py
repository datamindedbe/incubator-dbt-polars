from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass(frozen=True)
class FileFormatSpec:
    sink: Callable[..., Any]
    scan: Callable[..., pl.LazyFrame]
    auto_cast: bool


FILE_FORMATS: dict[str, FileFormatSpec] = {
    "parquet": FileFormatSpec(
        sink=pl.LazyFrame.sink_parquet,
        scan=pl.scan_parquet,
        auto_cast=False,
    ),
    "csv": FileFormatSpec(
        sink=pl.LazyFrame.sink_csv,
        scan=pl.scan_csv,
        auto_cast=True,
    ),
    "ndjson": FileFormatSpec(
        sink=pl.LazyFrame.sink_ndjson,
        scan=pl.scan_ndjson,
        auto_cast=True,
    ),
}
