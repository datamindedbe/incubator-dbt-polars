from dataclasses import dataclass

from dbt.adapters.base.column import Column

import polars as pl
from dbt.adapters.polars.utils import POLARS_TYPE_MAP


@dataclass
class PolarsColumn(Column):
    """Column whose type checks understand Polars type names such as `Float64`."""

    def polars_type(self) -> type[pl.DataType] | None:
        name = self.dtype.split("(")[0].strip()
        return POLARS_TYPE_MAP.get(name) or POLARS_TYPE_MAP.get(name.lower())

    @property
    def data_type(self) -> str:
        return self.dtype

    def is_string(self) -> bool:
        return self.polars_type() == pl.String

    def is_float(self) -> bool:
        polars_type = self.polars_type()
        return polars_type is not None and polars_type.is_float()

    def is_integer(self) -> bool:
        polars_type = self.polars_type()
        return polars_type is not None and polars_type.is_integer()

    def is_numeric(self) -> bool:
        polars_type = self.polars_type()
        return polars_type is not None and polars_type.is_decimal()
