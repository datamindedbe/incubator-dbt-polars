from pathlib import Path

from dbt_common.exceptions import DbtRuntimeError

import polars as pl

POLARS_TYPE_MAP: dict[str, type[pl.DataType]] = {
    # Polars-native names
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "UInt8": pl.UInt8,
    "UInt16": pl.UInt16,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Boolean": pl.Boolean,
    "Utf8": pl.Utf8,
    "String": pl.Utf8,
    "Categorical": pl.Categorical,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
    "Time": pl.Time,
    "Duration": pl.Duration,
    "Decimal": pl.Decimal,
    "Binary": pl.Binary,
    # SQL aliases (lowercase)
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "tinyint": pl.Int8,
    "smallint": pl.Int16,
    "integer": pl.Int32,
    "int": pl.Int32,
    "int4": pl.Int32,
    "int2": pl.Int16,
    "bigint": pl.Int64,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "float4": pl.Float32,
    "real": pl.Float32,
    "float32": pl.Float32,
    "float8": pl.Float64,
    "double": pl.Float64,
    "float64": pl.Float64,
    "float": pl.Float64,
    "double precision": pl.Float64,
    "varchar": pl.Utf8,
    "text": pl.Utf8,
    "string": pl.Utf8,
    "char": pl.Utf8,
    "character varying": pl.Utf8,
    "utf8": pl.Utf8,
    "categorical": pl.Categorical,
    "boolean": pl.Boolean,
    "bool": pl.Boolean,
    "date": pl.Date,
    "timestamp": pl.Datetime,
    "datetime": pl.Datetime,
    "time": pl.Time,
    "duration": pl.Duration,
    "decimal": pl.Decimal,
    "numeric": pl.Decimal,
    "binary": pl.Binary,
}

DTYPE_READ_OPTIONS = ("schema", "schema_overrides", "hive_schema")


def resolve_relative_path(path: str | Path, project_root: str) -> Path:
    """Resolve `path` against `project_root` if it is not already absolute."""
    path = Path(path)
    if not path.is_absolute():
        path = Path(project_root) / path
    return path.resolve()


def resolve_polars_type(type_str: str) -> type[pl.DataType]:
    key = type_str.split("(")[0].strip()
    dtype = POLARS_TYPE_MAP.get(key) or POLARS_TYPE_MAP.get(key.lower())
    if dtype is None:
        raise DbtRuntimeError(f"Unknown column type: '{type_str}'")
    return dtype


def polars_type_name(dtype: pl.DataType) -> str:
    """Polars repr of `dtype` with double quotes, so it can sit inside a SQL string
    literal and still evaluates to `dtype` in Python."""
    return str(dtype).replace("'", '"')


def resolve_dtype_names(read_options: dict) -> dict:
    """read_options with dtype names in schema options resolved to Polars types."""

    def resolve(dtype):
        return resolve_polars_type(dtype) if isinstance(dtype, str) else dtype

    resolved = dict(read_options)
    for key in DTYPE_READ_OPTIONS:
        value = resolved.get(key)
        if isinstance(value, dict):
            resolved[key] = {column: resolve(dtype) for column, dtype in value.items()}
        elif isinstance(value, list):
            resolved[key] = [resolve(dtype) for dtype in value]
    return resolved
