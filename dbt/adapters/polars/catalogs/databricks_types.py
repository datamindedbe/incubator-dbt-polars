"""Maps Polars dtypes to Unity Catalog column type descriptors.

Used only to populate column metadata when registering a table via the Unity
Catalog Tables API (`POST /api/2.1/unity-catalog/tables`). Unity Catalog does
not validate this against the table's actual Parquet/Delta physical types, so
this is a best-effort, display/documentation-oriented mapping, not a source of
truth for how data is physically stored.
"""

from __future__ import annotations

import json
from typing import Any

from dbt.adapters.events.logging import AdapterLogger

import polars as pl

logger = AdapterLogger("polars")

# Delta/Unity Catalog has no unsigned integer types; describe them as the
# next signed type wide enough to hold every value (mirrors IcebergCatalog's
# _cast_unsigned_to_signed rationale in icebergCatalog.py).
UNSIGNED_TO_SIGNED_NAME_TEXT: dict[type, tuple[str, str]] = {
    pl.UInt8: ("SHORT", "smallint"),
    pl.UInt16: ("INT", "int"),
    pl.UInt32: ("LONG", "bigint"),
    pl.UInt64: ("DECIMAL", "decimal(20,0)"),
}

SIMPLE_NAME_TEXT: dict[type, tuple[str, str]] = {
    pl.Boolean: ("BOOLEAN", "boolean"),
    pl.Int8: ("BYTE", "tinyint"),
    pl.Int16: ("SHORT", "smallint"),
    pl.Int32: ("INT", "int"),
    pl.Int64: ("LONG", "bigint"),
    pl.Float32: ("FLOAT", "float"),
    pl.Float64: ("DOUBLE", "double"),
    pl.Utf8: ("STRING", "string"),
    pl.Categorical: ("STRING", "string"),
    pl.Date: ("DATE", "date"),
    pl.Binary: ("BINARY", "binary"),
}


def decimal_name_text(dtype: pl.Decimal) -> tuple[str, str]:
    precision = dtype.precision if dtype.precision is not None else 38
    scale = dtype.scale or 0
    return "DECIMAL", f"decimal({precision},{scale})"


def datetime_name_text(dtype: pl.Datetime) -> tuple[str, str]:
    if dtype.time_zone:
        return "TIMESTAMP", "timestamp"
    return "TIMESTAMP_NTZ", "timestamp_ntz"


def json_value(dtype: Any) -> object:
    """Return the JSON-able value (str or dict) describing dtype, Spark-schema style."""
    base = type(dtype)

    if base in UNSIGNED_TO_SIGNED_NAME_TEXT:
        return UNSIGNED_TO_SIGNED_NAME_TEXT[base][1]
    if base in SIMPLE_NAME_TEXT:
        return SIMPLE_NAME_TEXT[base][1]
    if isinstance(dtype, pl.Decimal):
        return decimal_name_text(dtype)[1]
    if isinstance(dtype, pl.Datetime):
        return datetime_name_text(dtype)[1]
    if isinstance(dtype, (pl.List, pl.Array)):
        return {
            "type": "array",
            "elementType": json_value(dtype.inner),
            "containsNull": True,
        }
    if isinstance(dtype, pl.Struct):
        return {
            "type": "struct",
            "fields": [
                {
                    "name": f.name,
                    "type": json_value(f.dtype),
                    "nullable": True,
                    "metadata": {},
                }
                for f in dtype.fields
            ],
        }
    logger.debug(
        f"No Unity Catalog type mapping for Polars dtype {dtype!r}; "
        "describing this column as STRING in the registered table metadata."
    )
    return "string"


def polars_dtype_to_uc_type(dtype: Any) -> tuple[str, str, str]:
    """Return (type_name, type_text, type_json) for a Unity Catalog table column."""
    base = type(dtype)

    if base in UNSIGNED_TO_SIGNED_NAME_TEXT:
        name, text = UNSIGNED_TO_SIGNED_NAME_TEXT[base]
    elif base in SIMPLE_NAME_TEXT:
        name, text = SIMPLE_NAME_TEXT[base]
    elif isinstance(dtype, pl.Decimal):
        name, text = decimal_name_text(dtype)
    elif isinstance(dtype, pl.Datetime):
        name, text = datetime_name_text(dtype)
    elif isinstance(dtype, (pl.List, pl.Array)):
        _, inner_text, _ = polars_dtype_to_uc_type(dtype.inner)
        name, text = "ARRAY", f"array<{inner_text}>"
    elif isinstance(dtype, pl.Struct):
        field_texts = []
        for f in dtype.fields:
            _, f_text, _ = polars_dtype_to_uc_type(f.dtype)
            field_texts.append(f"{f.name}:{f_text}")
        name, text = "STRUCT", f"struct<{','.join(field_texts)}>"
    else:
        name, text = "STRING", "string"

    return name, text, json.dumps(json_value(dtype))
