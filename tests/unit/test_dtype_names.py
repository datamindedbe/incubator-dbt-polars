import polars as pl
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.utils import resolve_dtype_names


def test_dtype_names_resolve():
    options = resolve_dtype_names(
        {
            "separator": ";",
            "schema_overrides": {"a": "String", "b": "bigint", "c": pl.Date},
            "hive_schema": {"year": "Int32"},
        }
    )
    assert options == {
        "separator": ";",
        "schema_overrides": {"a": pl.String, "b": pl.Int64, "c": pl.Date},
        "hive_schema": {"year": pl.Int32},
    }


def test_positional_schema_overrides():
    assert resolve_dtype_names({"schema_overrides": ["String", "Int64"]}) == {
        "schema_overrides": [pl.String, pl.Int64]
    }


def test_unknown_dtype_name():
    with pytest.raises(DbtRuntimeError, match="Unknown column type: 'nope'"):
        resolve_dtype_names({"schema": {"a": "nope"}})
