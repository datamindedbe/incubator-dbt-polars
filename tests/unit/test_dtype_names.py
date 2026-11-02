import polars as pl
import polars.datatypes
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.utils import polars_type_name, resolve_dtype_names


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


@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int64,
        pl.Datetime("us"),
        pl.Datetime("ns", "UTC"),
        pl.Duration("ms"),
        pl.Decimal(38, 7),
        pl.List(pl.Int64),
        pl.Struct({"a": pl.Int64}),
        pl.Enum(["x"]),
    ],
)
def test_polars_type_name_has_no_single_quotes_and_round_trips(dtype):
    name = polars_type_name(dtype)
    assert "'" not in name
    assert eval(name, vars(polars.datatypes)) == dtype  # noqa: S307
