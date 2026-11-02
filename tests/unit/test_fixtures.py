from datetime import date, datetime, time, timezone
from decimal import Decimal

import polars as pl
import pytest

from dbt.adapters.polars.fixtures import fixture_frame


def test_rows_are_cast_to_schema():
    schema = {
        "id": pl.Int32,
        "amount": pl.Decimal(10, 2),
        "is_active": pl.Boolean,
        "day": pl.Date,
        "created_at": pl.Datetime("us", "UTC"),
        "start": pl.Time,
        "tags": pl.List(pl.String),
    }
    rows = [
        {
            "id": "1",
            "amount": 7.77,
            "is_active": "TRUE",
            "day": "2024-01-02",
            "created_at": "2024-01-02 10:00:00+00:00",
            "start": "12:30:00",
            "tags": ["a"],
        }
    ]
    frame = fixture_frame(rows, schema)
    assert frame.schema == pl.Schema(schema)
    assert frame.row(0) == (
        1,
        Decimal("7.77"),
        True,
        date(2024, 1, 2),
        datetime(2024, 1, 2, 10, tzinfo=timezone.utc),
        time(12, 30),
        ["a"],
    )


def test_missing_columns_are_null_and_keys_case_insensitive():
    frame = fixture_frame(
        [{"ID": 1}, {"Name": "b"}], {"id": pl.Int64, "name": pl.String}
    )
    assert frame.to_dicts() == [{"id": 1, "name": None}, {"id": None, "name": "b"}]


def test_no_rows():
    frame = fixture_frame([], {"id": pl.Int64})
    assert frame.schema == pl.Schema({"id": pl.Int64})
    assert frame.height == 0


def test_invalid_value_raises():
    with pytest.raises(pl.exceptions.InvalidOperationError):
        fixture_frame([{"amount": "abc"}], {"amount": pl.Float64})


@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int8,
        pl.UInt64,
        pl.Float32,
        pl.Datetime("ns", "Europe/Brussels"),
        pl.Decimal(10, 2),
        pl.Duration("us"),
        pl.List(pl.Int64),
        pl.Struct({"a": pl.Int64}),
    ],
)
def test_dtype_repr_is_valid_python(dtype):
    """get_fixture_sql writes str(dtype) into the fixture code."""
    namespace: dict = {}
    exec("from polars.datatypes import *", namespace)  # noqa: S102
    assert eval(str(dtype), namespace) == dtype  # noqa: S307
