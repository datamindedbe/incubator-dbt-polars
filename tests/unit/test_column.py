import pytest

from dbt.adapters.polars.column import PolarsColumn


@pytest.mark.parametrize(
    "dtype, is_string, is_float, is_integer, is_numeric",
    [
        ("String", True, False, False, False),
        ("Float64", False, True, False, False),
        ("Float32", False, True, False, False),
        ("Int64", False, False, True, False),
        ("UInt8", False, False, True, False),
        ("Decimal(precision=38, scale=7)", False, False, False, True),
        ("Boolean", False, False, False, False),
        ("Datetime(time_unit='us', time_zone=None)", False, False, False, False),
        ("List(Int64)", False, False, False, False),
    ],
)
def test_type_checks_understand_polars_type_names(
    dtype, is_string, is_float, is_integer, is_numeric
):
    column = PolarsColumn("c", dtype)
    assert column.is_string() == is_string
    assert column.is_float() == is_float
    assert column.is_integer() == is_integer
    assert column.is_numeric() == is_numeric


@pytest.mark.parametrize("dtype", ["String", "Decimal(precision=38, scale=7)"])
def test_data_type_is_the_polars_type_name(dtype):
    assert PolarsColumn("c", dtype).data_type == dtype
