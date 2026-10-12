import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.impl import source_file_config


def _source(config=None, external=None):
    return {
        "source_name": "raw",
        "name": "orders",
        "config": config or {},
        "external": external,
    }


def test_source_without_external():
    assert source_file_config(_source({"file_format": "csv"})) == {
        "file_format": "csv",
        "read_options": {},
        "location": None,
    }


def test_external_location_and_file_format():
    info = source_file_config(
        _source(external={"location": "erp/orders/*.CSV", "file_format": "csv"})
    )
    assert info["location"] == "erp/orders/*.CSV"
    assert info["file_format"] == "csv"


def test_file_format_set_in_both_places_must_agree():
    info = source_file_config(
        _source({"file_format": "csv"}, {"location": "a.csv", "file_format": "csv"})
    )
    assert info["file_format"] == "csv"
    with pytest.raises(DbtRuntimeError, match="Set it in one place"):
        source_file_config(_source({"file_format": "parquet"}, {"file_format": "csv"}))


@pytest.mark.parametrize(
    "location", ["/data/orders.csv", "az://container/orders.csv", "s3://b/o.csv"]
)
def test_location_must_be_relative(location):
    with pytest.raises(DbtRuntimeError, match="relative to the catalog root"):
        source_file_config(_source(external={"location": location}))
