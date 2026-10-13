from types import SimpleNamespace

import pytest
from dbt.artifacts.resources.v1.source_definition import ExternalTable
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.catalogs import external_location_of
from dbt.adapters.polars.impl import source_external_location, source_file_config
from dbt.adapters.polars.relation import PolarsRelation


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
        "external": {},
    }


def test_unset_external_keys_are_dropped():
    # The graph lists every ExternalTable field, the unset ones as None
    info = source_file_config(
        _source(external={"location": "erp/*.CSV", "file_format": None})
    )
    assert info["external"] == {"location": "erp/*.CSV"}


def test_source_file_config_does_not_validate():
    # A misconfigured source must only fail the runs that read it
    info = source_file_config(
        _source(external={"location": "/abs.csv", "file_format": "csv"})
    )
    assert info["external"] == {"location": "/abs.csv", "file_format": "csv"}


def test_external_location():
    assert source_external_location("raw.orders", {"location": "a.CSV"}) == "a.CSV"
    assert source_external_location("raw.orders", {}) is None


@pytest.mark.parametrize("key", ["file_format", "row_format", "options"])
def test_external_keys_other_than_location_raise(key):
    with pytest.raises(DbtRuntimeError, match=f"external.{key}"):
        source_external_location("raw.orders", {"location": "a.csv", key: "x"})


def _relation(external_location):
    return PolarsRelation.create(
        database="landing",
        schema="erp",
        identifier="orders",
        external_location=external_location,
    )


def test_external_location_of():
    assert external_location_of(_relation("erp/*.CSV")) == "erp/*.CSV"
    assert external_location_of(_relation(None)) is None


@pytest.mark.parametrize(
    "location", ["/data/orders.csv", "az://container/orders.csv", "s3://b/o.csv"]
)
def test_external_location_must_be_relative(location):
    with pytest.raises(DbtRuntimeError, match="relative to the catalog root"):
        external_location_of(_relation(location))


@pytest.mark.parametrize(
    ("external", "expected"),
    [(ExternalTable(location="erp/*.CSV"), "erp/*.CSV"), (ExternalTable(), None)],
)
def test_create_from_carries_external_location(external, expected):
    relation_config = SimpleNamespace(
        database="landing",
        schema="erp",
        identifier="orders",
        config={"file_format": "csv"},
        quoting_dict={},
        external=external,
    )
    relation = PolarsRelation.create_from(SimpleNamespace(quoting={}), relation_config)
    assert relation.external_location == expected
    assert relation.file_format == "csv"
