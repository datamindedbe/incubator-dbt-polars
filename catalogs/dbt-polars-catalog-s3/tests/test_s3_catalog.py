import pytest
from dbt.adapters.polars.catalogs import (
    PolarsRelation,
    create_catalog,
    resolve_catalog_plugin,
)


def s3_catalog(prefix: str):
    config = resolve_catalog_plugin("s3").config_class(
        name="landing", type="s3", schema="unused", bucket="bucket", prefix=prefix
    )
    return create_catalog(config, "")


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("", "s3://bucket/erp/orders/*.CSV"),
        ("raw", "s3://bucket/raw/erp/orders/*.CSV"),
        ("/raw/", "s3://bucket/raw/erp/orders/*.CSV"),
    ],
)
def test_get_uri_uses_external_location(prefix, expected):
    relation = PolarsRelation.create(
        database="landing",
        schema="erp",
        identifier="orders",
        file_format="csv",
        external_location="erp/orders/*.CSV",
    )
    assert s3_catalog(prefix).get_uri(relation) == expected
