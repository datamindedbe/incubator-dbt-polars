import pytest
from dbt.adapters.polars.catalogs import (
    PolarsRelation,
    create_catalog,
    resolve_catalog_plugin,
)


def azure_catalog(prefix: str):
    config = resolve_catalog_plugin("azure").config_class(
        name="landing",
        type="azure",
        schema="unused",
        account_name="account",
        container="container",
        prefix=prefix,
    )
    return create_catalog(config, "")


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("", "az://container/erp/orders/*.CSV"),
        ("raw", "az://container/raw/erp/orders/*.CSV"),
        ("/raw/", "az://container/raw/erp/orders/*.CSV"),
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
    assert azure_catalog(prefix).get_uri(relation) == expected
