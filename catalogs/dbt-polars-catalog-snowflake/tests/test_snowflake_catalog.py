import subprocess
import sys
from types import SimpleNamespace

import polars as pl
import pytest
from dbt.adapters.polars.catalogs import IcebergCatalog, PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_snowflake import SnowflakeCatalog, SnowflakeCatalogConfig

DATA = pl.DataFrame({"id": [1]})


def make_config(**kwargs) -> SnowflakeCatalogConfig:
    return SnowflakeCatalogConfig(
        name="snowflake",
        type="snowflake",
        schema="analytics",
        account="myorg-myaccount",
        database_name="analytics_db",
        **kwargs,
    )


def make_catalog(**kwargs) -> SnowflakeCatalog:
    return SnowflakeCatalog(make_config(**kwargs), project_root=".")


def relation(**kwargs) -> PolarsRelation:
    return PolarsRelation.create(
        database="snowflake",
        schema="analytics",
        identifier="orders",
        catalog="snowflake",
        **kwargs,
    )


def test_properties_point_to_horizon():
    properties = make_catalog(role="transformer")._catalog_properties

    assert properties["type"] == "rest"
    assert properties["uri"] == (
        "https://myorg-myaccount.snowflakecomputing.com/polaris/api/catalog"
    )
    assert properties["warehouse"] == "ANALYTICS_DB"
    assert properties["scope"] == "session:role:TRANSFORMER"
    assert properties["header.X-Iceberg-Access-Delegation"] == "vended-credentials"


def test_other_profile_keys_go_to_pyiceberg():
    properties = make_catalog(
        credential="pat", scope="session:role:OTHER"
    )._catalog_properties

    assert properties["credential"] == "pat"
    assert properties["scope"] == "session:role:OTHER"


def test_names_are_upper_cased_in_snowflake_and_lower_cased_for_dbt():
    catalog = make_catalog()

    assert catalog._id(relation()) == ("ANALYTICS", "ORDERS")
    assert catalog.dbt_name("ORDERS") == "orders"


def test_listed_tables_get_dbt_names():
    pytest.importorskip("pyiceberg")
    catalog = make_catalog()
    listed = []
    catalog._catalog_instance = SimpleNamespace(
        list_tables=lambda namespace: (
            listed.append(namespace)
            or [("ANALYTICS", "ORDERS"), ("ANALYTICS", "CUSTOMERS")]
        ),
        list_namespaces=lambda: [("ANALYTICS",), ("RAW",)],
    )

    names = [r.identifier for r in catalog.list_relations_without_caching(relation())]

    assert names == ["orders", "customers"]
    assert listed == [("ANALYTICS",)]
    assert catalog.list_schemas() == ["analytics", "raw"]


@pytest.mark.parametrize(
    "write",
    [
        lambda catalog, rel: catalog.write_relation(rel, DATA, []),
        lambda catalog, rel: catalog.validate_incremental_relation(rel, {}),
        lambda catalog, rel: catalog.apply_snapshot_delta(rel, DATA, DATA),
    ],
)
def test_writes_reject_file_format(write):
    with pytest.raises(
        DbtRuntimeError, match="file_format is not supported for the snowflake catalog"
    ):
        write(make_catalog(), relation(file_format="parquet"))


def record_iceberg_calls(monkeypatch, method: str, returns=None) -> list[tuple]:
    """Replace an IcebergCatalog method with one recording its arguments."""
    calls: list[tuple] = []

    def record(self, *args):
        calls.append(args)
        return returns

    monkeypatch.setattr(IcebergCatalog, method, record)
    return calls


def test_column_names_are_written_as_they_are_in_dbt(monkeypatch):
    calls = record_iceberg_calls(monkeypatch, "write_relation")

    make_catalog().write_relation(
        relation(), pl.DataFrame({"id": [1], "CustomerName": ["a"]}), ["id"], {}
    )

    _, data, partition_by, _ = calls[0]
    assert data.columns == ["id", "CustomerName"]
    assert partition_by == ["id"]


def test_catalog_does_not_import_pyiceberg_until_used():
    code = (
        "import sys\n"
        "from dbt_polars_catalog_snowflake import create_catalog, config_class\n"
        "create_catalog(config_class(name='s', type='snowflake', schema='s', "
        "account='o-a', database_name='d'), '.')\n"
        "assert not [m for m in sys.modules if m.startswith('pyiceberg')]\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
