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
    with pytest.raises(DbtRuntimeError, match="always writes Iceberg"):
        write(make_catalog(), relation(file_format="parquet"))


def record_iceberg_calls(monkeypatch, method: str, returns=None) -> list[tuple]:
    """Replace an IcebergCatalog method with one recording its arguments."""
    calls: list[tuple] = []

    def record(self, *args):
        calls.append(args)
        return returns

    monkeypatch.setattr(IcebergCatalog, method, record)
    return calls


def test_written_columns_are_upper_cased(monkeypatch):
    calls = record_iceberg_calls(monkeypatch, "write_relation")

    make_catalog().write_relation(
        relation(), pl.DataFrame({"id": [1], "Name": ["a"]}), ["id"], {}
    )

    _, data, partition_by, _ = calls[0]
    assert data.columns == ["ID", "NAME"]
    assert partition_by == ["ID"]


def test_merge_keys_and_except_columns_are_upper_cased(monkeypatch):
    calls = record_iceberg_calls(monkeypatch, "merge_relation")

    make_catalog().merge_relation(
        relation(), pl.DataFrame({"id": [1], "name": ["a"]}), ["id"], ["name"]
    )

    _, df, keys, except_cols, *_ = calls[0]
    assert df.columns == ["ID", "NAME"]
    assert keys == ["ID"]
    assert except_cols == ["NAME"]


def test_read_columns_are_lower_cased(monkeypatch):
    record_iceberg_calls(
        monkeypatch, "get_relation", pl.LazyFrame({"ID": [1], "NAME": ["a"]})
    )

    frame = make_catalog().get_relation(relation())

    assert frame.collect_schema().names() == ["id", "name"]


def test_columns_differing_only_in_case_are_rejected(monkeypatch):
    record_iceberg_calls(monkeypatch, "append_relation")

    with pytest.raises(DbtRuntimeError, match="only differ in case"):
        make_catalog().append_relation(relation(), pl.DataFrame({"id": [1], "ID": [2]}))


def test_column_names_are_kept_when_upper_casing_is_off(monkeypatch):
    calls = record_iceberg_calls(monkeypatch, "write_relation")

    make_catalog(uppercase_column_names=False).write_relation(
        relation(), pl.DataFrame({"id": [1]}), ["id"], {}
    )

    _, data, partition_by, _ = calls[0]
    assert data.columns == ["id"]
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
