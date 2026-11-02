import importlib.util
import subprocess
import sys

import polars as pl
import pytest
from dbt.adapters.polars.catalogs import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_databricks.catalog import DatabricksCatalog
from dbt_polars_catalog_databricks.config import DatabricksCatalogConfig
from dbt_polars_catalog_databricks.unity_catalog import UnityCatalogTable

RELATION = PolarsRelation.create(
    database="uc", schema="schema", identifier="table", catalog="uc"
)
DATA = pl.DataFrame({"id": [1]})


def table(table_format: str) -> UnityCatalogTable:
    return UnityCatalogTable(
        "main.schema.table",
        "table-id",
        "uri",
        table_format,
        managed=table_format == "iceberg",
    )


class RecordingCatalog:
    def __init__(self, name: str, calls: list) -> None:
        self.name = name
        self.calls = calls

    def __getattr__(self, method: str):
        return lambda *args, **kwargs: self.calls.append((self.name, method))


def databricks_catalog(existing: UnityCatalogTable | None, **config):
    catalog = DatabricksCatalog(
        DatabricksCatalogConfig(
            name="uc", type="databricks", schema="schema", catalog_name="main", **config
        ),
        project_root=".",
    )
    calls: list = []
    catalog.delta_catalog = RecordingCatalog("delta", calls)
    catalog.loaded_iceberg_catalog = RecordingCatalog("iceberg", calls)
    catalog.unity_catalog.registered_table = lambda relation: existing
    catalog.unity_catalog.create_schema = lambda relation: None
    catalog.unity_catalog.delete_table = lambda full_name: calls.append(
        ("unity_catalog", "delete_table")
    )
    catalog.unity_catalog.forget_table = lambda full_name: calls.append(
        ("unity_catalog", "forget_table")
    )
    return catalog, calls


def test_new_table_uses_default_table_format():
    catalog, calls = databricks_catalog(None)

    catalog.write_relation(RELATION, DATA, [], {})

    assert calls == [("delta", "write_relation")]


def test_profile_default_table_format_applies_to_new_tables():
    catalog, calls = databricks_catalog(None, default_table_format="iceberg")

    catalog.write_relation(RELATION, DATA, [], {})

    assert calls == [("iceberg", "write_relation"), ("unity_catalog", "forget_table")]


def test_model_table_format_overrides_profile_default():
    catalog, calls = databricks_catalog(None)

    catalog.write_relation(RELATION, DATA, [], {"table_format": "iceberg"})

    assert calls == [("iceberg", "write_relation"), ("unity_catalog", "forget_table")]


def test_write_in_other_format_drops_existing_table_first():
    catalog, calls = databricks_catalog(table("delta"))

    catalog.write_relation(RELATION, DATA, [], {"table_format": "iceberg"})

    assert calls == [
        ("delta", "drop_table"),
        ("iceberg", "write_relation"),
        ("unity_catalog", "forget_table"),
    ]


def test_existing_table_is_read_in_its_registered_format():
    catalog, calls = databricks_catalog(table("iceberg"))

    catalog.get_relation(RELATION)

    assert calls == [("iceberg", "get_relation")]


def test_table_model_replaces_table_of_unmanaged_format():
    catalog, calls = databricks_catalog(table("parquet"))

    catalog.write_relation(RELATION, DATA, [], {})

    assert calls == [("delta", "drop_table"), ("delta", "write_relation")]


@pytest.mark.parametrize("existing_format", ["iceberg", "parquet"])
def test_incremental_into_table_of_other_format_asks_for_full_refresh(
    existing_format,
):
    catalog, _ = databricks_catalog(table(existing_format))

    with pytest.raises(DbtRuntimeError, match="--full-refresh"):
        catalog.validate_incremental_relation(RELATION, {})


def test_incremental_into_table_of_same_format_is_valid():
    catalog, _ = databricks_catalog(table("iceberg"))

    catalog.validate_incremental_relation(RELATION, {"table_format": "iceberg"})


def test_iceberg_table_is_dropped_through_unity_catalog():
    catalog, calls = databricks_catalog(table("iceberg"))

    catalog.drop_relation(RELATION)

    assert calls == [
        ("unity_catalog", "delete_table"),
        ("iceberg", "_invalidate_table_cache"),
    ]


def test_file_format_is_rejected():
    catalog, _ = databricks_catalog(None)

    with pytest.raises(DbtRuntimeError, match="file_format is not supported"):
        catalog.write_relation(
            RELATION.incorporate(file_format="parquet"), DATA, [], {}
        )


def test_unsupported_model_table_format_is_rejected():
    catalog, _ = databricks_catalog(None)

    with pytest.raises(DbtRuntimeError, match="not supported"):
        catalog.write_relation(RELATION, DATA, [], {"table_format": "hudi"})


def test_table_format_is_an_alias_of_default_table_format():
    config = DatabricksCatalogConfig(
        name="uc",
        type="databricks",
        schema="schema",
        catalog_name="main",
        table_format="iceberg",
    )

    assert config.default_table_format == "iceberg"


def test_default_table_format_and_its_alias_cannot_both_be_set():
    with pytest.raises(DbtRuntimeError, match="not both"):
        DatabricksCatalogConfig(
            name="uc",
            type="databricks",
            schema="schema",
            catalog_name="main",
            default_table_format="delta",
            table_format="delta",
        )


def test_catalog_does_not_import_pyiceberg():
    code = (
        "import sys\n"
        "from dbt_polars_catalog_databricks import create_catalog, config_class\n"
        "create_catalog(config_class(name='uc', type='databricks', schema='s', "
        "catalog_name='main'), '.')\n"
        "assert not [m for m in sys.modules if m.startswith('pyiceberg')]\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.skipif(
    importlib.util.find_spec("pyiceberg") is not None,
    reason="tests the error for a missing pyiceberg",
)
def test_missing_pyiceberg_error_names_the_extra():
    catalog, _ = databricks_catalog(None)

    with pytest.raises(DbtRuntimeError, match=r"dbt-polars\[databricks,iceberg\]"):
        catalog.create_iceberg_catalog()


def test_pyiceberg_authenticates_with_the_sdk_credentials():
    pytest.importorskip("pyiceberg")
    from dbt_polars_catalog_databricks.iceberg import DatabricksSdkAuthManager

    class SdkConfig:
        def authenticate(self):
            return {"Authorization": "Bearer fresh-token"}

    assert DatabricksSdkAuthManager(SdkConfig()).auth_header() == "Bearer fresh-token"


def test_iceberg_catalog_uses_unity_catalog_iceberg_rest_endpoint():
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.rest.auth import AuthManagerFactory

    from dbt_polars_catalog_databricks.iceberg import (
        DatabricksIcebergCatalog,
        DatabricksSdkAuthManager,
    )

    class SdkConfig:
        host = "https://workspace.cloud.databricks.com/"
        workspace_id = None

    class WorkspaceClient:
        config = SdkConfig()

    catalog = DatabricksIcebergCatalog(
        DatabricksCatalogConfig(
            name="uc", type="databricks", schema="schema", catalog_name="main"
        ),
        WorkspaceClient(),
    )
    properties = catalog._catalog_properties
    auth = properties["auth"]

    assert properties["uri"] == (
        "https://workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest"
    )
    assert properties["warehouse"] == "main"
    assert isinstance(
        AuthManagerFactory.create(auth["impl"], auth["custom"]),
        DatabricksSdkAuthManager,
    )
