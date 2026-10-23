import importlib.util
import subprocess
import sys
import time
from types import SimpleNamespace

import polars as pl
import pytest
from dbt.adapters.polars.catalogs import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_fabric import FabricCatalog, FabricCatalogConfig

WORKSPACE_ID = "11111111-2222-3333-4444-555555555555"
LAKEHOUSE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
DATA = pl.DataFrame({"id": [1]})


class FakeCredential:
    def __init__(self, lifetime_seconds: float = 3600) -> None:
        self.lifetime_seconds = lifetime_seconds
        self.scopes: list[str] = []

    def get_token(self, scope: str):
        self.scopes.append(scope)
        return SimpleNamespace(
            token=f"token-{len(self.scopes)}",
            expires_on=time.time() + self.lifetime_seconds,
        )


class RecordingCatalog:
    def __init__(self, name: str, calls: list) -> None:
        self.name = name
        self.calls = calls

    def __getattr__(self, method: str):
        return lambda *args, **kwargs: self.calls.append((self.name, method))


def make_catalog(workspace: str = "ws", lakehouse: str = "lh", **kwargs):
    return FabricCatalog(
        FabricCatalogConfig(
            name="fabric",
            type="fabric",
            schema="analytics",
            workspace=workspace,
            lakehouse=lakehouse,
            **kwargs,
        ),
        project_root=".",
    )


def relation(schema: str = "analytics", identifier: str = "orders") -> PolarsRelation:
    return PolarsRelation.create(
        database="fabric", schema=schema, identifier=identifier, catalog="fabric"
    )


def directory(name: str):
    return SimpleNamespace(name=name, is_directory=True)


def catalog_with_directories(
    directories: dict[str, list[str]], delta_log_source: str = "delta", **config
):
    """A catalog whose OneLake has the given directory -> child directories."""
    catalog = make_catalog(**config)
    requests = []

    def get_paths(path, recursive=False):
        requests.append(path)
        return [directory(f"{path}/{child}") for child in directories.get(path, [])]

    catalog.onelake.file_system = SimpleNamespace(get_paths=get_paths)
    catalog.onelake.delta_log_source_format = lambda relation: delta_log_source
    return catalog, requests


def routing_catalog(existing_format: str | None, **config):
    catalog = make_catalog(**config)
    calls: list = []
    catalog.delta_catalog = RecordingCatalog("delta", calls)
    catalog.loaded_iceberg_catalog = RecordingCatalog("iceberg", calls)
    catalog.onelake.table_format = lambda relation: existing_format
    catalog.onelake.create_schema = lambda relation: None
    catalog.onelake.drop_table = lambda relation: calls.append(
        ("onelake", "drop_table")
    )
    return catalog, calls


# ── paths and config ────────────────────────────────────────────────────────


def test_uri_uses_lakehouse_item_suffix_for_display_names():
    assert make_catalog().delta_catalog.get_uri(relation()) == (
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/analytics/orders"
    )


def test_uri_uses_bare_ids_for_guids():
    catalog = make_catalog(workspace=WORKSPACE_ID, lakehouse=LAKEHOUSE_ID)
    assert catalog.delta_catalog.get_uri(relation()) == (
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE_ID}/Tables/analytics/orders"
    )


def test_iceberg_warehouse_uses_the_same_item_path():
    assert make_catalog().onelake.iceberg_warehouse() == "ws/lh.Lakehouse"
    catalog = make_catalog(workspace=WORKSPACE_ID, lakehouse=LAKEHOUSE_ID)
    assert catalog.onelake.iceberg_warehouse() == f"{WORKSPACE_ID}/{LAKEHOUSE_ID}"


def test_extra_profile_keys_go_to_the_credential():
    catalog = make_catalog(exclude_managed_identity_credential=True)
    assert catalog.config.credential_kwargs == {
        "exclude_managed_identity_credential": True
    }


def test_default_table_format_is_delta():
    assert make_catalog().default_table_format == "delta"


def test_unsupported_default_table_format_is_rejected():
    with pytest.raises(DbtRuntimeError, match="not supported"):
        make_catalog(default_table_format="hudi")


# ── credentials ─────────────────────────────────────────────────────────────


def test_storage_options_use_fabric_endpoint_with_storage_token():
    catalog = make_catalog()
    credential = FakeCredential()
    catalog.onelake.credential = credential

    options = catalog.delta_catalog.get_storage_options("abfss://ws@onelake/x")

    assert options == {
        "bearer_token": "token-1",
        "use_fabric_endpoint": "true",
        "timeout": "120s",
    }
    assert credential.scopes == ["https://storage.azure.com/.default"]


def test_token_is_cached_until_close_to_expiry():
    catalog = make_catalog()
    catalog.onelake.credential = FakeCredential()

    catalog.delta_catalog.get_storage_options("uri")
    catalog.delta_catalog.get_storage_options("uri", read_only=True)

    assert len(catalog.onelake.credential.scopes) == 1


def test_token_is_refreshed_when_about_to_expire():
    catalog = make_catalog()
    catalog.onelake.credential = FakeCredential(lifetime_seconds=60)

    catalog.delta_catalog.get_storage_options("uri")
    options = catalog.delta_catalog.get_storage_options("uri")

    assert options["bearer_token"] == "token-2"


# ── table format detection ──────────────────────────────────────────────────

TABLE_PATH = "lh.Lakehouse/Tables/analytics/orders"


@pytest.mark.parametrize(
    ("children", "delta_log_source", "table_format"),
    [
        (["_delta_log"], "delta", "delta"),
        # Fabric can add Iceberg metadata to a Delta table
        (["_delta_log", "metadata"], "delta", "delta"),
        # and adds a Delta log to every Iceberg table
        (["_delta_log", "metadata", "data"], "iceberg", "iceberg"),
        (["metadata", "data"], "delta", "iceberg"),
        (["something_else"], "delta", None),
        ([], "delta", None),
    ],
)
def test_table_format_is_detected_from_the_table_directory(
    children, delta_log_source, table_format
):
    catalog, _ = catalog_with_directories({TABLE_PATH: children}, delta_log_source)

    assert catalog.existing_table_format(relation()) == table_format


def test_detected_table_format_is_cached():
    catalog, requests = catalog_with_directories({TABLE_PATH: ["_delta_log"]})

    catalog.existing_table_format(relation())
    catalog.existing_table_format(relation())

    assert requests == [TABLE_PATH]


def test_relations_are_the_schema_directories():
    catalog, _ = catalog_with_directories(
        {"lh.Lakehouse/Tables/analytics": ["orders", "customers"]}
    )

    names = [r.identifier for r in catalog.list_relations_without_caching(relation())]

    assert names == ["orders", "customers"]


# ── routing ─────────────────────────────────────────────────────────────────


def test_new_table_uses_default_table_format():
    catalog, calls = routing_catalog(None)

    catalog.write_relation(relation(), DATA, [], {})

    assert calls == [("delta", "write_relation")]
    assert catalog.onelake.table_formats == {("analytics", "orders"): "delta"}


def test_model_table_format_overrides_profile_default():
    catalog, calls = routing_catalog(None)

    catalog.write_relation(relation(), DATA, [], {"table_format": "iceberg"})

    assert calls == [("iceberg", "write_relation")]
    assert catalog.onelake.table_formats == {("analytics", "orders"): "iceberg"}


def test_write_in_other_format_drops_existing_table_first():
    catalog, calls = routing_catalog("delta", default_table_format="iceberg")

    catalog.write_relation(relation(), DATA, [], {})

    assert calls == [
        ("onelake", "drop_table"),
        ("iceberg", "_invalidate_table_cache"),
        ("iceberg", "write_relation"),
    ]


def test_existing_table_is_read_in_its_own_format():
    catalog, calls = routing_catalog("iceberg")

    catalog.get_relation(relation())

    assert calls == [("iceberg", "get_relation")]


def test_incremental_into_table_of_other_format_asks_for_full_refresh():
    catalog, _ = routing_catalog("iceberg")

    with pytest.raises(DbtRuntimeError, match="--full-refresh"):
        catalog.validate_incremental_relation(relation(), {})


def test_write_rejects_file_format():
    csv_relation = PolarsRelation.create(
        database="fabric",
        schema="analytics",
        identifier="orders",
        catalog="fabric",
        file_format="csv",
    )
    with pytest.raises(DbtRuntimeError, match="file_format"):
        make_catalog().write_relation(csv_relation, DATA, [])


# ── OneLake errors ──────────────────────────────────────────────────────────


def catalog_failing_with(error_code: str):
    from azure.core.exceptions import ResourceNotFoundError

    error = ResourceNotFoundError(message=f"OneLake error {error_code}")
    error.error_code = error_code

    def fail(*args, **kwargs):
        raise error

    catalog = make_catalog()
    catalog.onelake.file_system = SimpleNamespace(
        get_paths=fail,
        create_directory=fail,
        get_directory_client=lambda path: SimpleNamespace(delete_directory=fail),
    )
    return catalog


def test_missing_path_is_treated_as_absent():
    catalog = catalog_failing_with("PathNotFound")

    assert catalog.list_relations_without_caching(relation()) == []
    assert catalog.existing_table_format(relation()) is None
    catalog.drop_relation(relation())
    catalog.drop_schema(relation())


def test_other_not_found_errors_are_raised_as_dbt_errors():
    catalog = catalog_failing_with("NotFound")

    for call in (
        lambda: catalog.list_relations_without_caching(relation()),
        lambda: catalog.drop_relation(relation()),
        lambda: catalog.drop_schema(relation()),
        lambda: catalog.create_schema(relation()),
        catalog.list_schemas,
    ):
        with pytest.raises(DbtRuntimeError, match="OneLake error NotFound"):
            call()


def test_default_schema_cannot_be_dropped():
    with pytest.raises(DbtRuntimeError, match="dbo"):
        make_catalog().drop_schema(relation(schema="dbo"))


# ── Iceberg ─────────────────────────────────────────────────────────────────


def test_catalog_does_not_import_pyiceberg():
    code = (
        "import sys\n"
        "from dbt_polars_catalog_fabric import create_catalog, config_class\n"
        "create_catalog(config_class(name='f', type='fabric', schema='s', "
        "workspace='ws', lakehouse='lh'), '.')\n"
        "assert not [m for m in sys.modules if m.startswith('pyiceberg')]\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.skipif(
    importlib.util.find_spec("pyiceberg") is not None,
    reason="tests the error for a missing pyiceberg",
)
def test_missing_pyiceberg_error_names_the_extra():
    with pytest.raises(DbtRuntimeError, match=r"dbt-polars\[fabric,iceberg\]"):
        make_catalog().create_iceberg_catalog()


def test_iceberg_catalog_uses_onelake_iceberg_rest_endpoint():
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.rest.auth import AuthManagerFactory

    catalog = make_catalog()
    catalog.onelake.credential = FakeCredential()
    properties = catalog.create_iceberg_catalog()._catalog_properties
    auth = properties["auth"]

    assert properties["uri"] == "https://onelake.table.fabric.microsoft.com/iceberg"
    assert properties["warehouse"] == "ws/lh.Lakehouse"
    assert properties["adls.account-host"] == "onelake.blob.fabric.microsoft.com"
    assert properties["py-io-impl"] == "pyiceberg.io.fsspec.FsspecFileIO"
    manager = AuthManagerFactory.create(auth["impl"], auth["custom"])
    assert manager.auth_header() == "Bearer token-1"
