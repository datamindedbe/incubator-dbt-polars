import importlib.util
import json
import subprocess
import sys
import time
from types import SimpleNamespace

import polars as pl
import pytest
from azure.core.exceptions import ResourceNotFoundError
from dbt.adapters.polars.catalogs import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake.exceptions import TableNotFoundError

from dbt_polars_catalog_fabric import FabricCatalog, FabricCatalogConfig, onelake

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
    catalog = FabricCatalog(
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
    # Unit tests never authenticate against Azure
    catalog.onelake.credential = FakeCredential()
    return catalog


def relation(schema: str = "analytics", identifier: str = "orders") -> PolarsRelation:
    return PolarsRelation.create(
        database="fabric", schema=schema, identifier=identifier, catalog="fabric"
    )


def fake_delta_log(monkeypatch, configuration: dict | None) -> list[str]:
    """Make every Delta log have `configuration`, or not exist when None."""
    opened = []

    def delta_table(uri, storage_options):
        opened.append(uri)
        if configuration is None:
            raise TableNotFoundError("No files in log segment")
        return SimpleNamespace(
            metadata=lambda: SimpleNamespace(configuration=configuration)
        )

    monkeypatch.setattr(onelake, "DeltaTable", delta_table)
    return opened


def fake_metadata_api(monkeypatch, pages: list) -> list[dict]:
    """Answer Delta metadata API calls with (status, body) pages, in order.

    A page that is an exception is raised instead.
    """
    sent = []

    def get(url, params, headers, timeout):
        sent.append(dict(params))
        page = pages[len(sent) - 1]
        if isinstance(page, Exception):
            raise page
        status, body = page
        return SimpleNamespace(
            status_code=status, ok=status == 200, json=lambda: body, text=str(body)
        )

    monkeypatch.setattr(onelake.requests, "get", get)
    monkeypatch.setattr(onelake.time, "sleep", lambda seconds: None)
    return sent


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

ICEBERG_SOURCE = {"XTABLE_METADATA": json.dumps({"sourceTableFormat": "ICEBERG"})}


@pytest.mark.parametrize(
    ("configuration", "table_format"),
    [
        ({}, "delta"),
        # Fabric's Delta log for an Iceberg table
        (ICEBERG_SOURCE, "iceberg"),
        (None, None),
    ],
)
def test_table_format_is_read_from_the_delta_log(
    monkeypatch, configuration, table_format
):
    fake_delta_log(monkeypatch, configuration)

    assert make_catalog().existing_table_format(relation()) == table_format


def test_detected_table_format_is_cached(monkeypatch):
    opened = fake_delta_log(monkeypatch, {})
    catalog = make_catalog()

    catalog.existing_table_format(relation())
    catalog.existing_table_format(relation())

    assert len(opened) == 1


def test_relations_are_listed_across_pages(monkeypatch):
    sent = fake_metadata_api(
        monkeypatch,
        [
            (200, {"tables": [{"name": "orders"}], "next_page_token": "next"}),
            (200, {"tables": [{"name": "customers"}]}),
        ],
    )

    names = [
        r.identifier for r in make_catalog().list_relations_without_caching(relation())
    ]

    assert names == ["orders", "customers"]
    assert sent[1] == {
        "catalog_name": "lh.Lakehouse",
        "schema_name": "analytics",
        "page_token": "next",
    }


def test_schemas_are_listed_through_the_metadata_api(monkeypatch):
    fake_metadata_api(monkeypatch, [(200, {"schemas": [{"name": "dbo"}]})])

    assert make_catalog().list_schemas() == ["dbo"]


def test_metadata_api_retries_timeouts_and_server_errors(monkeypatch):
    sent = fake_metadata_api(
        monkeypatch,
        [
            onelake.requests.ReadTimeout("Read timed out"),
            (503, {"error": {"code": "ServiceUnavailable"}}),
            (200, {"schemas": [{"name": "dbo"}]}),
        ],
    )

    assert make_catalog().list_schemas() == ["dbo"]
    assert len(sent) == 3


def test_metadata_api_gives_up_after_the_last_attempt(monkeypatch):
    fake_metadata_api(monkeypatch, [onelake.requests.ReadTimeout("Read timed out")] * 4)

    with pytest.raises(onelake.requests.ReadTimeout):
        make_catalog().list_schemas()


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


@pytest.mark.parametrize("file_format", ["csv", "delta", "iceberg"])
def test_write_rejects_any_file_format(file_format):
    file_format_relation = PolarsRelation.create(
        database="fabric",
        schema="analytics",
        identifier="orders",
        catalog="fabric",
        file_format=file_format,
    )
    with pytest.raises(DbtRuntimeError, match="use table_format"):
        make_catalog().write_relation(file_format_relation, DATA, [])


def test_incremental_and_snapshot_writes_reject_file_format():
    catalog, calls = routing_catalog("delta")
    file_format_relation = relation().incorporate(file_format="iceberg")

    with pytest.raises(DbtRuntimeError, match="use table_format"):
        catalog.validate_incremental_relation(file_format_relation, {})
    with pytest.raises(DbtRuntimeError, match="use table_format"):
        catalog.apply_snapshot_delta(file_format_relation, DATA, DATA)
    assert calls == []


# ── OneLake errors ──────────────────────────────────────────────────────────


def catalog_failing_with(monkeypatch, error_code: str):
    """OneLake answers every call with a 404 carrying `error_code`."""
    error = ResourceNotFoundError(message=f"OneLake error {error_code}")
    error.error_code = error_code

    def fail(*args, **kwargs):
        raise error

    catalog = make_catalog()
    catalog.onelake.file_system = SimpleNamespace(
        create_directory=fail,
        get_directory_client=lambda path: SimpleNamespace(delete_directory=fail),
    )
    body = {"error": {"code": error_code, "message": f"OneLake error {error_code}"}}
    fake_metadata_api(monkeypatch, [(404, body)] * 5)
    return catalog


def test_missing_path_is_treated_as_absent(monkeypatch):
    catalog = catalog_failing_with(monkeypatch, "PathNotFound")

    assert catalog.list_relations_without_caching(relation()) == []
    assert catalog.list_schemas() == []
    catalog.drop_schema(relation())


def test_other_not_found_errors_are_raised_as_dbt_errors(monkeypatch):
    # e.g. a paused capacity
    catalog = catalog_failing_with(monkeypatch, "NotFound")

    for call in (
        lambda: catalog.list_relations_without_caching(relation()),
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
    manager = AuthManagerFactory.create(auth["impl"], auth["custom"])
    assert manager.auth_header() == "Bearer token-1"


def test_iceberg_warehouse_uses_ids_as_given():
    pytest.importorskip("pyiceberg")
    catalog = make_catalog(workspace=WORKSPACE_ID, lakehouse=LAKEHOUSE_ID)
    catalog.onelake.credential = FakeCredential()

    properties = catalog.create_iceberg_catalog()._catalog_properties

    assert properties["warehouse"] == f"{WORKSPACE_ID}/{LAKEHOUSE_ID}"
