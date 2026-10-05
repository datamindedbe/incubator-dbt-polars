import time
from types import SimpleNamespace

import pytest
from dbt.adapters.polars.catalogs import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_fabric import FabricCatalog, FabricCatalogConfig

WORKSPACE_ID = "11111111-2222-3333-4444-555555555555"
LAKEHOUSE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


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
    return catalog


def relation(schema: str = "analytics", identifier: str = "orders") -> PolarsRelation:
    return PolarsRelation.create(
        database="fabric", schema=schema, identifier=identifier, catalog="fabric"
    )


def test_uri_uses_lakehouse_item_suffix_for_display_names():
    assert make_catalog().get_uri(relation()) == (
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Tables/analytics/orders"
    )


def test_uri_uses_bare_ids_for_guids():
    catalog = make_catalog(workspace=WORKSPACE_ID, lakehouse=LAKEHOUSE_ID)
    assert catalog.get_uri(relation()) == (
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE_ID}/Tables/analytics/orders"
    )


def test_extra_profile_keys_go_to_the_credential():
    catalog = make_catalog(exclude_managed_identity_credential=True)
    assert catalog.config.credential_kwargs == {
        "exclude_managed_identity_credential": True
    }


def test_storage_options_use_fabric_endpoint_with_storage_token():
    catalog = make_catalog()
    credential = FakeCredential()
    catalog.credential = credential

    options = catalog.get_storage_options("abfss://ws@onelake/x")

    assert options == {
        "bearer_token": "token-1",
        "use_fabric_endpoint": "true",
        "timeout": "120s",
    }
    assert credential.scopes == ["https://storage.azure.com/.default"]


def test_token_is_cached_until_close_to_expiry():
    catalog = make_catalog()
    catalog.credential = FakeCredential()

    catalog.get_storage_options("uri")
    catalog.get_storage_options("uri", read_only=True)

    assert len(catalog.credential.scopes) == 1


def test_token_is_refreshed_when_about_to_expire():
    catalog = make_catalog()
    catalog.credential = FakeCredential(lifetime_seconds=60)

    catalog.get_storage_options("uri")
    options = catalog.get_storage_options("uri")

    assert options["bearer_token"] == "token-2"


def test_write_rejects_non_delta_file_format():
    csv_relation = PolarsRelation.create(
        database="fabric",
        schema="analytics",
        identifier="orders",
        catalog="fabric",
        file_format="csv",
    )
    with pytest.raises(DbtRuntimeError, match="file_format"):
        make_catalog().write_relation(csv_relation, None, [])


def catalog_failing_with(error_code: str):
    from azure.core.exceptions import ResourceNotFoundError

    error = ResourceNotFoundError(message=f"OneLake error {error_code}")
    error.error_code = error_code

    def fail(*args, **kwargs):
        raise error

    catalog = make_catalog()
    catalog.file_system = SimpleNamespace(
        get_paths=fail,
        create_directory=fail,
        get_directory_client=lambda path: SimpleNamespace(delete_directory=fail),
    )
    return catalog


def test_missing_path_is_treated_as_absent():
    catalog = catalog_failing_with("PathNotFound")

    assert catalog.list_relations_without_caching(relation()) == []
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
