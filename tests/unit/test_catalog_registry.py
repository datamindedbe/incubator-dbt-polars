import sys
import types
from dataclasses import dataclass

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.catalogs import (
    BUILTIN_CATALOGS,
    CatalogConfig,
    LocalCatalog,
    create_catalog,
    resolve_catalog_plugin,
)
from dbt.adapters.polars.connections import PolarsCredentials


@dataclass
class FakeCatalogConfig(CatalogConfig):
    name: str
    type: str
    schema: str
    root: str

    def unique_field(self) -> str:
        return self.root

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "root")


def create_fake_catalog(config, project_root):
    return LocalCatalog(config, project_root)


@pytest.fixture
def register_module(monkeypatch):
    def register(name: str, **attributes):
        module = types.ModuleType(name)
        for key, value in attributes.items():
            setattr(module, key, value)
        monkeypatch.setitem(sys.modules, name, module)
        resolve_catalog_plugin.cache_clear()

    yield register
    resolve_catalog_plugin.cache_clear()


def test_builtin_types_resolve_without_import():
    assert resolve_catalog_plugin("local") is BUILTIN_CATALOGS["local"]


def test_extension_module_is_resolved_by_prefix(register_module):
    register_module(
        "dbt_polars_catalog_fake",
        config_class=FakeCatalogConfig,
        create_catalog=create_fake_catalog,
    )

    plugin = resolve_catalog_plugin("fake")

    assert plugin.config_class is FakeCatalogConfig
    assert plugin.create_catalog is create_fake_catalog


def test_missing_extension_suggests_package():
    with pytest.raises(
        DbtRuntimeError, match="pip install dbt-polars-catalog-my-custom"
    ):
        resolve_catalog_plugin("my_custom")


def test_extension_internal_import_error_propagates(monkeypatch):
    def failing_import(name):
        raise ModuleNotFoundError("No module named 'requests'", name="requests")

    monkeypatch.setattr("dbt.adapters.polars.catalogs.import_module", failing_import)
    resolve_catalog_plugin.cache_clear()

    with pytest.raises(ModuleNotFoundError, match="requests"):
        resolve_catalog_plugin("broken")


def test_invalid_type_is_rejected():
    with pytest.raises(DbtRuntimeError, match="Invalid catalog type"):
        resolve_catalog_plugin("Not-Valid")


def test_module_without_config_class_is_rejected(register_module):
    register_module("dbt_polars_catalog_noconfig", create_catalog=create_fake_catalog)

    with pytest.raises(DbtRuntimeError, match="config_class"):
        resolve_catalog_plugin("noconfig")


def test_module_without_create_catalog_is_rejected(register_module):
    register_module("dbt_polars_catalog_nocreate", config_class=FakeCatalogConfig)

    with pytest.raises(DbtRuntimeError, match="create_catalog"):
        resolve_catalog_plugin("nocreate")


def test_create_catalog_must_return_a_base_catalog(register_module):
    register_module(
        "dbt_polars_catalog_notacatalog",
        config_class=FakeCatalogConfig,
        create_catalog=lambda config, project_root: object(),
    )
    config = FakeCatalogConfig(
        name="c", type="notacatalog", schema="s", root="/tmp/data"
    )

    with pytest.raises(DbtRuntimeError, match="expected a BaseCatalog"):
        create_catalog(config, "")


def test_profile_with_extension_catalog(register_module, tmp_path):
    register_module(
        "dbt_polars_catalog_fake",
        config_class=FakeCatalogConfig,
        create_catalog=create_fake_catalog,
    )

    creds = PolarsCredentials.from_dict(
        {
            "schema": "my_schema",
            "catalog_type": "fake",
            "root": str(tmp_path),
        }
    )

    config = creds.catalog_configs["default"]
    assert isinstance(config, FakeCatalogConfig)
    assert isinstance(create_catalog(config, ""), LocalCatalog)


def test_unexpected_catalog_option_gives_readable_error():
    with pytest.raises(DbtRuntimeError, match="Invalid options for catalog"):
        PolarsCredentials.from_dict(
            {
                "schema": "my_schema",
                "catalog_type": "local",
                "root": "./data",
                "not_an_option": 1,
            }
        )
