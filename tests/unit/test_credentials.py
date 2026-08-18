import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.polars.connections import PolarsCredentials


def test_shorthand_builds_a_single_catalog_entry():
    creds = PolarsCredentials.from_dict(
        {
            "database": "",
            "schema": "my_schema",
            "catalog_type": "local",
            "root": "./data",
        }
    )

    assert creds.schema == "my_schema"
    assert list(creds.catalog_configs) == ["default"]
    config = creds.catalog_configs["default"]
    assert config.type == "local"
    assert config.schema == "my_schema"
    assert config.root == "./data"


def test_shorthand_names_the_catalog_after_database():
    creds = PolarsCredentials.from_dict(
        {
            "database": "my_catalog",
            "schema": "my_schema",
            "catalog_type": "local",
            "root": "./data",
        }
    )

    assert creds.database == "my_catalog"
    assert list(creds.catalog_configs) == ["my_catalog"]


def test_shorthand_names_the_catalog_after_the_catalog_alias():
    # dbt-core translates profile aliases (via `_ALIASES`) before calling
    # `from_dict` - mirror that here rather than relying on `from_dict` alone.
    data = PolarsCredentials.translate_aliases(
        {
            "catalog": "my_catalog",
            "schema": "my_schema",
            "catalog_type": "local",
            "root": "./data",
        }
    )
    creds = PolarsCredentials.from_dict(data)

    assert creds.database == "my_catalog"
    assert list(creds.catalog_configs) == ["my_catalog"]


def test_shorthand_requires_catalog_type():
    with pytest.raises(DbtRuntimeError):
        PolarsCredentials.from_dict({"database": "", "schema": "my_schema"})


def test_explicit_catalogs_rejects_top_level_schema():
    with pytest.raises(DbtRuntimeError):
        PolarsCredentials.from_dict(
            {
                "database": "",
                "schema": "my_schema",
                "catalogs": [
                    {"name": "local", "type": "local", "root": "./data", "schema": "s"}
                ],
            }
        )


def test_explicit_catalogs_rejects_catalog_type():
    with pytest.raises(DbtRuntimeError):
        PolarsCredentials.from_dict(
            {
                "database": "",
                "schema": "",
                "catalog_type": "local",
                "catalogs": [
                    {"name": "local", "type": "local", "root": "./data", "schema": "s"}
                ],
            }
        )


def test_explicit_catalogs_requires_a_schema_per_entry():
    with pytest.raises(DbtRuntimeError):
        PolarsCredentials.from_dict(
            {
                "database": "",
                "schema": "",
                "catalogs": [{"name": "local", "type": "local", "root": "./data"}],
            }
        )


def test_explicit_catalogs_backfills_schema_to_the_default_catalog():
    creds = PolarsCredentials.from_dict(
        {
            "database": "local2",
            "schema": "",
            "catalogs": [
                {
                    "name": "local",
                    "type": "local",
                    "root": "./a",
                    "schema": "schema_a",
                },
                {
                    "name": "local2",
                    "type": "local",
                    "root": "./b",
                    "schema": "schema_b",
                },
            ],
        }
    )

    assert creds.schema == "schema_b"
