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
    creds = PolarsCredentials.from_dict(
        {
            "catalog": "my_catalog",
            "schema": "my_schema",
            "catalog_type": "local",
            "root": "./data",
        }
    )

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


def test_credentials_survive_a_to_dict_from_dict_round_trip():
    for raw in (
        {
            "database": "",
            "schema": "my_schema",
            "catalog_type": "local",
            "root": "./data",
        },
        {
            "database": "cat1",
            "catalogs": [
                {"name": "cat1", "type": "local", "root": "./data1", "schema": "s1"},
                {"name": "cat2", "type": "local", "root": "./data2", "schema": "s2"},
            ],
        },
    ):
        creds = PolarsCredentials.from_dict(raw)
        creds2 = PolarsCredentials.from_dict(creds.to_dict())

        assert creds2.database == creds.database
        assert creds2.schema == creds.schema
        assert creds2.catalogs == creds.catalogs


def credentials_with_secrets() -> PolarsCredentials:
    return PolarsCredentials.from_dict(
        {
            "database": "unity",
            "catalogs": [
                {
                    "name": "unity",
                    "type": "databricks",
                    "catalog_name": "main",
                    "host": "https://adb-123.azuredatabricks.net",
                    "token": "secret-databricks-token",
                    "schema": "dev",
                },
                {
                    "name": "rest",
                    "type": "iceberg",
                    "pyiceberg_type": "rest",
                    "uri": "https://example.com/iceberg",
                    "credential": "client:secret",
                    "token": "secret-iceberg-token",
                    "schema": "dev",
                },
            ],
        }
    )


def test_connection_info_hides_catalog_secrets():
    catalogs = dict(credentials_with_secrets().connection_info())["catalogs"]

    assert catalogs == [
        {
            "name": "unity",
            "type": "databricks",
            "catalog_name": "main",
            "host": "https://adb-123.azuredatabricks.net",
            "schema": "dev",
        },
        {
            "name": "rest",
            "type": "iceberg",
            "pyiceberg_type": "rest",
            "uri": "https://example.com/iceberg",
            "schema": "dev",
        },
    ]


def test_unique_field_comes_from_the_default_catalog():
    assert credentials_with_secrets().unique_field == "main/dev"
