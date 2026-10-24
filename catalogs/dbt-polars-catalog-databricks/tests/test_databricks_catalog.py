import pytest

from dbt_polars_catalog_databricks.config import DatabricksCatalogConfig
from dbt_polars_catalog_databricks.delta import (
    DatabricksDeltaCatalog,
    delete_azure_directory,
)
from dbt_polars_catalog_databricks.unity_catalog import (
    UnityCatalogClient,
    alter_column_comments_sql,
    comment_on_table_sql,
    quote_identifier,
    quoted_full_name,
    table_format_of,
)

TABLE_URI = "abfss://container@account.dfs.core.windows.net/schema/table"
TABLE_INFO = {
    "full_name": "main.schema.table",
    "table_id": "table-id",
    "storage_location": TABLE_URI,
}


def catalog_recording_credential_requests():
    config = DatabricksCatalogConfig(
        name="uc", type="databricks", schema="schema", catalog_name="main"
    )
    catalog = DatabricksDeltaCatalog(config, UnityCatalogClient(config))
    requests = []

    def fake_request(method, path, *, query=None, body=None):
        requests.append(body)
        return {"azure_user_delegation_sas": {"sas_token": "token"}}

    catalog.unity_catalog.request = fake_request
    return catalog, requests


def test_registered_table_read_requests_read_credentials():
    catalog, requests = catalog_recording_credential_requests()
    catalog.unity_catalog.remember_table(TABLE_INFO)

    catalog.get_storage_options(TABLE_URI, read_only=True)

    assert requests == [{"table_id": "table-id", "operation": "READ"}]


def test_registered_table_write_requests_read_write_credentials():
    catalog, requests = catalog_recording_credential_requests()
    catalog.unity_catalog.remember_table(TABLE_INFO)

    catalog.get_storage_options(TABLE_URI)

    assert requests == [{"table_id": "table-id", "operation": "READ_WRITE"}]


def test_unregistered_path_read_requests_path_read_credentials():
    catalog, requests = catalog_recording_credential_requests()

    catalog.get_storage_options(TABLE_URI, read_only=True)

    assert requests == [{"url": TABLE_URI, "operation": "PATH_READ"}]


def test_credentials_are_cached_per_operation():
    catalog, requests = catalog_recording_credential_requests()
    catalog.unity_catalog.remember_table(TABLE_INFO)

    catalog.get_storage_options(TABLE_URI, read_only=True)
    catalog.get_storage_options(TABLE_URI, read_only=True)
    catalog.get_storage_options(TABLE_URI)

    assert [r["operation"] for r in requests] == ["READ", "READ_WRITE"]


def test_read_reuses_cached_read_write_credentials():
    catalog, requests = catalog_recording_credential_requests()
    catalog.unity_catalog.remember_table(TABLE_INFO)

    catalog.get_storage_options(TABLE_URI)
    catalog.get_storage_options(TABLE_URI, read_only=True)

    assert [r["operation"] for r in requests] == ["READ_WRITE"]


def test_dropping_managed_delta_table_only_unregisters_it():
    catalog, _ = catalog_recording_credential_requests()
    table = catalog.unity_catalog.remember_table(
        {**TABLE_INFO, "table_type": "MANAGED"}
    )
    requests = []
    catalog.unity_catalog.request = lambda method, path, *, query=None, body=None: (
        requests.append((method, path))
    )

    catalog.drop_table(table)

    assert requests == [("DELETE", "/api/2.1/unity-catalog/tables/main.schema.table")]


class FakeResponse:
    def __init__(self, status_code: int, continuation: str | None = None) -> None:
        self.status_code = status_code
        self.headers = {"x-ms-continuation": continuation} if continuation else {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def record_azure_deletes(monkeypatch, responses: list) -> list[dict]:
    import requests

    calls: list[dict] = []

    def delete(url, *, params, headers, timeout):
        calls.append({"url": url, "params": dict(params), "headers": headers})
        return responses.pop(0)

    monkeypatch.setattr(requests, "delete", delete)
    return calls


def test_azure_storage_is_deleted_as_one_directory(monkeypatch):
    calls = record_azure_deletes(monkeypatch, [FakeResponse(200)])

    delete_azure_directory(f"{TABLE_URI}/", {"sas_token": "?sv=1&sig=x"})

    assert calls == [
        {
            "url": "https://account.dfs.core.windows.net/container/schema/table"
            "?sv=1&sig=x",
            "params": {"recursive": "true"},
            "headers": {"x-ms-version": "2021-08-06"},
        }
    ]


def test_azure_directory_delete_follows_continuations(monkeypatch):
    calls = record_azure_deletes(
        monkeypatch, [FakeResponse(200, "next"), FakeResponse(200)]
    )

    delete_azure_directory(TABLE_URI, {"bearer_token": "aad"})

    assert [call["params"] for call in calls] == [
        {"recursive": "true"},
        {"recursive": "true", "continuation": "next"},
    ]
    assert calls[0]["headers"]["Authorization"] == "Bearer aad"


def test_already_deleted_azure_directory_is_not_an_error(monkeypatch):
    record_azure_deletes(monkeypatch, [FakeResponse(404)])

    delete_azure_directory(TABLE_URI, {"sas_token": "sv=1"})


def test_quote_identifier_wraps_in_backticks():
    assert quote_identifier("my_table") == "`my_table`"


def test_quote_identifier_escapes_embedded_backticks():
    assert quote_identifier("weird`name") == "`weird``name`"


def test_quoted_full_name_quotes_each_part():
    assert quoted_full_name("cat", "schema", "table") == "`cat`.`schema`.`table`"


def test_comment_on_table_sql_reserved_word_column_table_name():
    # Table names can be reserved words too - the whole qualified name must
    # be quoted, not just the leaf identifier.
    name = quoted_full_name("cat", "schema", "order")
    sql = comment_on_table_sql(name, "a comment")
    assert sql == "COMMENT ON TABLE `cat`.`schema`.`order` IS 'a comment'"


def test_comment_on_table_sql_escapes_single_quotes():
    name = quoted_full_name("cat", "schema", "table")
    sql = comment_on_table_sql(name, "it's a table")
    assert "it''s a table" in sql
    assert "it's a table" not in sql


def test_alter_column_comments_sql_quotes_reserved_word_column():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comments_sql(name, {"date": "when it happened"})
    assert sql == (
        "ALTER TABLE `cat`.`schema`.`table` ALTER COLUMN `date` "
        "COMMENT 'when it happened'"
    )


def test_alter_column_comments_sql_combines_columns_in_one_statement():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comments_sql(name, {"a": "first", "order": "second"})
    assert sql == (
        "ALTER TABLE `cat`.`schema`.`table` ALTER COLUMN "
        "`a` COMMENT 'first', `order` COMMENT 'second'"
    )


def test_alter_column_comments_sql_escapes_single_quotes_in_comment():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comments_sql(name, {"col": "user's note"})
    assert "user''s note" in sql
    assert "user's note" not in sql


def test_alter_column_comments_sql_escapes_backtick_in_column_name():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comments_sql(name, {"weird`col": "a comment"})
    assert "`weird``col`" in sql


UNIFORM_PROPERTIES = {"delta.universalFormat.enabledFormats": "iceberg"}


@pytest.mark.parametrize(
    ("info", "table_format"),
    [
        ({"data_source_format": "DELTA", "table_type": "EXTERNAL"}, "delta"),
        ({"data_source_format": "ICEBERG", "table_type": "MANAGED"}, "iceberg"),
        ({"data_source_format": "PARQUET", "table_type": "EXTERNAL"}, "parquet"),
        ({"table_type": "VIEW"}, "delta"),
        (
            {
                "data_source_format": "DELTA",
                "table_type": "MANAGED",
                "properties": UNIFORM_PROPERTIES,
            },
            "iceberg",
        ),
        (
            {
                "data_source_format": "DELTA",
                "table_type": "EXTERNAL",
                "properties": UNIFORM_PROPERTIES,
            },
            "delta",
        ),
    ],
)
def test_table_format_of(info, table_format):
    assert table_format_of(info) == table_format
