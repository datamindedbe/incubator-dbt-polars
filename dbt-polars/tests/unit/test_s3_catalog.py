"""Unit tests for S3Catalog.

S3 API calls are intercepted by moto (no real AWS credentials needed).
polars/deltalake Delta Lake I/O is mocked where the call would require
a real Delta table on disk — moto only stubs the S3 HTTP layer, not
the delta-rs engine that writes Parquet + transaction logs.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, patch

import boto3
import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.polars.catalogs.s3Catalog import S3Catalog, S3CatalogConfig
from dbt.adapters.polars.relation import PolarsRelation, TableFormat
from moto import mock_aws

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

BUCKET = "test-bucket"
REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_config(
    bucket: str = BUCKET,
    key_prefix: str = "",
    region: str = REGION,
) -> S3CatalogConfig:
    return S3CatalogConfig(
        name="test-s3",
        type="s3",
        bucket=bucket,
        key_prefix=key_prefix,
        region=region,
    )


def make_catalog(
    bucket: str = BUCKET,
    key_prefix: str = "",
    region: str = REGION,
) -> S3Catalog:
    return S3Catalog(make_config(bucket=bucket, key_prefix=key_prefix, region=region))


def make_relation(
    schema: str = "test_schema",
    identifier: str = "test_table",
    database: str = "test-s3",
) -> PolarsRelation:
    return PolarsRelation.create(
        database=database,
        schema=schema,
        identifier=identifier,
        type=RelationType.Table,
        format=TableFormat.delta,
        catalog=database,
    )


@pytest.fixture()
def s3() -> Iterator[S3Client]:
    """Start moto S3 mock and create the test bucket. Yields the boto3 client."""
    with mock_aws():
        client = cast("S3Client", boto3.client("s3", region_name=REGION))
        client.create_bucket(Bucket=BUCKET)
        yield client


def put_object(client, key: str, body: bytes = b"x") -> None:
    client.put_object(Bucket=BUCKET, Key=key, Body=body)


# ---------------------------------------------------------------------------
# S3CatalogConfig
# ---------------------------------------------------------------------------


class TestS3CatalogConfig:
    def test_unique_field_with_prefix(self):
        config = make_config(bucket="my-bucket", key_prefix="dbt/dev")
        assert config.unique_field() == "s3://my-bucket/dbt/dev"

    def test_unique_field_strips_slashes(self):
        config = make_config(bucket="my-bucket", key_prefix="/dbt/dev/")
        assert config.unique_field() == "s3://my-bucket/dbt/dev"

    def test_unique_field_no_prefix(self):
        config = make_config(bucket="my-bucket", key_prefix="")
        assert config.unique_field() == "s3://my-bucket"

    def test_connection_keys(self):
        assert make_config().connection_keys() == ("name", "bucket", "key_prefix")


class TestS3CatalogURIs:
    def test_root_prefix_empty(self):
        assert make_catalog(key_prefix="")._root_s3_prefix() == ""

    def test_root_prefix_with_value(self):
        assert make_catalog(key_prefix="dbt/dev")._root_s3_prefix() == "dbt/dev/"

    def test_root_prefix_strips_slashes(self):
        assert make_catalog(key_prefix="/dbt/dev/")._root_s3_prefix() == "dbt/dev/"

    def test_schema_uri_no_prefix(self):
        assert (
            make_catalog(key_prefix="")._schema_uri("my_schema")
            == "s3://test-bucket/my_schema"
        )

    def test_schema_uri_with_prefix(self):
        assert (
            make_catalog(key_prefix="dbt/dev")._schema_uri("my_schema")
            == "s3://test-bucket/dbt/dev/my_schema"
        )

    def test_relation_uri_no_prefix(self):
        rel = make_relation(schema="my_schema", identifier="my_table")
        assert (
            make_catalog(key_prefix="")._relation_uri(rel)
            == "s3://test-bucket/my_schema/my_table"
        )

    def test_relation_uri_with_prefix(self):
        rel = make_relation(schema="my_schema", identifier="my_table")
        assert (
            make_catalog(key_prefix="dbt/dev")._relation_uri(rel)
            == "s3://test-bucket/dbt/dev/my_schema/my_table"
        )

    def test_s3_prefix_from_uri(self):
        assert (
            make_catalog()._s3_prefix_from_uri("s3://test-bucket/dbt/dev/schema/table")
            == "dbt/dev/schema/table"
        )


# ---------------------------------------------------------------------------
# create_schema (no-op)
# ---------------------------------------------------------------------------


class TestCreateSchema:
    def test_create_schema_is_noop(self):
        make_catalog().create_schema(make_relation())

    def test_create_schema_missing_schema_raises(self):
        rel = PolarsRelation.create(database="test", schema=None, identifier="tbl")
        with pytest.raises(Exception):
            make_catalog().create_schema(rel)


# ---------------------------------------------------------------------------
# drop_schema  (real moto S3)
# ---------------------------------------------------------------------------


class TestDropSchema:
    def test_drop_schema_removes_all_objects_no_prefix(self, s3):
        put_object(s3, "test_schema/tbl/_delta_log/00.json")
        put_object(s3, "test_schema/tbl/part-0.parquet")
        put_object(s3, "other_schema/tbl/part-0.parquet")

        make_catalog(key_prefix="").drop_schema(make_relation(schema="test_schema"))

        remaining = [
            o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        ]
        assert all("test_schema" not in k for k in remaining)
        assert any("other_schema" in k for k in remaining)

    def test_drop_schema_removes_all_objects_with_prefix(self, s3):
        put_object(s3, "dbt/dev/test_schema/tbl/_delta_log/00.json")
        put_object(s3, "dbt/dev/other_schema/tbl/part.parquet")

        make_catalog(key_prefix="dbt/dev").drop_schema(
            make_relation(schema="test_schema")
        )

        remaining = [
            o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        ]
        assert all("test_schema" not in k for k in remaining)
        assert any("other_schema" in k for k in remaining)

    def test_drop_schema_empty_is_noop(self, s3):
        make_catalog(key_prefix="").drop_schema(make_relation(schema="missing"))


# ---------------------------------------------------------------------------
# list_schemas  (real moto S3)
# ---------------------------------------------------------------------------


class TestListSchemas:
    def test_list_schemas_no_prefix(self, s3):
        put_object(s3, "schema_a/tbl/file")
        put_object(s3, "schema_b/tbl/file")

        result = make_catalog(key_prefix="").list_schemas()
        assert sorted(result) == ["schema_a", "schema_b"]

    def test_list_schemas_with_prefix(self, s3):
        put_object(s3, "dbt/dev/schema_a/tbl/file")
        put_object(s3, "dbt/dev/schema_b/tbl/file")
        put_object(s3, "other/schema_c/tbl/file")  # different prefix — excluded

        result = make_catalog(key_prefix="dbt/dev").list_schemas()
        assert sorted(result) == ["schema_a", "schema_b"]

    def test_list_schemas_empty_bucket(self, s3):
        assert make_catalog().list_schemas() == []


# ---------------------------------------------------------------------------
# drop_relation  (real moto S3)
# ---------------------------------------------------------------------------


class TestDropRelation:
    def test_drop_relation_removes_only_its_objects(self, s3):
        put_object(s3, "schema/tbl_a/_delta_log/00.json")
        put_object(s3, "schema/tbl_a/part.parquet")
        put_object(s3, "schema/tbl_b/part.parquet")  # sibling — must survive

        make_catalog(key_prefix="").drop_relation(
            make_relation(schema="schema", identifier="tbl_a")
        )

        remaining = [
            o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        ]
        assert all("tbl_a" not in k for k in remaining)
        assert any("tbl_b" in k for k in remaining)

    def test_drop_relation_with_key_prefix(self, s3):
        put_object(s3, "env/dev/schema/tbl/_delta_log/00.json")

        make_catalog(key_prefix="env/dev").drop_relation(
            make_relation(schema="schema", identifier="tbl")
        )

        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="env/dev/schema/tbl/")
        assert resp.get("Contents") is None


# ---------------------------------------------------------------------------
# table_exists (mocks DeltaTable — moto doesn't implement delta-rs reads)
# ---------------------------------------------------------------------------


class TestTableExists:
    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_table_exists_true(self, mock_dt_cls):
        mock_dt_cls.is_deltatable.return_value = True
        assert make_catalog().table_exists(make_relation()) is True
        mock_dt_cls.is_deltatable.assert_called_once_with(
            "s3://test-bucket/test_schema/test_table",
            storage_options={"AWS_REGION": "us-east-1"},
        )

    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_table_exists_false(self, mock_dt_cls):
        mock_dt_cls.is_deltatable.return_value = False
        assert make_catalog().table_exists(make_relation()) is False


# ---------------------------------------------------------------------------
# get_relation / write_relation  (mock polars — delta-rs needs real S3 or local)
# ---------------------------------------------------------------------------


class TestGetRelation:
    @patch("dbt.adapters.polars.formats.table.deltaFormat.pl")
    def test_get_relation_calls_scan_delta(self, mock_pl):
        mock_pl.scan_delta.return_value = MagicMock()
        make_catalog(key_prefix="dbt/dev").get_relation(
            make_relation(schema="s", identifier="t")
        )
        mock_pl.scan_delta.assert_called_once_with(
            "s3://test-bucket/dbt/dev/s/t",
            storage_options={"AWS_REGION": "us-east-1"},
        )


class TestWriteRelation:
    def test_write_relation_calls_write_delta(self):
        import polars as real_pl

        df = MagicMock(spec=real_pl.DataFrame)
        make_catalog().write_relation(make_relation(), df)
        df.write_delta.assert_called_once_with(
            "s3://test-bucket/test_schema/test_table",
            mode="overwrite",
            storage_options={"AWS_REGION": "us-east-1"},
        )


# ---------------------------------------------------------------------------
# truncate_relation
# ---------------------------------------------------------------------------


class TestTruncateRelation:
    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_truncate_calls_delete(self, mock_dt_cls):
        dt_instance = MagicMock()
        mock_dt_cls.return_value = dt_instance

        make_catalog().truncate_relation(make_relation())

        mock_dt_cls.assert_called_once_with(
            "s3://test-bucket/test_schema/test_table",
            storage_options={"AWS_REGION": "us-east-1"},
        )
        dt_instance.delete.assert_called_once()


# ---------------------------------------------------------------------------
# append_relation
# ---------------------------------------------------------------------------


class TestAppendRelation:
    def test_append_no_schema_evolution(self):
        import polars as real_pl

        df = MagicMock(spec=real_pl.DataFrame)
        make_catalog().append_relation(
            make_relation(), df, allow_schema_evolution=False
        )
        df.write_delta.assert_called_once_with(
            "s3://test-bucket/test_schema/test_table",
            mode="append",
            delta_write_options=None,
            storage_options={"AWS_REGION": "us-east-1"},
        )

    def test_append_with_schema_evolution(self):
        import polars as real_pl

        df = MagicMock(spec=real_pl.DataFrame)
        make_catalog().append_relation(make_relation(), df, allow_schema_evolution=True)
        df.write_delta.assert_called_once_with(
            "s3://test-bucket/test_schema/test_table",
            mode="append",
            delta_write_options={"schema_mode": "merge"},
            storage_options={"AWS_REGION": "us-east-1"},
        )


# ---------------------------------------------------------------------------
# merge_relation
# ---------------------------------------------------------------------------


class TestMergeRelation:
    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_merge_calls_delta_merge(self, mock_dt_cls):
        import polars as real_pl

        dt_instance = MagicMock()
        mock_dt_cls.return_value = dt_instance
        merge_builder = MagicMock()
        dt_instance.merge.return_value = merge_builder
        merge_builder.when_matched_update_all.return_value = merge_builder
        merge_builder.when_not_matched_insert_all.return_value = merge_builder

        df = MagicMock(spec=real_pl.DataFrame)
        make_catalog().merge_relation(
            make_relation(), df, predicate="s.id = t.id", except_cols=["updated_at"]
        )

        mock_dt_cls.assert_called_once_with(
            "s3://test-bucket/test_schema/test_table",
            storage_options={"AWS_REGION": "us-east-1"},
        )
        dt_instance.merge.assert_called_once_with(
            df.to_arrow(),
            "s.id = t.id",
            source_alias="DBT_INTERNAL_SOURCE",
            target_alias="DBT_INTERNAL_DEST",
        )
        merge_builder.when_matched_update_all.assert_called_once_with(
            except_cols=["updated_at"]
        )
        merge_builder.when_not_matched_insert_all.assert_called_once()
        merge_builder.execute.assert_called_once()


# ---------------------------------------------------------------------------
# delete_matched_relation
# ---------------------------------------------------------------------------


class TestDeleteMatchedRelation:
    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_delete_matched_calls_delta_merge(self, mock_dt_cls):
        import polars as real_pl

        dt_instance = MagicMock()
        mock_dt_cls.return_value = dt_instance
        merge_builder = MagicMock()
        dt_instance.merge.return_value = merge_builder
        merge_builder.when_matched_delete.return_value = merge_builder

        df = MagicMock(spec=real_pl.DataFrame)
        make_catalog().delete_matched_relation(
            make_relation(), df, predicate="s.id = t.id"
        )

        merge_builder.when_matched_delete.assert_called_once()
        merge_builder.execute.assert_called_once()


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------


class TestComments:
    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_set_relation_comment(self, mock_dt_cls):
        dt_instance = MagicMock()
        mock_dt_cls.return_value = dt_instance

        make_catalog().set_relation_comment(make_relation(), "my comment")
        dt_instance.alter.set_table_description.assert_called_once_with("my comment")

    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_get_relation_comment(self, mock_dt_cls):
        dt_instance = MagicMock()
        mock_dt_cls.return_value = dt_instance
        dt_instance.metadata.return_value.description = "stored comment"

        assert make_catalog().get_relation_comment(make_relation()) == "stored comment"

    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_set_column_comments(self, mock_dt_cls):
        dt_instance = MagicMock()
        mock_dt_cls.return_value = dt_instance

        make_catalog().set_column_comments(
            make_relation(), {"col_a": "desc a", "col_b": "desc b"}
        )

        assert dt_instance.alter.set_column_metadata.call_count == 2
        dt_instance.alter.set_column_metadata.assert_any_call(
            "col_a", {"comment": "desc a"}
        )
        dt_instance.alter.set_column_metadata.assert_any_call(
            "col_b", {"comment": "desc b"}
        )

    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_get_column_comments(self, mock_dt_cls):
        dt_instance = MagicMock()
        mock_dt_cls.return_value = dt_instance

        col_a = MagicMock()
        col_a.name = "col_a"
        col_a.metadata = {"comment": "desc a"}
        col_b = MagicMock()
        col_b.name = "col_b"
        col_b.metadata = {}
        dt_instance.schema.return_value.fields = [col_a, col_b]

        result = make_catalog().get_column_comments(make_relation())
        assert result == {"col_a": "desc a"}


# ---------------------------------------------------------------------------
# list_relations_without_caching  (moto for S3 listing, mock for DeltaTable)
# ---------------------------------------------------------------------------


class TestListRelationsWithoutCaching:
    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_lists_only_delta_tables(self, mock_dt_cls, s3):
        put_object(s3, "schema/delta_table/_delta_log/00.json")
        put_object(s3, "schema/delta_table/part.parquet")
        put_object(s3, "schema/not_delta/just_a_file.txt")

        mock_dt_cls.is_deltatable.side_effect = lambda uri, **kw: "delta_table" in uri

        schema_rel = PolarsRelation.create(
            database="test-s3",
            schema="schema",
            identifier=None,
            type=RelationType.Table,
            catalog="test-s3",
        )
        result = make_catalog(key_prefix="").list_relations_without_caching(schema_rel)

        assert len(result) == 1
        assert result[0].identifier == "delta_table"

    @patch("dbt.adapters.polars.formats.table.deltaFormat._DeltaTable")
    def test_list_relations_with_key_prefix(self, mock_dt_cls, s3):
        put_object(s3, "env/dev/schema/tbl1/part.parquet")
        put_object(s3, "env/dev/schema/tbl2/part.parquet")
        mock_dt_cls.is_deltatable.return_value = True

        schema_rel = PolarsRelation.create(
            database="test-s3",
            schema="schema",
            identifier=None,
            type=RelationType.Table,
            catalog="test-s3",
        )
        result = make_catalog(key_prefix="env/dev").list_relations_without_caching(
            schema_rel
        )

        assert {r.identifier for r in result} == {"tbl1", "tbl2"}

    def test_missing_schema_raises(self):
        schema_rel = PolarsRelation.create(
            database="test-s3", schema=None, identifier=None, type=RelationType.Table
        )
        with pytest.raises(Exception, match="missing a schema"):
            make_catalog().list_relations_without_caching(schema_rel)
