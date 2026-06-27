from __future__ import annotations

import shutil
import uuid
from collections.abc import Iterator
from pathlib import Path

import boto3
import polars as pl
import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.polars.catalogs.s3Catalog import S3Catalog, S3CatalogConfig
from dbt.adapters.polars.relation import PolarsRelation, TableFormat
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable
from moto import mock_aws

_BUCKET = "test-bucket"
_REGION = "us-east-1"


class _FakeS3Catalog(S3Catalog):
    """Test double: delta-rs ops use a local tmpdir; S3 metadata ops go through moto.

    Lets functional tests exercise real write/read round-trips without real AWS.
    The S3 key structure is mirrored in tmpdir so listing and delta checks stay
    consistent.
    """

    def __init__(self, config: S3CatalogConfig, local_root: Path) -> None:
        super().__init__(config)
        self._local_root = local_root

    def _delta_path(self, relation: PolarsRelation) -> str:
        """Local path that mirrors the S3 key for this relation."""
        key = self._s3_prefix_from_uri(self._relation_uri(relation))
        return str(self._local_root / key)

    def _put_s3_marker(self, relation: PolarsRelation) -> None:
        """Put a key in moto S3 so listing operations can discover this table."""
        key = (
            self._s3_prefix_from_uri(self._relation_uri(relation))
            + "/_delta_log/00000000000000000000.json"
        )
        self._s3_client.put_object(Bucket=self.config.bucket, Key=key, Body=b"{}")

    # ------------------------------------------------------------------
    # delta-rs operations → local storage
    # ------------------------------------------------------------------

    def table_exists(self, relation: PolarsRelation) -> bool:
        return DeltaTable.is_deltatable(self._delta_path(relation))

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return pl.scan_delta(self._delta_path(relation))

    def write_relation(self, relation: PolarsRelation, df: pl.DataFrame) -> None:
        path = self._delta_path(relation)
        Path(path).mkdir(parents=True, exist_ok=True)
        df.write_delta(path, mode="overwrite")
        self._put_s3_marker(relation)

    def truncate_relation(self, relation: PolarsRelation) -> None:
        DeltaTable(self._delta_path(relation)).delete()

    def append_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None:
        opts = {"schema_mode": "merge"} if allow_schema_evolution else None
        df.write_delta(
            self._delta_path(relation), mode="append", delta_write_options=opts
        )

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[str] | None = None,
    ) -> None:
        dt = DeltaTable(self._delta_path(relation))
        (
            dt.merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
            )
            .when_matched_update_all(except_cols=except_cols)
            .when_not_matched_insert_all()
            .execute()
        )

    def delete_matched_relation(
        self, relation: PolarsRelation, df: pl.DataFrame, predicate: str
    ) -> None:
        dt = DeltaTable(self._delta_path(relation))
        (
            dt.merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
            )
            .when_matched_delete()
            .execute()
        )

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        DeltaTable(self._delta_path(relation)).alter.set_table_description(comment)

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        dt = DeltaTable(self._delta_path(relation))
        for col, c in comments.items():
            dt.alter.set_column_metadata(col, {"comment": c})

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        return DeltaTable(self._delta_path(relation)).metadata().description

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        dt = DeltaTable(self._delta_path(relation))
        return {
            f.name: f.metadata["comment"]
            for f in dt.schema().fields
            if f.metadata.get("comment")
        }

    def drop_relation(self, relation: PolarsRelation) -> None:
        local = Path(self._delta_path(relation))
        if local.exists():
            shutil.rmtree(local)
        super().drop_relation(relation)  # boto3 cleanup via moto

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        schema_prefix = (
            self._s3_prefix_from_uri(self._schema_uri(schema_relation.schema)) + "/"
        )
        paginator = self._s3_client.get_paginator("list_objects_v2")
        relations: list[PolarsRelation] = []
        for page in paginator.paginate(
            Bucket=self.config.bucket, Prefix=schema_prefix, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []):
                if (raw := cp.get("Prefix")) is None:
                    continue
                table_name = raw[len(schema_prefix) :].rstrip("/")
                if not table_name:
                    continue
                candidate = schema_relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=table_name,
                    type=RelationType.Table,
                    format=TableFormat.delta,
                    catalog=schema_relation.catalog,
                )
                if DeltaTable.is_deltatable(self._delta_path(candidate)):
                    relations.append(candidate)
        return relations


@pytest.fixture()
def catalog(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[_FakeS3Catalog]:
    """Per-test catalog: S3 backed by moto, delta tables stored in tmpdir."""
    run_id = uuid.uuid4().hex[:8]
    test_name = request.node.name.replace("[", "_").replace("]", "")
    with mock_aws():
        boto3.client("s3", region_name=_REGION).create_bucket(Bucket=_BUCKET)
        config = S3CatalogConfig(
            name="s3-test",
            type="s3",
            bucket=_BUCKET,
            region=_REGION,
            key_prefix=f"ci/{test_name}-{run_id}",
        )
        yield _FakeS3Catalog(config, tmp_path)


def make_relation(
    catalog: _FakeS3Catalog,
    schema: str = "test_schema",
    identifier: str = "test_table",
) -> PolarsRelation:
    return PolarsRelation.create(
        database=catalog.config.name,
        schema=schema,
        identifier=identifier,
        type=RelationType.Table,
        format=TableFormat.delta,
        catalog=catalog.config.name,
    )


class TestS3CatalogIntegration:
    def test_write_and_read(self, catalog):
        rel = make_relation(catalog)
        df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        catalog.write_relation(rel, df)
        result = catalog.get_relation(rel).collect()

        assert result.sort("id").equals(df.sort("id"))

    def test_table_exists_after_write(self, catalog):
        rel = make_relation(catalog)
        assert catalog.table_exists(rel) is False

        catalog.write_relation(rel, pl.DataFrame({"id": [1]}))
        assert catalog.table_exists(rel) is True

    def test_list_schemas(self, catalog):
        rel = make_relation(catalog, schema="schema_a", identifier="tbl")
        catalog.write_relation(rel, pl.DataFrame({"x": [1]}))

        assert "schema_a" in catalog.list_schemas()

    def test_list_relations_without_caching(self, catalog):
        for name in ("tbl1", "tbl2"):
            catalog.write_relation(
                make_relation(catalog, schema="myschema", identifier=name),
                pl.DataFrame({"x": [1]}),
            )

        schema_rel = PolarsRelation.create(
            database=catalog.config.name,
            schema="myschema",
            identifier=None,
            type=RelationType.Table,
            catalog=catalog.config.name,
        )
        listed = catalog.list_relations_without_caching(schema_rel)
        assert {r.identifier for r in listed} == {"tbl1", "tbl2"}

    def test_truncate_relation(self, catalog):
        rel = make_relation(catalog)
        catalog.write_relation(rel, pl.DataFrame({"id": [1, 2, 3]}))

        catalog.truncate_relation(rel)
        assert len(catalog.get_relation(rel).collect()) == 0

    def test_append_relation(self, catalog):
        rel = make_relation(catalog)
        catalog.write_relation(rel, pl.DataFrame({"id": [1, 2]}))
        catalog.append_relation(rel, pl.DataFrame({"id": [3, 4]}))
        assert len(catalog.get_relation(rel).collect()) == 4

    def test_drop_relation(self, catalog):
        rel = make_relation(catalog)
        catalog.write_relation(rel, pl.DataFrame({"id": [1]}))
        assert catalog.table_exists(rel) is True

        catalog.drop_relation(rel)
        assert catalog.table_exists(rel) is False

    def test_drop_schema(self, catalog):
        for name in ("t1", "t2"):
            catalog.write_relation(
                make_relation(catalog, schema="to_drop", identifier=name),
                pl.DataFrame({"x": [1]}),
            )

        assert "to_drop" in catalog.list_schemas()
        catalog.drop_schema(make_relation(catalog, schema="to_drop", identifier="t1"))
        assert "to_drop" not in catalog.list_schemas()

    def test_overwrite_relation(self, catalog):
        rel = make_relation(catalog)
        catalog.write_relation(rel, pl.DataFrame({"id": [1, 2, 3]}))
        catalog.write_relation(rel, pl.DataFrame({"id": [99]}))

        assert catalog.get_relation(rel).collect()["id"].to_list() == [99]

    def test_merge_relation(self, catalog):
        rel = make_relation(catalog)
        catalog.write_relation(rel, pl.DataFrame({"id": [1, 2], "v": ["a", "b"]}))

        catalog.merge_relation(
            rel,
            pl.DataFrame({"id": [2, 3], "v": ["B", "C"]}),
            predicate="DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id",
        )
        result = catalog.get_relation(rel).collect().sort("id")
        assert result["v"].to_list() == ["a", "B", "C"]

    def test_relation_comment(self, catalog):
        rel = make_relation(catalog)
        catalog.write_relation(rel, pl.DataFrame({"id": [1]}))

        catalog.set_relation_comment(rel, "test description")
        assert catalog.get_relation_comment(rel) == "test description"
