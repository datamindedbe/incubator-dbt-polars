"""Reusable tests for catalog implementations.

Subclass `CatalogTests` (BaseCatalog subclasses) or `StorageCatalogTests`
(StorageCatalog subclasses, which inherit the data operations) and provide a
`catalog` fixture returning a configured catalog instance. The feature classes
can also be combined individually.
"""

import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

import polars as pl
from dbt.adapters.polars.catalogs import BaseCatalog, PolarsRelation
from polars.testing import assert_frame_equal


class CatalogTestBase:
    @pytest.fixture
    def catalog(self) -> BaseCatalog:
        raise NotImplementedError("Provide a `catalog` fixture")

    @pytest.fixture
    def schema_relation(self, catalog):
        relation = PolarsRelation.create(
            database=catalog.config.name,
            schema=f"catalog_test_{uuid.uuid4().hex[:8]}",
            catalog=catalog.config.name,
        )
        catalog.create_schema(relation)
        yield relation
        catalog.drop_schema(relation)

    @staticmethod
    def table(
        schema_relation: PolarsRelation, identifier: str, file_format: str | None = None
    ) -> PolarsRelation:
        return PolarsRelation.create(
            database=schema_relation.database,
            schema=schema_relation.schema,
            identifier=identifier,
            catalog=schema_relation.catalog,
            file_format=file_format,
        )

    @staticmethod
    def read_sorted(catalog: BaseCatalog, relation: PolarsRelation) -> pl.DataFrame:
        return catalog.get_relation(relation).collect().sort("id")


class SchemaTests(CatalogTestBase):
    def test_create_list_and_drop_schema(self, catalog, schema_relation):
        assert schema_relation.schema in catalog.list_schemas()
        catalog.drop_schema(schema_relation)
        assert schema_relation.schema not in catalog.list_schemas()
        catalog.create_schema(schema_relation)

    def test_list_relations(self, catalog, schema_relation):
        for identifier in ("first_table", "second_table"):
            relation = self.table(schema_relation, identifier)
            catalog.write_relation(relation, pl.DataFrame({"id": [1]}), [])

        listed = {
            r.identifier
            for r in catalog.list_relations_without_caching(schema_relation)
        }

        assert listed == {"first_table", "second_table"}

    def test_create_schema_is_idempotent(self, catalog, schema_relation):
        catalog.create_schema(schema_relation)

        assert schema_relation.schema in catalog.list_schemas()

    def test_drop_schema_with_tables(self, catalog, schema_relation):
        relation = self.table(schema_relation, "people")
        catalog.write_relation(relation, pl.DataFrame({"id": [1]}), [])

        catalog.drop_schema(schema_relation)

        assert schema_relation.schema not in catalog.list_schemas()
        assert not catalog.table_exists(relation)

    def test_missing_schema(self, catalog, schema_relation):
        missing = schema_relation.incorporate(
            path={"schema": f"{schema_relation.schema}_missing"}
        )

        assert catalog.list_relations_without_caching(missing) == []
        catalog.drop_schema(missing)


class WriteTests(CatalogTestBase):
    def test_missing_table(self, catalog, schema_relation):
        relation = self.table(schema_relation, "missing")

        assert not catalog.table_exists(relation)
        catalog.drop_relation(relation)

    def test_write_read_and_drop(self, catalog, schema_relation):
        relation = self.table(schema_relation, "people")
        data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        catalog.write_relation(relation, data, partition_by=[])

        assert catalog.table_exists(relation)
        assert_frame_equal(self.read_sorted(catalog, relation), data)
        catalog.drop_relation(relation)
        assert not catalog.table_exists(relation)

    def test_overwrite_replaces_data(self, catalog, schema_relation):
        relation = self.table(schema_relation, "people")
        catalog.write_relation(relation, pl.DataFrame({"id": [1]}), partition_by=[])
        catalog.write_relation(relation, pl.DataFrame({"id": [2]}), partition_by=[])

        assert self.read_sorted(catalog, relation)["id"].to_list() == [2]

    def test_partitioned_write(self, catalog, schema_relation):
        relation = self.table(schema_relation, "partitioned")
        data = pl.DataFrame({"id": [1, 2], "part": ["x", "y"]})

        catalog.write_relation(relation, data, partition_by=["part"])

        assert catalog.get_partition_columns(relation) == ["part"]


class IncrementalTests(CatalogTestBase):
    def test_append(self, catalog, schema_relation):
        relation = self.table(schema_relation, "appended")
        catalog.write_relation(relation, pl.DataFrame({"id": [1]}), [])

        catalog.append_relation(relation, pl.DataFrame({"id": [2]}))

        assert self.read_sorted(catalog, relation)["id"].to_list() == [1, 2]

    def test_merge(self, catalog, schema_relation):
        relation = self.table(schema_relation, "merged")
        catalog.write_relation(
            relation, pl.DataFrame({"id": [1, 2], "value": ["a", "b"]}), []
        )

        catalog.merge_relation(
            relation, pl.DataFrame({"id": [2, 3], "value": ["B", "c"]}), keys=["id"]
        )

        assert_frame_equal(
            self.read_sorted(catalog, relation),
            pl.DataFrame({"id": [1, 2, 3], "value": ["a", "B", "c"]}),
        )

    def test_delete_matched(self, catalog, schema_relation):
        relation = self.table(schema_relation, "deleted")
        catalog.write_relation(relation, pl.DataFrame({"id": [1, 2, 3]}), [])

        catalog.delete_matched_relation(
            relation, pl.DataFrame({"id": [2]}), keys=["id"]
        )

        assert self.read_sorted(catalog, relation)["id"].to_list() == [1, 3]

    def test_truncate(self, catalog, schema_relation):
        relation = self.table(schema_relation, "truncated")
        catalog.write_relation(relation, pl.DataFrame({"id": [1, 2]}), [])

        catalog.truncate_relation(relation)

        assert catalog.table_exists(relation)
        assert catalog.get_relation(relation).collect().height == 0


class SnapshotTests(CatalogTestBase):
    def test_apply_snapshot_delta(self, catalog, schema_relation):
        relation = self.table(schema_relation, "snapshot")
        catalog.write_relation(
            relation,
            pl.DataFrame({"id": [1], "dbt_scd_id": ["a"], "valid_to": [None]}).cast(
                {"valid_to": pl.String}
            ),
            [],
        )

        catalog.apply_snapshot_delta(
            relation,
            rows_to_close=pl.DataFrame(
                {"id": [1], "dbt_scd_id": ["a"], "valid_to": ["closed"]}
            ),
            rows_to_insert=pl.DataFrame(
                {"id": [1], "dbt_scd_id": ["b"], "valid_to": [None]}
            ).cast({"valid_to": pl.String}),
        )

        result = catalog.get_relation(relation).collect().sort("dbt_scd_id")
        assert result["valid_to"].to_list() == ["closed", None]


class CommentTests(CatalogTestBase):
    def test_comments(self, catalog, schema_relation):
        relation = self.table(schema_relation, "commented")
        catalog.write_relation(relation, pl.DataFrame({"id": [1]}), [])

        catalog.set_relation_comment(relation, "table comment")
        catalog.set_column_comments(relation, {"id": "column comment"})

        assert catalog.get_relation_comment(relation) == "table comment"
        assert catalog.get_column_comments(relation) == {"id": "column comment"}


class FileFormatTests(CatalogTestBase):
    file_format = "parquet"

    def test_file_format_write_read_and_drop(self, catalog, schema_relation):
        relation = self.table(schema_relation, "people", self.file_format)
        data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        catalog.write_relation(relation, data, partition_by=[])

        assert catalog.table_exists(relation)
        assert_frame_equal(self.read_sorted(catalog, relation), data)
        catalog.drop_relation(relation)
        assert not catalog.table_exists(relation)

    def test_file_format_is_listed(self, catalog, schema_relation):
        default_relation = self.table(schema_relation, "default_table")
        file_relation = self.table(schema_relation, "file_table", self.file_format)
        catalog.write_relation(default_relation, pl.DataFrame({"id": [1]}), [])
        catalog.write_relation(file_relation, pl.DataFrame({"id": [1]}), [])

        listed = {
            (r.identifier, r.file_format)
            for r in catalog.list_relations_without_caching(schema_relation)
        }

        assert listed == {("default_table", None), ("file_table", self.file_format)}


class ConcurrencyTests(CatalogTestBase):
    def test_concurrent_writes(self, catalog, schema_relation):
        relations = [self.table(schema_relation, f"table_{i}") for i in range(8)]

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(
                    catalog.write_relation, relation, pl.DataFrame({"id": [i]}), []
                )
                for i, relation in enumerate(relations)
            ]
            for future in futures:
                future.result()

        for i, relation in enumerate(relations):
            assert self.read_sorted(catalog, relation)["id"].to_list() == [i]


class CatalogTests(
    SchemaTests,
    WriteTests,
    IncrementalTests,
    SnapshotTests,
    CommentTests,
    ConcurrencyTests,
):
    pass


class StorageCatalogTests(SchemaTests, WriteTests, ConcurrencyTests, FileFormatTests):
    pass
