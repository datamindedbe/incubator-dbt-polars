import polars as pl
import pytest

from dbt.adapters.polars.catalogs import (
    PolarsRelation,
    create_catalog,
    resolve_catalog_plugin,
)
from dbt.adapters.polars.testing import CatalogTests

pytest.importorskip("pyiceberg")


class TestIcebergCatalog(CatalogTests):
    @pytest.fixture
    def catalog(self, tmp_path):
        config = resolve_catalog_plugin("iceberg").config_class(
            name="catalog_test",
            type="iceberg",
            schema="unused",
            pyiceberg_type="sql",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=f"file://{tmp_path}/warehouse",
        )
        return create_catalog(config, "")

    def written_relation(self, catalog) -> PolarsRelation:
        relation = PolarsRelation.create(schema="docs", identifier="customers")
        catalog.write_relation(relation, pl.DataFrame({"id": [1], "name": ["a"]}), [])
        return relation

    def commits(self, catalog, relation) -> int:
        return len(
            catalog._catalog.load_table(catalog._id(relation)).metadata.metadata_log
        )

    def test_unchanged_comment_is_not_committed_again(self, catalog):
        relation = self.written_relation(catalog)
        catalog.set_relation_comment(relation, "Customers.")
        commits = self.commits(catalog, relation)

        catalog.set_relation_comment(relation, "Customers.")

        assert self.commits(catalog, relation) == commits

    def test_only_changed_column_comments_are_committed(self, catalog):
        relation = self.written_relation(catalog)
        catalog.set_column_comments(relation, {"id": "Identifier.", "name": "Name."})
        commits = self.commits(catalog, relation)

        catalog.set_column_comments(relation, {"id": "Identifier.", "name": "Name."})
        assert self.commits(catalog, relation) == commits

        catalog.set_column_comments(
            relation, {"id": "Identifier.", "name": "Full name."}
        )
        catalog._invalidate_table_cache(catalog._id(relation))
        assert self.commits(catalog, relation) == commits + 1
        assert catalog.get_column_comments(relation) == {
            "id": "Identifier.",
            "name": "Full name.",
        }
