"""merge and delete+insert on an Iceberg table with a composite unique_key.

These used to delete through one pyiceberg Or(And(EqualTo, ...), ...) with an And per batch
row, which does not scale: 22 s at 5,000 rows and a segfault at 20,000. They now prune files
with one In() per key column and anti-join each candidate file in Polars.
"""

import time

import pytest

import polars as pl
from dbt.adapters.polars.catalogs import create_catalog, resolve_catalog_plugin
from dbt.adapters.polars.testing import CatalogTestBase
from polars.testing import assert_frame_equal

pytest.importorskip("pyiceberg")

KEYS = ["region", "product"]


def _sorted(catalog, relation):
    return catalog.get_relation(relation).collect().sort(KEYS)


class TestIcebergCompositeKey(CatalogTestBase):
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

    def test_merge_composite_key(self, catalog, schema_relation):
        relation = self.table(schema_relation, "merged")
        catalog.write_relation(
            relation,
            pl.DataFrame(
                {"region": ["eu", "eu", "us"], "product": ["a", "b", "a"], "v": [1, 2, 3]}
            ),
            [],
        )

        catalog.merge_relation(
            relation,
            pl.DataFrame({"region": ["eu", "us"], "product": ["b", "b"], "v": [20, 40]}),
            keys=KEYS,
        )

        assert_frame_equal(
            _sorted(catalog, relation),
            pl.DataFrame(
                {
                    "region": ["eu", "eu", "us", "us"],
                    "product": ["a", "b", "a", "b"],
                    "v": [1, 20, 3, 40],
                }
            ),
        )

    def test_merge_composite_key_with_except_cols(self, catalog, schema_relation):
        relation = self.table(schema_relation, "merged_partial")
        catalog.write_relation(
            relation,
            pl.DataFrame(
                {"region": ["eu", "us"], "product": ["a", "a"], "v": [1, 2], "w": ["x", "y"]}
            ),
            [],
        )

        catalog.merge_relation(
            relation,
            pl.DataFrame(
                {"region": ["us", "eu"], "product": ["a", "c"], "v": [20, 5], "w": ["Y", "z"]}
            ),
            keys=KEYS,
            except_cols=["w"],
        )

        assert_frame_equal(
            _sorted(catalog, relation),
            pl.DataFrame(
                {
                    "region": ["eu", "eu", "us"],
                    "product": ["a", "c", "a"],
                    "v": [1, 5, 20],
                    "w": ["x", "z", "y"],
                }
            ),
        )

    def test_delete_matched_composite_key_spares_cross_combinations(
        self, catalog, schema_relation
    ):
        """(eu, b) and (us, a) each match the batch on ONE key column only: the prune filter
        admits them, the anti-join must keep them."""
        relation = self.table(schema_relation, "deleted")
        catalog.write_relation(
            relation,
            pl.DataFrame(
                {"region": ["eu", "eu", "us", "us"], "product": ["a", "b", "a", "b"]}
            ),
            [],
        )

        catalog.delete_matched_relation(
            relation,
            pl.DataFrame({"region": ["eu", "us"], "product": ["a", "b"]}),
            keys=KEYS,
        )

        assert _sorted(catalog, relation).rows() == [("eu", "b"), ("us", "a")]

    def test_rows_in_other_files_are_untouched(self, catalog, schema_relation):
        relation = self.table(schema_relation, "files")
        catalog.write_relation(
            relation, pl.DataFrame({"region": ["eu"], "product": ["a"], "v": [1]}), []
        )
        catalog.append_relation(
            relation, pl.DataFrame({"region": ["us"], "product": ["a"], "v": [2]})
        )

        catalog.merge_relation(
            relation,
            pl.DataFrame({"region": ["us"], "product": ["a"], "v": [9]}),
            keys=KEYS,
        )

        assert _sorted(catalog, relation)["v"].to_list() == [1, 9]

    def test_null_key_never_matches(self, catalog, schema_relation):
        relation = self.table(schema_relation, "nulls")
        catalog.write_relation(
            relation,
            pl.DataFrame({"region": ["eu", None], "product": ["a", "a"], "v": [1, 2]}),
            [],
        )

        catalog.merge_relation(
            relation,
            pl.DataFrame({"region": [None], "product": ["a"], "v": [3]}),
            keys=KEYS,
        )

        assert sorted(_sorted(catalog, relation)["v"].to_list()) == [1, 2, 3]

    def test_large_composite_key_merge(self, catalog, schema_relation):
        """50,000 batch rows, every one matched: the per-row Or crashed at 20,000."""
        relation = self.table(schema_relation, "large")
        rows = (
            pl.DataFrame({"region": [f"r{i}" for i in range(200)]})
            .join(pl.DataFrame({"product": [f"p{i}" for i in range(250)]}), how="cross")
            .with_columns(v=pl.lit(1))
        )
        catalog.write_relation(relation, rows, [])

        t0 = time.perf_counter()
        catalog.merge_relation(relation, rows.with_columns(v=pl.lit(2)), keys=KEYS)
        elapsed = time.perf_counter() - t0

        result = _sorted(catalog, relation)
        assert result.height == 50_000
        assert result["v"].unique().to_list() == [2]
        assert elapsed < 60
