import pytest
from dbt.tests.util import check_relations_equal, run_dbt

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_intersect import (
    models__data_intersect_empty_sql,
    models__test_intersect_a_overlap_a_sql,
    models__test_intersect_a_overlap_b_sql,
    models__test_intersect_a_overlap_empty_sql,
    models__test_intersect_b_overlap_a_sql,
    models__test_intersect_empty_overlap_a_sql,
    models__test_intersect_empty_overlap_empty_sql,
    seeds__data_intersect_a_csv,
    seeds__data_intersect_a_overlap_b_csv,
    seeds__data_intersect_b_csv,
)


class BaseIntersect(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_intersect_a.csv": seeds__data_intersect_a_csv,
            "data_intersect_b.csv": seeds__data_intersect_b_csv,
            "data_intersect_a_overlap_b.csv": seeds__data_intersect_a_overlap_b_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_intersect_empty.sql": self.interpolate_macro_namespace(
                models__data_intersect_empty_sql, "intersect"
            ),
            "test_intersect_a_overlap_b.sql": self.interpolate_macro_namespace(
                models__test_intersect_a_overlap_b_sql, "intersect"
            ),
            "test_intersect_b_overlap_a.sql": self.interpolate_macro_namespace(
                models__test_intersect_b_overlap_a_sql, "intersect"
            ),
            "test_intersect_a_overlap_a.sql": self.interpolate_macro_namespace(
                models__test_intersect_a_overlap_a_sql, "intersect"
            ),
            "test_intersect_a_overlap_empty.sql": self.interpolate_macro_namespace(
                models__test_intersect_a_overlap_empty_sql, "intersect"
            ),
            "test_intersect_empty_overlap_a.sql": self.interpolate_macro_namespace(
                models__test_intersect_empty_overlap_a_sql, "intersect"
            ),
            "test_intersect_empty_overlap_empty.sql": self.interpolate_macro_namespace(
                models__test_intersect_empty_overlap_empty_sql, "intersect"
            ),
        }

    def test_build_assert_equal(self, project):
        run_dbt(["deps"])
        run_dbt(["build"])

        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_b", "data_intersect_a_overlap_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_b_overlap_a", "data_intersect_a_overlap_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_a", "data_intersect_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_a_overlap_empty", "data_intersect_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_empty_overlap_a", "data_intersect_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_intersect_empty_overlap_empty", "data_intersect_empty"],
        )


# Same empty-relation type-loss issue as test_except: CSV and ndjson round-trip
# an empty operand's columns with no type information, breaking the id join.
@pytest.mark.skip_configs("csv", "ndjson")
class TestIntersect(BaseIntersect):
    pass
