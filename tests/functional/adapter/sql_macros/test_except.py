import pytest
from dbt.tests.util import check_relations_equal, run_dbt

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_except import (
    models__data_except_empty_sql,
    models__test_except_a_minus_a_sql,
    models__test_except_a_minus_b_sql,
    models__test_except_a_minus_empty_sql,
    models__test_except_b_minus_a_sql,
    models__test_except_empty_minus_a_sql,
    models__test_except_empty_minus_empty_sql,
    seeds__data_except_a_csv,
    seeds__data_except_a_minus_b_csv,
    seeds__data_except_b_csv,
    seeds__data_except_b_minus_a_csv,
)


class BaseExcept(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_except_a.csv": seeds__data_except_a_csv,
            "data_except_b.csv": seeds__data_except_b_csv,
            "data_except_a_minus_b.csv": seeds__data_except_a_minus_b_csv,
            "data_except_b_minus_a.csv": seeds__data_except_b_minus_a_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "data_except_empty.sql": self.interpolate_macro_namespace(
                models__data_except_empty_sql, "except"
            ),
            "test_except_a_minus_b.sql": self.interpolate_macro_namespace(
                models__test_except_a_minus_b_sql, "except"
            ),
            "test_except_b_minus_a.sql": self.interpolate_macro_namespace(
                models__test_except_b_minus_a_sql, "except"
            ),
            "test_except_a_minus_a.sql": self.interpolate_macro_namespace(
                models__test_except_a_minus_a_sql, "except"
            ),
            "test_except_a_minus_empty.sql": self.interpolate_macro_namespace(
                models__test_except_a_minus_empty_sql, "except"
            ),
            "test_except_empty_minus_a.sql": self.interpolate_macro_namespace(
                models__test_except_empty_minus_a_sql, "except"
            ),
            "test_except_empty_minus_empty.sql": self.interpolate_macro_namespace(
                models__test_except_empty_minus_empty_sql, "except"
            ),
        }

    def test_build_assert_equal(self, project):
        run_dbt(["deps"])
        run_dbt(["build"])

        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_b", "data_except_a_minus_b"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_b_minus_a", "data_except_b_minus_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_a", "data_except_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_a_minus_empty", "data_except_a"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_empty_minus_a", "data_except_empty"],
        )
        check_relations_equal(
            project.adapter,
            ["test_except_empty_minus_empty", "data_except_empty"],
        )


# CSV and ndjson have no embedded schema: an empty relation round-trips with no
# type information (CSV infers every column as str; ndjson can't infer at all),
# so the id join in these except models fails on empty operands. Parquet embeds
# a real schema even for zero rows, so it isn't affected.
@pytest.mark.skip_configs("csv", "ndjson")
class TestExcept(BaseExcept):
    pass
