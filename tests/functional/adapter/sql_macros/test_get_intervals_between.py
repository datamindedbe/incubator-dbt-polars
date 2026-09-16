import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_get_intervals_between import (
    models__test_get_intervals_between_sql,
    models__test_get_intervals_between_yml,
)


class BaseGetIntervalsBetween(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__test_get_intervals_between_sql, "get_intervals_between"
            ),
        }


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "dbt-polars intentionally does not support get_intervals_between() — it "
        "calls datediff(), which has no interval arithmetic on Polars. Revisit if "
        "Polars adds support."
    ),
)
class TestGetIntervalsBetween(BaseGetIntervalsBetween):
    pass
