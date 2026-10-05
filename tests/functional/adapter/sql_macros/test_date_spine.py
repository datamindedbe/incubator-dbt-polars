import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_date_spine import (
    models__test_date_spine_sql,
    models__test_date_spine_yml,
)


class BaseDateSpine(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__test_date_spine_sql, "date_spine"
            ),
        }


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "date_spine() is built from get_intervals_between(), which calls "
        "datediff() — not yet supported by dbt-polars."
    ),
)
class TestDateSpine(BaseDateSpine):
    pass
