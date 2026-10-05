import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_last_day import (
    models__test_last_day_sql,
    models__test_last_day_yml,
    seeds__data_last_day_csv,
)


class BaseLastDay(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": models__test_last_day_yml,
            "test_last_day.sql": self.interpolate_macro_namespace(
                models__test_last_day_sql, "last_day"
            ),
        }


class TestLastDay(BaseLastDay):
    pass


# Weeks run Monday to Sunday, as in Postgres.
models__test_last_day_week_sql = """
select
    {{ last_day("DATE '2023-09-06'", 'week') }} as actual,
    DATE '2023-09-10' as expected
"""


class TestLastDayWeek(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": models__test_last_day_yml,
            "test_last_day.sql": models__test_last_day_week_sql,
        }
