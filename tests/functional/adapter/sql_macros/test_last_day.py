import pytest
from dbt.exceptions import CompilationError

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


@pytest.mark.xfail(
    strict=True,
    raises=CompilationError,
    reason=(
        "This fixture includes last_day(..., 'quarter'), which needs "
        "date_trunc('quarter') — not supported by dbt-polars. Month and year work."
    ),
)
class TestLastDay(BaseLastDay):
    pass


models__test_last_day_month_year_sql = """
with data as (

    select * from {{ ref('data_last_day') }}
    where date_part is null or date_part != 'quarter'

)

select
    case
        when date_part = 'month' then {{ last_day('date_day', 'month') }}
        when date_part = 'year' then {{ last_day('date_day', 'year') }}
        else null
    end as actual,
    result as expected

from data
"""


class TestLastDayMonthYear(BaseLastDay):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": models__test_last_day_yml,
            "test_last_day.sql": models__test_last_day_month_year_sql,
        }
