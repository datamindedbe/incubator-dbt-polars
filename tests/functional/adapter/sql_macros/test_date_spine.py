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
        "The fixture joins the spine to string literals ('2023-09-01'); Polars "
        "only joins keys of the same type. See TestDateSpineDates."
    ),
)
class TestDateSpine(BaseDateSpine):
    pass


# The upstream fixture with date-typed expected values, as its BigQuery/Redshift
# branch does, plus a month spine that crosses month ends.
models__test_date_spine_dates_sql = """
with generated_dates as (
    {{ date_spine("day", "'2023-09-01'", "'2023-09-10'") }}
), expected_dates as (
    select DATE '2023-09-01' as expected
    union all select DATE '2023-09-02'
    union all select DATE '2023-09-03'
    union all select DATE '2023-09-04'
    union all select DATE '2023-09-05'
    union all select DATE '2023-09-06'
    union all select DATE '2023-09-07'
    union all select DATE '2023-09-08'
    union all select DATE '2023-09-09'
)
select generated_dates.date_day, expected_dates.expected
from generated_dates
full outer join expected_dates on generated_dates.date_day = expected_dates.expected
"""

models__test_date_spine_months_sql = """
with generated_months as (
    {{ date_spine("month", "DATE '2023-01-31'", "DATE '2023-05-01'") }}
), expected_months as (
    select DATE '2023-01-31' as expected
    union all select DATE '2023-02-28'
    union all select DATE '2023-03-31'
    union all select DATE '2023-04-30'
)
select generated_months.date_month, expected_months.expected
from generated_months
full outer join expected_months
    on generated_months.date_month = expected_months.expected
"""

models__test_date_spine_dates_yml = """
version: 2
models:
  - name: test_date_spine_dates
    data_tests:
      - assert_equal:
          actual: date_day
          expected: expected
  - name: test_date_spine_months
    data_tests:
      - assert_equal:
          actual: date_month
          expected: expected
"""


class TestDateSpineDates(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine_dates.yml": models__test_date_spine_dates_yml,
            "test_date_spine_dates.sql": models__test_date_spine_dates_sql,
            "test_date_spine_months.sql": models__test_date_spine_months_sql,
        }
