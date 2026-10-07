import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_dateadd import (
    models__test_dateadd_sql,
    models__test_dateadd_yml,
    seeds__data_dateadd_csv,
)


class BaseDateAdd(BaseUtils):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            # dbt-polars only materializes tables, not views (see PolarsTestMixin)
            "models": {"+materialized": "table"},
            # this is only needed for BigQuery, right?
            # no harm having it here until/unless there's an adapter that doesn't
            # support the 'timestamp' type
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "timestamp",
                            "result": "timestamp",
                        },
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                models__test_dateadd_sql, "dateadd"
            ),
        }


class TestDateAdd(BaseDateAdd):
    pass


# Day and larger dateparts keep the input type: a date stays a date.
models__test_dateadd_dates_sql = """
select
    {{ dateadd('month', -1, "DATE '2018-03-31'") }} as actual,
    DATE '2018-02-28' as expected
union all select
    {{ dateadd('quarter', 1, "DATE '2018-01-31'") }} as actual,
    DATE '2018-04-30' as expected
union all select
    {{ dateadd('year', '-1', "DATE '2020-02-29'") }} as actual,
    DATE '2019-02-28' as expected
union all select
    {{ dateadd('day', -364, "DATE '2018-12-31'") }} as actual,
    DATE '2018-01-01' as expected
union all select
    {{ dateadd('day', 1, "'2018-01-01'") }} as actual,
    DATE '2018-01-02' as expected
union all select
    {{ dateadd('week', '1 + 1', "DATE '2018-01-01'") }} as actual,
    DATE '2018-01-15' as expected
union all select
    {{ dateadd('quarter', '0 + 1', "DATE '2018-01-31'") }} as actual,
    DATE '2018-04-30' as expected
"""

models__test_dateadd_timestamps_sql = """
select
    {{ dateadd('hour', 12, "DATE '2018-01-01'") }} as actual,
    TIMESTAMP '2018-01-01 12:00:00' as expected
union all select
    {{ dateadd('month', '0 + 1', "TIMESTAMP '2018-01-31 13:05:00'") }} as actual,
    TIMESTAMP '2018-02-28 13:05:00' as expected
union all select
    {{ dateadd('day', -1, "'2018-01-01 06:00:00'") }} as actual,
    TIMESTAMP '2017-12-31 06:00:00' as expected
union all select
    {{ dateadd('hour', 1, "'2023-01-01T10:00:00Z'") }} as actual,
    TIMESTAMP '2023-01-01 11:00:00' as expected
union all select
    {{ dateadd('minute', 30, "'2023-01-01T10:00:00.5-05:30'") }} as actual,
    TIMESTAMP '2023-01-01 16:00:00.5' as expected
"""

models__test_dateadd_dates_and_timestamps_yml = """
version: 2
models:
  - name: test_dateadd_dates
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
  - name: test_dateadd_timestamps
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""


class TestDateAddLiteralAndExpressionCounts(BaseDateAdd):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": models__test_dateadd_dates_and_timestamps_yml,
            "test_dateadd_dates.sql": models__test_dateadd_dates_sql,
            "test_dateadd_timestamps.sql": models__test_dateadd_timestamps_sql,
        }


# Ephemeral, so the timezone survives: Delta would store these values as UTC.
models__brussels_timestamps_py = """
from datetime import datetime

import polars as pl


def model(dbt, session):
    dbt.config(materialized="ephemeral")
    return pl.DataFrame(
        {"from_ts": [datetime(2023, 1, 31, 0, 30)], "months": [1]}
    ).with_columns(pl.col("from_ts").dt.replace_time_zone("Europe/Brussels"))
"""

models__test_dateadd_timezone_sql = """
select
    strftime({{ dateadd('month', 'months', 'from_ts') }}, '%Y-%m-%d %H:%M') as actual,
    '2023-02-28 00:30' as expected
from {{ ref('brussels_timestamps') }}
"""


class TestDateAddTimezoneAware(BaseDateAdd):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "brussels_timestamps.py": models__brussels_timestamps_py,
            "test_dateadd.yml": models__test_dateadd_yml,
            "test_dateadd.sql": models__test_dateadd_timezone_sql,
        }
