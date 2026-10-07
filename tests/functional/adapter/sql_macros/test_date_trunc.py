import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_date_trunc import (
    models__test_date_trunc_sql,
    models__test_date_trunc_yml,
    seeds__data_date_trunc_csv,
)


class BaseDateTrunc(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": seeds__data_date_trunc_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                models__test_date_trunc_sql, "date_trunc"
            ),
        }


class TestDateTrunc(BaseDateTrunc):
    pass


# Weeks start on Monday, as in Postgres.
models__test_date_trunc_week_quarter_sql = """
select
    {{ date_trunc('week', "TIMESTAMP '2023-09-10 18:30:00'") }} as actual,
    TIMESTAMP '2023-09-04 00:00:00' as expected
union all select
    {{ date_trunc('week', "DATE '2023-09-06'") }}, TIMESTAMP '2023-09-04 00:00:00'
union all select
    {{ date_trunc('quarter', "TIMESTAMP '2023-12-31 23:59:59'") }},
    TIMESTAMP '2023-10-01 00:00:00'
union all select
    {{ date_trunc('quarter', "DATE '2023-02-15'") }}, TIMESTAMP '2023-01-01 00:00:00'
union all select
    {{ date_trunc('quarter', "cast(null as date)") }}, cast(null as timestamp)
"""


models__test_date_trunc_string_literals_sql = """
select
    {{ date_trunc('minute', "'2023-09-10 18:30:45'") }} as actual,
    TIMESTAMP '2023-09-10 18:30:00' as expected
union all select
    {{ date_trunc('hour', "'2023-09-10 18:30:45'") }}, TIMESTAMP '2023-09-10 18:00:00'
union all select
    {{ date_trunc('day', "'2023-09-10 18:30:45'") }}, TIMESTAMP '2023-09-10 00:00:00'
union all select
    {{ date_trunc('week', "'2023-09-10 18:30:45'") }}, TIMESTAMP '2023-09-04 00:00:00'
union all select
    {{ date_trunc('month', "'2023-09-10 18:30:45'") }}, TIMESTAMP '2023-09-01 00:00:00'
union all select
    {{ date_trunc('quarter', "'2023-09-10 18:30:45'") }},TIMESTAMP '2023-07-01 00:00:00'
union all select
    {{ date_trunc('year', "'2023-09-10'") }}, TIMESTAMP '2023-01-01 00:00:00'
union all select
    {{ date_trunc('hour', "'2023-09-10T18:30:45+0100'") }},
    TIMESTAMP '2023-09-10 17:00:00'
union all select
    {{ date_trunc('day', "'2023-09-10T00:30:00+01:00'") }},
    TIMESTAMP '2023-09-09 00:00:00'
"""


class TestDateTruncStringLiterals(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": models__test_date_trunc_string_literals_sql,
        }


class TestDateTruncWeekQuarter(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": models__test_date_trunc_week_quarter_sql,
        }


# Ephemeral, so the timezone survives: Delta would store these values as UTC.
# Both timestamps fall on the previous day in UTC. Hour and minute truncation are
# left out: they cast to timestamp, which normalizes to UTC.
models__brussels_timestamps_py = """
from datetime import datetime

import polars as pl


def model(dbt, session):
    dbt.config(materialized="ephemeral")
    return pl.DataFrame(
        {
            "new_year": [datetime(2023, 1, 1, 0, 30)],
            "monday": [datetime(2023, 1, 2, 0, 30)],
        }
    ).with_columns(pl.all().dt.replace_time_zone("Europe/Brussels"))
"""

models__test_date_trunc_timezone_sql = """
with truncated as (
    select
        {{ date_trunc('day', 'new_year') }} as day_trunc,
        {{ date_trunc('week', 'monday') }} as week_trunc,
        {{ date_trunc('month', 'new_year') }} as month_trunc,
        {{ date_trunc('quarter', 'new_year') }} as quarter_trunc,
        {{ date_trunc('year', 'new_year') }} as year_trunc
    from {{ ref('brussels_timestamps') }}
)
select strftime(day_trunc, '%Y-%m-%d %H:%M') as actual, '2023-01-01 00:00' as expected
from truncated
union all select strftime(week_trunc, '%Y-%m-%d %H:%M'), '2023-01-02 00:00'
from truncated
union all select strftime(month_trunc, '%Y-%m-%d %H:%M'), '2023-01-01 00:00'
from truncated
union all select strftime(quarter_trunc, '%Y-%m-%d %H:%M'), '2023-01-01 00:00'
from truncated
union all select strftime(year_trunc, '%Y-%m-%d %H:%M'), '2023-01-01 00:00'
from truncated
"""


class TestDateTruncTimezoneAware(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "brussels_timestamps.py": models__brussels_timestamps_py,
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": models__test_date_trunc_timezone_sql,
        }
