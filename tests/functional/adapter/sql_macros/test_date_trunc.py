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


# CSV and ndjson have no native date/datetime type: reading a written timestamp
# back gives a plain string, which the model's strict_cast(Date) can't parse.
# Parquet and Delta both preserve the real Datetime dtype.
@pytest.mark.skip_configs("csv", "ndjson")
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


class TestDateTruncWeekQuarter(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": models__test_date_trunc_week_quarter_sql,
        }
