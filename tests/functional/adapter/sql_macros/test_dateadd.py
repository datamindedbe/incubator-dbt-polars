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


models__test_dateadd_literals_sql = """
select
    {{ dateadd('month', -1, "DATE '2018-03-31'") }} as actual,
    TIMESTAMP '2018-02-28 00:00:00' as expected
union all select
    {{ dateadd('quarter', 1, "DATE '2018-01-31'") }},
    TIMESTAMP '2018-04-30 00:00:00'
union all select
    {{ dateadd('year', '-1', "DATE '2020-02-29'") }},
    TIMESTAMP '2019-02-28 00:00:00'
union all select
    {{ dateadd('hour', 12, "DATE '2018-01-01'") }},
    TIMESTAMP '2018-01-01 12:00:00'
union all select
    {{ dateadd('day', -364, "DATE '2018-12-31'") }},
    TIMESTAMP '2018-01-01 00:00:00'
union all select
    {{ dateadd('week', '1 + 1', "DATE '2018-01-01'") }},
    TIMESTAMP '2018-01-15 00:00:00'
union all select
    {{ dateadd('quarter', '0 + 1', "DATE '2018-01-31'") }},
    TIMESTAMP '2018-04-30 00:00:00'
"""


class TestDateAddLiteralAndExpressionCounts(BaseDateAdd):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": models__test_dateadd_yml,
            "test_dateadd.sql": models__test_dateadd_literals_sql,
        }
