import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_datediff import (
    models__test_datediff_sql,
    models__test_datediff_yml,
    seeds__data_datediff_csv,
)


class BaseDateDiff(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                models__test_datediff_sql, "datediff"
            ),
        }


# CSV and ndjson have no native datetime type: the seed's timestamps are read
# back as strings, which cast(... as date) can't parse.
@pytest.mark.skip_configs("csv", "ndjson")
class TestDateDiff(BaseDateDiff):
    pass


models__test_datediff_edge_cases_sql = """
select {{ datediff(
    "TIMESTAMP '2018-01-01 00:00:00'", "TIMESTAMP '2018-10-01 00:00:00'", 'quarter'
) }} as actual, 3 as expected
union all select {{ datediff(
    "TIMESTAMP '2018-01-01 00:00:00.999'", "TIMESTAMP '2018-01-01 00:00:01.001'",
    'millisecond'
) }}, 2
union all select {{ datediff(
    "TIMESTAMP '2018-01-01 00:00:00.000001'", "TIMESTAMP '2018-01-01 00:00:00.00001'",
    'microsecond'
) }}, 9
union all select {{ datediff(
    "TIMESTAMP '2019-12-31 00:00:00'", "TIMESTAMP '2019-12-24 00:00:00'", 'week'
) }}, -1
union all select {{ datediff(
    "TIMESTAMP '2019-12-28 23:00:00'", "TIMESTAMP '2019-12-29 01:00:00'", 'week'
) }}, 1
union all select {{ datediff(
    "TIMESTAMP '1969-12-31 23:59:59'", "TIMESTAMP '1970-01-01 00:00:00'", 'day'
) }}, 1
union all select {{ datediff(
    "TIMESTAMP '1969-12-31 23:59:59'", "TIMESTAMP '1970-01-01 00:00:00'", 'second'
) }}, 1
union all select {{ datediff("'2020-01-01'", "DATE '2020-03-15'", 'month') }}, 2
"""


class TestDateDiffEdgeCases(BaseDateDiff):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": models__test_datediff_edge_cases_sql,
        }
