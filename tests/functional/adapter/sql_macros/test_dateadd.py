import pytest
from dbt.exceptions import CompilationError

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


@pytest.mark.xfail(
    strict=True,
    raises=CompilationError,
    reason=(
        "dbt-polars intentionally does not support dateadd() — Polars' SQL engine "
        "has no interval arithmetic. Revisit if Polars adds support."
    ),
)
class TestDateAdd(BaseDateAdd):
    pass
