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
        "dbt-polars intentionally does not support last_day() — it's built from "
        "dateadd()/date_trunc(), and dateadd() has no interval arithmetic on "
        "Polars. Revisit if Polars adds support."
    ),
)
class TestLastDay(BaseLastDay):
    pass
