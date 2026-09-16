import pytest
from dbt.exceptions import CompilationError

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


@pytest.mark.xfail(
    strict=True,
    raises=CompilationError,
    reason=(
        "dbt-polars intentionally does not support datediff() — Polars' SQL engine "
        "has no datediff()/date_diff() function. Revisit if Polars adds support."
    ),
)
class TestDateDiff(BaseDateDiff):
    pass
