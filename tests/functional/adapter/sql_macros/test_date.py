import pytest
from dbt.exceptions import CompilationError

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_date import (
    models__test_date_sql,
    models__test_date_standalone_sql,
    models__test_date_standalone_yml,
    models__test_date_yml,
)


class BaseDate(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date.yml": models__test_date_yml,
            "test_date.sql": self.interpolate_macro_namespace(
                models__test_date_sql, "date"
            ),
        }


@pytest.mark.xfail(
    strict=True,
    raises=CompilationError,
    reason=(
        "This fixture calls date() through date_spine(), which is separately "
        "unsupported (see test_date_spine.py) — date() itself works, see "
        "TestDateStandalone."
    ),
)
class TestDate(BaseDate):
    pass


class TestDateStandalone(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_standalone.yml": models__test_date_standalone_yml,
            "test_date_standalone.sql": self.interpolate_macro_namespace(
                models__test_date_standalone_sql, "date"
            ),
        }
