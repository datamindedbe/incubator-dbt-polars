import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_safe_cast_invalid import (
    models__test_safe_cast_invalid_sql,
    models__test_safe_cast_invalid_yml,
    seeds__data_safe_cast_invalid_csv,
)


class TestSafeCastInvalid(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_safe_cast_invalid.csv": seeds__data_safe_cast_invalid_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_safe_cast_invalid.yml": models__test_safe_cast_invalid_yml,
            "test_safe_cast_invalid.sql": models__test_safe_cast_invalid_sql,
        }
