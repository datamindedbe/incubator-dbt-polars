import pytest

from tests.functional.adapter.sql_macros.base_array_utils import BaseArrayUtils
from tests.functional.adapter.sql_macros.fixture_array_construct import (
    models__array_construct_actual_sql,
    models__array_construct_expected_sql,
)


class BaseArrayConstruct(BaseArrayUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_construct_actual_sql,
            "expected.sql": models__array_construct_expected_sql,
        }


@pytest.mark.skip_configs("csv")  # CSV can't store nested/array data
class TestArrayConstruct(BaseArrayConstruct):
    pass
