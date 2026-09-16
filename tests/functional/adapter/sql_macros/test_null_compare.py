import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_null_compare import (
    MODELS__TEST_MIXED_NULL_COMPARE_SQL,
    MODELS__TEST_MIXED_NULL_COMPARE_YML,
    MODELS__TEST_NULL_COMPARE_SQL,
    MODELS__TEST_NULL_COMPARE_YML,
)


class BaseMixedNullCompare(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_mixed_null_compare.yml": MODELS__TEST_MIXED_NULL_COMPARE_YML,
            "test_mixed_null_compare.sql": MODELS__TEST_MIXED_NULL_COMPARE_SQL,
        }

    def test_build_assert_equal(self, project):
        run_dbt()
        run_dbt(["test"], expect_pass=False)


class BaseNullCompare(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_null_compare.yml": MODELS__TEST_NULL_COMPARE_YML,
            "test_null_compare.sql": MODELS__TEST_NULL_COMPARE_SQL,
        }


_NULL_DTYPE_XFAIL_REASON = (
    "dbt-polars can't materialize a column that is entirely NULL when writing "
    "Delta Lake (the default file_format): such a column has Polars dtype `Null`, "
    "which delta-rs rejects with 'dataframe contains unsupported data types: "
    "{Null}'. This is a storage-format limitation, not a SQL macro gap. Revisit if "
    "dbt-polars starts coercing all-null columns to a concrete type before write."
)


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=_NULL_DTYPE_XFAIL_REASON)
class TestMixedNullCompare(BaseMixedNullCompare):
    pass


@pytest.mark.xfail(strict=True, raises=AssertionError, reason=_NULL_DTYPE_XFAIL_REASON)
class TestNullCompare(BaseNullCompare):
    pass
