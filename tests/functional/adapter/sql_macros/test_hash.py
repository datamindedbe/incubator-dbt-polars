import pytest
from dbt.tests.util import run_dbt

from dbt.adapters.polars.testing.mixin import PolarsTestMixin
from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_hash import (
    models__test_hash_sql,
    models__test_hash_yml,
    seeds__data_hash_csv,
)


class BaseHash(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": seeds__data_hash_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": models__test_hash_yml,
            "test_hash.sql": self.interpolate_macro_namespace(
                models__test_hash_sql, "hash"
            ),
        }


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "dbt-polars intentionally does not support hash() — Polars' SQL engine "
        "exposes no hashing function. Revisit if Polars adds support."
    ),
)
class TestHash(BaseHash):
    pass


class TestHashOnlyFailsModelsUsingIt(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "uses_hash.sql": "select {{ dbt.hash(\"'a'\") }} as hashed",
            "plain_model.sql": "select 1 as id",
        }

    def test_hash_fails_at_run_time_only(self, project):
        run_dbt(["parse"])
        run_dbt(["run", "-s", "plain_model"])

        results = run_dbt(["run"], expect_pass=False)
        status_by_model = {result.node.name: result.status for result in results}
        assert status_by_model == {"uses_hash": "error", "plain_model": "success"}
        failed = next(result for result in results if result.node.name == "uses_hash")
        assert "hash() is not supported by dbt-polars" in failed.message
