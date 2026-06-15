import pytest
from dbt.tests.util import run_dbt
from tests.conftest import PolarsTestMixin

MODEL_SELECT_ONE = "select 1 as a"


class TestSelectOneModel(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"select_one.sql": MODEL_SELECT_ONE}

    def test_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
