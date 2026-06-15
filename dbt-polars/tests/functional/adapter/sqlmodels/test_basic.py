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


class TestDuplicateNameInContext(PolarsTestMixin):
    model_in_schema_a = """
        {{ config(schema='schema_a', alias='shared_name') }}
        select 1 as id
    """

    model_in_schema_b = """
        {{ config(schema='schema_b', alias='shared_name') }}
        select 2 as id
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_in_schema_a.sql": self.model_in_schema_a,
            "model_in_schema_b.sql": self.model_in_schema_b,
        }

    def test_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)


class TestCTESameNameAsModel(PolarsTestMixin):
    first = """
        select 1 as id
    """

    second = """
        select 2 as id
    """

    combined = """
    WITH combined AS (
        select * from {{ ref('first') }}
    )
    select * from {{ ref('second') }}
    where id in (select id from combined)
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first.sql": self.first,
            "second.sql": self.second,
            "combined.sql": self.combined,
        }

    def test_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        assert all(r.status == "success" for r in results)
