import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from dbt.adapters.polars.testing.mixin import PolarsTestMixin

merge_clauses_sql = """
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='k',
          merge_clauses={'when_matched': [{'action': 'do_nothing'}]}) }}
select 1 as k
"""

unknown_key_python = """
def model(dbt, session):
    dbt.config(materialized="table", foo="bar")
    return dbt.ref("upstream")
"""

known_keys_sql = """
{{ config(materialized='table', partition_by='k', meta={'owner': 'data-team'}) }}
select 1 as k, 'a' as v
"""


class TestUnknownSqlModelConfigFails(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": merge_clauses_sql}

    def test_unknown_sql_model_config_fails(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "unsupported config key(s): merge_clauses" in log_output


class TestUnknownPythonModelConfigFails(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": "select 1 as k",
            "my_model.py": unknown_key_python,
        }

    def test_unknown_python_model_config_fails(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "unsupported config key(s): foo" in log_output


class TestKnownModelConfigPasses(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": known_keys_sql}

    def test_known_model_config_passes(self, project):
        run_dbt(["run"])
