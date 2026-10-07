import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from dbt.adapters.polars.testing.mixin import PolarsTestMixin

my_table_sql = "{{ config(materialized='table') }}\nselect 1 as id"

# dbt-core requires on_schema_change for incremental models with a contract.
my_incremental_sql = (
    "{{ config(materialized='incremental', on_schema_change='fail') }}\nselect 1 as id"
)


def contract_schema_yml(enforced: bool, *model_names: str) -> str:
    models = "".join(
        f"""
  - name: {model_name}
    config:
      contract:
        enforced: {str(enforced).lower()}
    columns:
      - name: id
        data_type: bigint
"""
        for model_name in model_names
    )
    return "version: 2\nmodels:" + models


class TestEnforcedContractFails(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_table.sql": my_table_sql,
            "my_incremental.sql": my_incremental_sql,
            "schema.yml": contract_schema_yml(True, "my_table", "my_incremental"),
        }

    def test_enforced_contract_fails(self, project):
        results, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert all(result.status == "error" for result in results)
        assert "Model contracts are not yet supported in dbt-polars" in log_output


class TestUnenforcedContractPasses(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_table.sql": my_table_sql,
            "schema.yml": contract_schema_yml(False, "my_table"),
        }

    def test_unenforced_contract_passes(self, project):
        run_dbt(["run"])
