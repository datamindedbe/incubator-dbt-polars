import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from dbt.adapters.polars.testing.mixin import PolarsTestMixin

my_ephemeral_sql = """
{{ config(materialized="ephemeral") }}
select cast(1 as bigint) as id
"""

my_table_sql = """
{{ config(materialized="table") }}
select id, 'blue' as color from {{ ref('my_ephemeral') }}
"""

my_incremental_sql = """
{{ config(materialized="incremental", on_schema_change="fail") }}
select id, 'blue' as color from {{ ref('my_ephemeral') }}
"""


def contract_model_yml(model_name: str, id_type: str = "bigint", extra: str = ""):
    return f"""
  - name: {model_name}
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: {id_type}
{extra}
      - name: color
        data_type: text
"""


def contract_schema_yml(*model_ymls: str):
    return "version: 2\nmodels:" + "".join(model_ymls)


class TestContractEnforced(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_ephemeral.sql": my_ephemeral_sql,
            "my_table.sql": my_table_sql,
            "my_incremental.sql": my_incremental_sql,
            "schema.yml": contract_schema_yml(
                contract_model_yml("my_table"), contract_model_yml("my_incremental")
            ),
        }

    def test_matching_contract_passes(self, project):
        run_dbt(["run"])
        run_dbt(["run"])


class TestContractWrongDataType(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_ephemeral.sql": my_ephemeral_sql,
            "my_table.sql": my_table_sql,
            "schema.yml": contract_schema_yml(
                contract_model_yml("my_table", id_type="integer")
            ),
        }

    def test_wrong_data_type_fails(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "data type mismatch" in log_output


class TestContractWrongColumnName(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_ephemeral.sql": my_ephemeral_sql,
            "my_table.sql": my_table_sql.replace("as color", "as colour"),
            "schema.yml": contract_schema_yml(contract_model_yml("my_table")),
        }

    def test_wrong_column_name_fails(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "missing in definition" in log_output


class TestContractConstraintsUnsupported(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_ephemeral.sql": my_ephemeral_sql,
            "my_table.sql": my_table_sql,
            "schema.yml": contract_schema_yml(
                contract_model_yml(
                    "my_table",
                    extra="        constraints:\n          - type: not_null",
                )
            ),
        }

    def test_constraints_fail(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "dbt-polars does not support constraints" in log_output
        assert "id: not_null" in log_output
