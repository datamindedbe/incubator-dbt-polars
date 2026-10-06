import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from dbt.adapters.polars.testing.mixin import PolarsTestMixin

my_model_sql = "{{ config(materialized='table') }}\nselect 1 as id"

my_snapshot_sql = """
{% snapshot my_snapshot %}
{{ config(
    unique_key='id',
    strategy='check',
    check_cols='all',
    post_hook="vacuum {{ this }}",
) }}
select 1 as id
{% endsnapshot %}
"""

seed_csv = "id\n1\n"


class TestSqlPreHookFails(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": '{{ config(pre_hook="delete from {{ this }}") }}'
            + my_model_sql
        }

    def test_sql_pre_hook_fails(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "dbt-polars does not support SQL hooks" in log_output


class TestSqlPostHookFails(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+post-hook": "grant select on {{ this }} to reporter"}}

    def test_sql_post_hook_fails(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "dbt-polars does not support SQL hooks" in log_output


class TestSnapshotSqlHookFails(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"my_snapshot.sql": my_snapshot_sql}

    def test_snapshot_sql_hook_fails(self, project):
        _, log_output = run_dbt_and_capture(["snapshot"], expect_pass=False)
        assert "dbt-polars does not support SQL hooks" in log_output


class TestMacroOnlyHookPasses(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "{{ config(pre_hook=\"{{ log('running pre-hook', "
            + 'info=True) }}") }}'
            + my_model_sql
        }

    def test_macro_only_hook_passes(self, project):
        _, log_output = run_dbt_and_capture(["run"])
        assert "running pre-hook" in log_output


class TestGrantsFail(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "{{ config(grants={'select': ['reporter']}) }}"
            + my_model_sql
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+grants": {"select": ["reporter"]}}}

    def test_model_grants_fail(self, project):
        _, log_output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "dbt-polars does not support grants" in log_output

    def test_seed_grants_fail(self, project):
        _, log_output = run_dbt_and_capture(["seed"], expect_pass=False)
        assert "dbt-polars does not support grants" in log_output


class TestNoHooksOrGrantsPasses(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql}

    def test_plain_model_passes(self, project):
        run_dbt(["run"])
