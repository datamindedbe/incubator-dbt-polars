import pytest
from dbt.tests.util import run_dbt

from dbt.adapters.polars.testing.utils import polars_read_relation

models__null_unique_key_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = ['k1', 'k2']
) }}

{% if not is_incremental() %}

select 1 as k1, 'a' as k2, 'first' as val
union all
select 1 as k1, cast(null as varchar) as k2, 'first_null' as val

{% else %}

select 1 as k1, 'a' as k2, 'second' as val
union all
select 1 as k1, cast(null as varchar) as k2, 'second_null' as val

{% endif %}
"""


class BaseIncrementalNullUniqueKey:
    """A NULL in a unique_key column never matches, so the row is appended
    instead of replacing the existing row with the same NULL key."""

    strategy: str

    @pytest.fixture(scope="class")
    def models(self):
        return {"null_unique_key.sql": models__null_unique_key_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+incremental_strategy": self.strategy}}

    def test_null_key_rows_are_appended_not_replaced(self, project):
        run_dbt(["run"])
        run_dbt(["run"])

        rows = polars_read_relation(
            project.adapter, "null_unique_key", ["k1", "k2", "val"], order_by="val"
        )
        assert rows == [
            (1, None, "first_null"),
            (1, "a", "second"),
            (1, None, "second_null"),
        ]


class TestIncrementalNullUniqueKeyMerge(BaseIncrementalNullUniqueKey):
    strategy = "merge"


class TestIncrementalNullUniqueKeyDeleteInsert(BaseIncrementalNullUniqueKey):
    strategy = "delete+insert"
