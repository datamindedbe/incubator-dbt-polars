import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import get_connection, relation_from_name, run_dbt

from tests.conftest import PolarsTestMixin


# First run: 1 row. Incremental run: 1 new row (different id → append grows table).
_APPEND_MODEL = """
{{ config(materialized='incremental') }}

{% if is_incremental() %}
    SELECT 2 AS id, 'b' AS name
{% else %}
    SELECT 1 AS id, 'a' AS name
{% endif %}
"""

# First run: 1 row. Incremental run: same id, different value → merge updates in place.
_MERGE_MODEL = """
{{ config(materialized='incremental', unique_key='id', incremental_strategy='merge') }}

{% if is_incremental() %}
    SELECT 1 AS id, 'updated' AS name
{% else %}
    SELECT 1 AS id, 'original' AS name
{% endif %}
"""

# First run: 2 rows. Incremental run: replaces one existing row + inserts one new row.
_DELETE_INSERT_MODEL = """
{{ config(materialized='incremental', unique_key='id', incremental_strategy='delete+insert') }}

{% if is_incremental() %}
    SELECT 1 AS id, 'updated' AS name
    UNION ALL
    SELECT 3 AS id, 'new' AS name
{% else %}
    SELECT 1 AS id, 'original' AS name
    UNION ALL
    SELECT 2 AS id, 'keep' AS name
{% endif %}
"""


def _read_relation(project, name: str):
    with get_connection(project.adapter):
        rel = relation_from_name(project.adapter, name)
        return (
            project.adapter.get_catalog(rel.database)
            .get_relation(rel)
            .collect()
            .sort("id")
        )


class TestIncrementalAppend(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"append_model.sql": _APPEND_MODEL}

    def test_append(self, project):
        run_dbt(["run"])
        df_after_first = _read_relation(project, "append_model")
        assert df_after_first["id"].to_list() == [1]

        run_dbt(["run"])
        df_after_second = _read_relation(project, "append_model")
        assert df_after_second["id"].to_list() == [1, 2]
        assert df_after_second["name"].to_list() == ["a", "b"]


class TestIncrementalFullRefresh(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"append_model.sql": _APPEND_MODEL}

    def test_full_refresh_recreates_table(self, project):
        run_dbt(["run"])
        run_dbt(["run"])  # append so table has 2 rows
        run_dbt(["run", "--full-refresh"])
        df = _read_relation(project, "append_model")
        # full-refresh re-runs the non-incremental branch → back to 1 row
        assert df["id"].to_list() == [1]


class TestIncrementalMerge(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": _MERGE_MODEL}

    def test_merge_upserts_existing_row(self, project):
        run_dbt(["run"])
        df_after_first = _read_relation(project, "merge_model")
        assert df_after_first["name"].to_list() == ["original"]

        run_dbt(["run"])
        df_after_second = _read_relation(project, "merge_model")
        # merge should update the existing row, not add a second one
        assert len(df_after_second) == 1
        assert df_after_second["name"].to_list() == ["updated"]


_FAIL_ON_TYPE_CHANGE_MODEL = """
{{ config(materialized='incremental', on_schema_change='fail') }}

{% if is_incremental() %}
    SELECT 2 AS id, 'hello' AS value
{% else %}
    SELECT 1 AS id, 42 AS value
{% endif %}
"""


class TestIncrementalFailOnTypeChange(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"fail_on_type_change.sql": _FAIL_ON_TYPE_CHANGE_MODEL}

    def test_fail_on_type_change(self, project):
        run_dbt(["run"])

        results = run_dbt(["run"], expect_pass=False)
        assert results[0].status == RunStatus.Error
        assert "Compilation Error" in results[0].message


class TestIncrementalDeleteInsert(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"delete_insert_model.sql": _DELETE_INSERT_MODEL}

    def test_delete_insert(self, project):
        run_dbt(["run"])
        df_after_first = _read_relation(project, "delete_insert_model")
        assert df_after_first["id"].to_list() == [1, 2]

        run_dbt(["run"])
        df_after_second = _read_relation(project, "delete_insert_model")
        # id=1 replaced, id=2 kept, id=3 inserted
        assert df_after_second["id"].to_list() == [1, 2, 3]
        assert df_after_second["name"].to_list() == ["updated", "keep", "new"]
