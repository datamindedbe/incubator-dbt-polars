import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import check_relations_equal, run_dbt
from tests.functional.adapter.basic.files import (
    schema_base_yml,
    seeds_added_csv,
    seeds_base_csv,
)
from tests.utils import polars_relation_row_count

incremental_sql = """
{{ config(materialized="incremental", unique_key="id") }}
select * from {{ source('raw', 'seed') }}
"""

# Polars SQLContext doesn't support current_timestamp; use a fixed string for the key.
incremental_not_schema_change_sql = """
{{ config(materialized="incremental", unique_key="id",
   on_schema_change="sync_all_columns") }}
select
    1 as id,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class BaseIncremental:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv, "added.csv": seeds_added_csv}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        assert polars_relation_row_count(project.adapter, "base") == 10

        # added table rowcount
        assert polars_relation_row_count(project.adapter, "added") == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1


class BaseIncrementalNotSchemaChange:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}

    def test_incremental_not_schema_change(self, project):
        # Schema change is not evaluated on first run, so two are needed
        run_dbt(["run", "--select", "incremental_not_schema_change"])
        run_result = (
            run_dbt(["run", "--select", "incremental_not_schema_change"])
            .results[0]
            .status
        )

        assert run_result == RunStatus.Success


class Testincremental(BaseIncremental):
    pass


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass
