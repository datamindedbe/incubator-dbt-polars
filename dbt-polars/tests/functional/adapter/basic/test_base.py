import pytest
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    check_result_nodes_by_name,
    run_dbt,
)
from tests.functional.adapter.basic.files import (
    base_materialized_var_sql,
    base_table_sql,
    schema_base_yml,
    seeds_base_csv,
)
from tests.utils import polars_relation_row_count


class BaseSimpleMaterializations:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "second_model.sql": base_table_sql,
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_base(self, project):

        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(
            results, ["second_model", "table_model", "swappable"]
        )

        # check relation types
        expected = {
            "base": "table",
            "second_model": "table",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        assert polars_relation_row_count(project.adapter, "base") == 10

        # relations_equal
        check_relations_equal(
            project.adapter, ["base", "second_model", "table_model", "swappable"]
        )

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to incremental
        results = run_dbt(
            ["run", "-m", "swappable", "--vars", "materialized_var: incremental"]
        )
        assert len(results) == 1

        # check relation types, swappable is table (incremental materializes as table)
        expected = {
            "base": "table",
            "second_model": "table",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass
