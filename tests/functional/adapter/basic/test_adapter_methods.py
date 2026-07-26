import pytest
from dbt.tests.util import check_relations_equal, get_connection, run_dbt

tests__get_columns_in_relation_sql = """
{% set columns = adapter.get_columns_in_relation(ref('model')) %}
{% set limit_query = 0 %}
{% if (columns | length) == 0 %}
    {% set limit_query = 1 %}
{% endif %}

select 1 as id limit {{ limit_query }}

"""

models__upstream_sql = """
select 1 as id

"""

models__expected_sql = """
-- make sure this runs after 'model'
-- {{ ref('model') }}
select 2 as id

"""

models__model_sql = """
select 2 as id

"""


class BaseAdapterMethod:
    """
    Tests drop_schema, create_schema, get_relation, list_schemas, and
    get_columns_in_relation.
    """

    @pytest.fixture(scope="class")
    def tests(self):
        return {"get_columns_in_relation.sql": tests__get_columns_in_relation_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": models__upstream_sql,
            "expected.sql": models__expected_sql,
            "model.sql": models__model_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "adapter_methods",
            "models": {"+materialized": "table"},
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

    @pytest.fixture(scope="class")
    def equal_tables(self):
        return ["model", "expected"]

    def test_adapter_methods(self, project, equal_tables):
        run_dbt(["compile"])  # trigger any compile-time issues
        result = run_dbt()
        assert len(result) == 3
        check_relations_equal(project.adapter, equal_tables)

        with get_connection(project.adapter):
            schema_relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )

            # drop_schema removes the schema
            project.adapter.drop_schema(schema_relation)
            assert project.test_schema not in project.adapter.list_schemas(
                project.database
            )

            # get_relation returns None for a relation in a dropped schema
            assert (
                project.adapter.get_relation(
                    project.database, project.test_schema, "upstream"
                )
                is None
            )

            # create_schema makes the schema visible again
            project.adapter.create_schema(schema_relation)
            assert project.test_schema in project.adapter.list_schemas(project.database)


class TestBaseCaching(BaseAdapterMethod):
    pass
