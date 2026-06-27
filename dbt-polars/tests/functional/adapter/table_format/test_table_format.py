import pytest
from dbt.adapters.polars.relation import TableFormat
from dbt.tests.util import get_connection, relation_from_name, run_dbt
from tests.conftest import PolarsTestMixin, with_table_format

SEEDS__BASIC = """id,name
1,a
2,b
"""

MODELS__BASIC = "select 1 as id"


def _table_format_of(project, name: str) -> TableFormat:
    """Look up `name` via list_relations_without_caching and return its format."""
    with get_connection(project.adapter):
        relation = relation_from_name(project.adapter, name)
        schema_relation = project.adapter.Relation.create(
            database=relation.database, schema=relation.schema
        )
        relations = project.adapter.list_relations_without_caching(schema_relation)

    matches = [r for r in relations if r.identifier == relation.identifier]
    assert len(matches) == 1, (
        f"expected exactly one relation named {name!r} in the schema, found {matches}"
    )
    return matches[0].format


@pytest.mark.table_format_specific
class TestTableFormat(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def project_config_update(self, table_format):
        config = with_table_format(
            {"models": {"+materialized": "table"}}, table_format, resource="models"
        )
        return with_table_format(config, table_format, resource="seeds")

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"basic_seed.csv": SEEDS__BASIC}

    @pytest.fixture(scope="class")
    def models(self):
        return {"basic_model.sql": MODELS__BASIC}

    def test_seed_and_model_use_the_selected_table_format(self, project, table_format):
        run_dbt(["seed"])
        run_dbt(["run"])

        assert _table_format_of(project, "basic_seed").value == table_format
        assert _table_format_of(project, "basic_model").value == table_format
