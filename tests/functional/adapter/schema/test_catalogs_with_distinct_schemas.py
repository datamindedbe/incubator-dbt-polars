import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.util import get_connection, run_dbt

from dbt.adapters.polars.relation import PolarsRelation
from tests.conftest import PolarsTestMixin

_MODEL_A = """
{{ config(materialized='table', catalog='local') }}
SELECT 1 AS id
"""

_MODEL_B = """
{{ config(materialized='table', catalog='local2') }}
SELECT 1 AS id
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestCatalogsWithDistinctSchemas(PolarsTestMixin):
    """Each catalog should write to its own configured schema, even when
    those schemas differ from one catalog to the next.
    """

    @pytest.fixture(scope="class")
    def profiles_config_update(self, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {
                        "type": "polars",
                        "catalogs": [
                            {
                                "type": "local",
                                "name": "local",
                                "root": "data_a",
                                "schema": f"{unique_schema}_a",
                            },
                            {
                                "type": "local",
                                "name": "local2",
                                "root": "data_b",
                                "schema": f"{unique_schema}_b",
                            },
                        ],
                    }
                },
                "target": "default",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"model_a.sql": _MODEL_A, "model_b.sql": _MODEL_B}

    def test_each_catalog_writes_to_its_own_schema(self, project, unique_schema):
        run_dbt(["run"])

        with get_connection(project.adapter):
            catalog_a = project.adapter.get_storage_catalog("local")
            catalog_b = project.adapter.get_storage_catalog("local2")

            relation_a = PolarsRelation.create(
                database="local",
                schema=f"{unique_schema}_a",
                identifier="model_a",
                type=RelationType.Table,
                catalog="local",
            )
            relation_b = PolarsRelation.create(
                database="local2",
                schema=f"{unique_schema}_b",
                identifier="model_b",
                type=RelationType.Table,
                catalog="local2",
            )

            assert catalog_a.table_exists(relation_a), (
                "model_a should land in local's own schema"
            )
            assert catalog_b.table_exists(relation_b), (
                "model_b should land in local2's own schema"
            )
