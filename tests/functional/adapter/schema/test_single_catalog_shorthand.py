import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.util import get_connection, run_dbt

from dbt.adapters.polars.relation import PolarsRelation
from tests.conftest import PolarsTestMixin

_MODEL = """
{{ config(materialized='table') }}
SELECT 1 AS id
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestSingleCatalogShorthandProfile(PolarsTestMixin):
    """A profile using the single-catalog shorthand (`catalog_type` plus the
    catalog's own options at the top level, no explicit `catalogs` list)
    should run end to end, not just parse at the credentials level.
    """

    @pytest.fixture(scope="class")
    def profiles_config_update(self, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {
                        "type": "polars",
                        "catalog_type": "local",
                        "root": "shorthand_data",
                        "schema": unique_schema,
                    }
                },
                "target": "default",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"shorthand_model.sql": _MODEL}

    def test_shorthand_profile_runs(self, project):
        run_dbt(["run"])

        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            relation = PolarsRelation.create(
                database=project.database,
                schema=project.test_schema,
                identifier="shorthand_model",
                type=RelationType.Table,
                catalog=project.database,
            )
            assert catalog.table_exists(relation)
