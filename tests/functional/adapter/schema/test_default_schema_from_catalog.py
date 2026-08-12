import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.util import get_connection, run_dbt

from dbt.adapters.polars.relation import PolarsRelation
from tests.conftest import PolarsTestMixin

_MODEL = """
{{ config(materialized='table') }}
SELECT 1 AS id
"""

_SEED_CSV = "id,value\n1,hello\n2,world\n"


@pytest.mark.require_configs("default")
class TestDefaultSchemaFromCatalog(PolarsTestMixin):
    """A model without a `schema` config should default to the schema
    configured on the catalog it writes to, not a global/top-level schema.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"no_schema_model.sql": _MODEL}

    def test_schema_inferred_from_catalog(self, project):
        run_dbt(["run"])

        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            relation = PolarsRelation.create(
                database=project.database,
                schema=catalog.schema,
                identifier="no_schema_model",
                type=RelationType.Table,
                catalog=project.database,
            )
            assert catalog.table_exists(relation), (
                "model's default schema should come from the catalog's own "
                "`schema` config when the model doesn't set one explicitly"
            )


@pytest.mark.require_configs("default")
class TestSeedDefaultSchemaFromCatalog(PolarsTestMixin):
    """A seed without a `schema` config should default to the schema
    configured on the catalog it writes to, not a global/top-level schema.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"no_schema_seed.csv": _SEED_CSV}

    def test_schema_inferred_from_catalog(self, project):
        run_dbt(["seed"])

        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            relation = PolarsRelation.create(
                database=project.database,
                schema=catalog.schema,
                identifier="no_schema_seed",
                type=RelationType.Table,
                catalog=project.database,
            )
            assert catalog.table_exists(relation), (
                "seed's default schema should come from the catalog's own "
                "`schema` config when the seed doesn't set one explicitly"
            )
