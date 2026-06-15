import pytest
from tests.profiles import default_target

# Patch check_relations_equal in dbt.tests.util before any test modules are
# imported, so all dbt base test classes automatically use the Polars-native
# comparison (this adapter has no SQL engine to run the EXCEPT-based SQL).
import dbt.tests.util
from tests.utils import polars_check_relations_equal

dbt.tests.util.check_relations_equal = polars_check_relations_equal

# import os
# import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return default_target()


class PolarsTestMixin:
    """Overrides SQL-based fixtures from dbt base test classes that don't apply
    to the Polars adapter (which has no SQL engine)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+materialized": "table"}}

    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
        )
        project.adapter.drop_schema(relation)

    # Uncomment to keep all schemas -- useful for debugging
    # @pytest.fixture(scope="class", autouse=True)
    # def keep_test_schema(self, project):
    #     project.drop_test_schema = lambda: None
    #     yield
