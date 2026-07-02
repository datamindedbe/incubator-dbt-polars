import pytest
from dbt.tests.adapter.basic import files
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests


class TestSingularTests(BaseSingularTests):
    pass


class TestGenericTests(BaseGenericTests):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": files.config_materialized_table + files.model_base,
            "table_model.sql": files.base_table_sql,
            "schema.yml": files.schema_base_yml,
            "schema_view.yml": files.generic_test_view_yml,
            "schema_table.yml": files.generic_test_table_yml,
        }
