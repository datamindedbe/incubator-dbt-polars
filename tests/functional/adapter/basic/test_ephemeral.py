import os

import pytest
from dbt.tests.util import (
    check_relations_equal,
    check_result_nodes_by_name,
    get_manifest,
    run_dbt,
)

from tests.functional.adapter.basic.files import (
    base_ephemeral_sql,
    ephemeral_table_sql,
    schema_base_yml,
    seeds_base_csv,
)
from tests.utils import polars_relation_row_count


class BaseEphemeral:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "ephemeral"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": base_ephemeral_sql,
            "second_model.sql": ephemeral_table_sql,
            "table_model.sql": ephemeral_table_sql,
            "schema.yml": schema_base_yml,
        }

    def test_ephemeral(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 1
        check_result_nodes_by_name(results, ["base"])

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2
        check_result_nodes_by_name(results, ["second_model", "table_model"])

        # base table rowcount
        assert polars_relation_row_count(project.adapter, "base") == 10

        # relations equal
        check_relations_equal(project.adapter, ["base", "second_model", "table_model"])

        # catalog node count
        catalog = run_dbt(["docs", "generate"])
        catalog_path = os.path.join(project.project_root, "target", "catalog.json")
        assert os.path.exists(catalog_path)
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

        # manifest (not in original)
        manifest = get_manifest(project.project_root)
        assert len(manifest.nodes) == 4
        assert len(manifest.sources) == 1


class TestEphemeral(BaseEphemeral):
    pass
