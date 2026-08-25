import os
from pathlib import Path

import pytest
from dbt.tests.util import run_dbt

from tests.conftest import PolarsTestMixin

_MODEL = """
{{ config(materialized='table') }}
SELECT 1 AS id
"""


@pytest.mark.require_profiles("iceberg")
@pytest.mark.require_configs("default")
class TestRelativeWarehouseFromOtherCwd(PolarsTestMixin):
    """A catalog's relative filesystem path (here, iceberg's `warehouse`)
    must resolve relative to the dbt project directory, not relative to
    whatever directory the shell happened to be in when dbt was invoked.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"cwd_model.sql": _MODEL}

    @pytest.fixture(scope="class")
    def profiles_config_update(self, unique_schema, tmp_path_factory):
        # The sqlite catalog db is anchored to an absolute tmp path so this
        # test isolates the `warehouse` resolution specifically. `warehouse`
        # itself stays relative — that's what's under test.
        catalog_dir = str(tmp_path_factory.mktemp("iceberg_catalog_db"))
        return {
            "test": {
                "outputs": {
                    "default": {
                        "type": "polars",
                        "catalogs": [
                            {
                                "type": "iceberg",
                                "name": "local",
                                "uri": f"sqlite:///{catalog_dir}/local.db",
                                "warehouse": "file://iceberg_warehouse",
                                "schema": unique_schema,
                            }
                        ],
                    }
                },
                "target": "default",
            }
        }

    def test_relative_warehouse_resolves_against_project_dir(self, project, tmp_path):
        other_cwd = tmp_path / "elsewhere"
        other_cwd.mkdir()

        orig_cwd = os.getcwd()
        os.chdir(other_cwd)
        try:
            run_dbt(["run", "--project-dir", str(project.project_root)])
        finally:
            os.chdir(orig_cwd)

        project_dir = Path(str(project.project_root))
        assert (project_dir / "iceberg_warehouse").exists(), (
            "relative catalog path should resolve against the project "
            "directory, regardless of the invocation cwd"
        )
        assert not (other_cwd / "iceberg_warehouse").exists(), (
            "relative catalog path incorrectly resolved against the cwd dbt "
            "was invoked from, instead of the project directory"
        )
