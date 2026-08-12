import os
from pathlib import Path

import pytest
from dbt.tests.util import run_dbt

from tests.conftest import PolarsTestMixin

_MODEL = """
{{ config(materialized='table') }}
SELECT 1 AS id
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestRelativeLocalRootFromOtherCwd(PolarsTestMixin):
    """A catalog's relative filesystem path (here, local's `root`) must
    resolve relative to the dbt project directory, not relative to
    whatever directory the shell happened to be in when dbt was invoked.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"cwd_model.sql": _MODEL}

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
                                "root": "local_data",
                                "schema": unique_schema,
                            }
                        ],
                    }
                },
                "target": "default",
            }
        }

    def test_relative_root_resolves_against_project_dir(self, project, tmp_path):
        other_cwd = tmp_path / "elsewhere"
        other_cwd.mkdir()

        orig_cwd = os.getcwd()
        os.chdir(other_cwd)
        try:
            run_dbt(["run", "--project-dir", str(project.project_root)])
        finally:
            os.chdir(orig_cwd)

        project_dir = Path(str(project.project_root))
        assert (project_dir / "local_data").exists(), (
            "relative catalog path should resolve against the project "
            "directory, regardless of the invocation cwd"
        )
        assert not (other_cwd / "local_data").exists(), (
            "relative catalog path incorrectly resolved against the cwd dbt "
            "was invoked from, instead of the project directory"
        )
