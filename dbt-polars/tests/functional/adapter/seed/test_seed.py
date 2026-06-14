import csv as csv_module
import io
from pathlib import Path

import polars as pl
import pytest
from dbt.tests import util
from dbt.tests.adapter.simple_seed import seeds
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseBasicSeedTests,
    BaseSeedConfigFullRefreshOff,
    BaseSeedCustomSchema,
    BaseSeedParsing,
    BaseSeedSpecificFormats,
    BaseSeedWithEmptyDelimiter,
    BaseSeedWithUniqueDelimiter,
    BaseSeedWithWrongDelimiter,
    BaseSimpleSeedEnabledViaConfig,
    BaseSimpleSeedWithBOM,
    BaseTestEmptySeed,
    SeedTestBase,
)
from tests.conftest import PolarsTestMixin


class Setup:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        pass


def _write_seed_expected(project) -> None:
    """Write seed_expected as a Delta table directly to the catalog root path."""
    rows = list(csv_module.DictReader(io.StringIO(seeds.seed__actual_csv)))
    df = (
        pl.DataFrame(rows)
        .with_columns(pl.col(pl.String).replace("", None))
        .with_columns(
            pl.col("seed_id").cast(pl.Int64),
            pl.col("birthday").str.to_datetime(format="%Y-%m-%d %H:%M:%S"),
        )
    )
    catalog_root = project.adapter.config.credentials.catalog_configs[
        project.database
    ].root
    path = (
        Path(project.project_root)
        / catalog_root
        / project.test_schema
        / "seed_expected"
    )
    path.mkdir(parents=True, exist_ok=True)
    df.write_delta(str(path), mode="overwrite")


# Can't pass the full-refresh flag test as Databricks does not have cascade support
class TestBasicSeedTests(SeedTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected(project)


class TestSeedCustomSchema(BaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected(project)


class TestEmptySeed(BaseTestEmptySeed):
    pass


class TestSimpleSeedEnabledViaConfig(PolarsTestMixin, BaseSimpleSeedEnabledViaConfig):
    pass


class TestSeedParsing(Setup, BaseSeedParsing):
    @pytest.mark.skip(reason="requires SQL model execution, not supported by the Polars adapter")
    def test_dbt_run_skips_seeds(self, project):
        # TODO: Remove this function once supported
        pass


class TestSimpleSeedWithBOM(BaseSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected(project)
        util.copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


class TestSeedWithExplicitCatalog:
    """Verify that a seed with an explicit catalog config is written to the correct catalog."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_actual.csv": seeds.seed__actual_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "+catalog": "local2",
            },
        }

    def test_seed_uses_explicit_catalog(self, project):
        result = util.run_dbt(["seed"])
        assert len(result) == 1

        credentials = project.adapter.config.credentials
        local2_root = credentials.catalog_configs["local2"].root
        default_root = credentials.catalog_configs[project.database].root

        seed_in_local2 = (
            Path(project.project_root) / local2_root / project.test_schema / "seed_actual"
        )
        seed_in_default = (
            Path(project.project_root) / default_root / project.test_schema / "seed_actual"
        )

        assert not seed_in_default.exists(), f"Seed incorrectly written to default catalog at {seed_in_default}"
        assert seed_in_local2.exists(), f"Seed not found in local2 catalog at {seed_in_local2}"