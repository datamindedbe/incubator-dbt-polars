from pathlib import Path

import pytest
from dbt.tests import util
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedCustomSchema as DbtBaseSeedCustomSchema,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSimpleSeedWithBOM as DbtBaseSimpleSeedWithBOM,
)
from dbt.tests.adapter.simple_seed.test_seed import SeedTestBase

from dbt.adapters.polars.testing.catalog import (
    BaseBasicSeedTests,
    BaseEmptySeed,
    BaseSeedCustomSchema,
    BaseSeedParsing,
    BaseSeedWithExplicitCatalog,
    BaseSeedWithExplicitCatalogParsing,
    BaseSimpleSeedEnabledViaConfig,
    BaseSimpleSeedWithBOM,
)
from dbt.adapters.polars.testing.catalog.seed import parse_seed_expected_df


def write_seed_expected_to_disk(project) -> None:
    """Write seed_expected as a Delta table directly to the catalog root path."""
    df = parse_seed_expected_df()
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


@pytest.mark.require_profiles("local")
class TestBasicSeedTestsLocal(SeedTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        write_seed_expected_to_disk(project)


@pytest.mark.require_profiles("local")
class TestSeedCustomSchemaLocal(DbtBaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        write_seed_expected_to_disk(project)


@pytest.mark.require_profiles("local")
class TestSimpleSeedWithBOMLocal(DbtBaseSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        write_seed_expected_to_disk(project)
        util.copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


@pytest.mark.require_profiles("iceberg")
class TestBasicSeedTests(BaseBasicSeedTests):
    pass


@pytest.mark.require_profiles("iceberg")
class TestSeedCustomSchema(BaseSeedCustomSchema):
    pass


@pytest.mark.require_profiles("iceberg")
class TestSimpleSeedWithBOM(BaseSimpleSeedWithBOM):
    pass


class TestEmptySeed(BaseEmptySeed):
    pass


class TestSimpleSeedEnabledViaConfig(BaseSimpleSeedEnabledViaConfig):
    pass


class TestSeedParsing(BaseSeedParsing):
    pass


@pytest.mark.skip_profiles("iceberg-databricks", "databricks")
class TestSeedWithExplicitCatalogParsing(BaseSeedWithExplicitCatalogParsing):
    pass


@pytest.mark.skip_profiles("iceberg-databricks", "databricks")
class TestSeedWithExplicitCatalog(BaseSeedWithExplicitCatalog):
    pass
