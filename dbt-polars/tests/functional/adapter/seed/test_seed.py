import csv as csv_module
import io
from pathlib import Path

import polars as pl
import pytest
from dbt.tests import util
from dbt.tests.adapter.simple_seed import seeds
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedCustomSchema,
    BaseSeedParsing,
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


def _parse_seed_expected_df() -> pl.DataFrame:
    rows = list(csv_module.DictReader(io.StringIO(seeds.seed__actual_csv)))
    return (
        pl.DataFrame(rows)
        .with_columns(pl.col(pl.String).replace("", None))
        .with_columns(
            pl.col("seed_id").cast(pl.Int64),
            pl.col("birthday").str.to_datetime(format="%Y-%m-%d %H:%M:%S"),
        )
    )


def _write_seed_expected(project) -> None:
    """Write seed_expected as a Delta table directly to the catalog root path."""
    df = _parse_seed_expected_df()
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


def _write_seed_expected_iceberg(project) -> None:
    """Write seed_expected via the adapter's catalog."""
    from dbt.tests.util import get_connection

    df = _parse_seed_expected_df()
    with get_connection(project.adapter):
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="seed_expected",
        )
        catalog = project.adapter.get_storage_catalog(project.database)
        catalog.create_schema(relation)
        catalog.write_relation(relation, df, [])


@pytest.mark.require_profiles("local")
class TestBasicSeedTests(SeedTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected(project)


@pytest.mark.require_profiles("local")
class TestSeedCustomSchema(BaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected(project)


class TestEmptySeed(BaseTestEmptySeed):
    pass


class TestSimpleSeedEnabledViaConfig(PolarsTestMixin, BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "seed_enabled": {"enabled": True},
                    "seed_disabled": {"enabled": False},
                },
                "quote_columns": False,
            },
        }


class TestSeedParsing(Setup, BaseSeedParsing):
    def test_dbt_run_skips_seeds(self, project):
        pass


@pytest.mark.require_profiles("local")
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


@pytest.mark.require_profiles("iceberg")
class TestBasicSeedTestsIceberg(SeedTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected_iceberg(project)


@pytest.mark.require_profiles("iceberg")
class TestSeedCustomSchemaIceberg(BaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected_iceberg(project)


@pytest.mark.require_profiles("iceberg")
class TestSimpleSeedWithBOMIceberg(BaseSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        _write_seed_expected_iceberg(project)
        util.copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


@pytest.mark.skip_profiles("iceberg-databricks")
class TestSeedWithExplicitCatalogParsing:
    """Manifest parsing must not raise DbtCatalogIntegrationNotFoundError
    when seeds carry an explicit +catalog config."""

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

    def test_parse_with_explicit_catalog(self, project):
        util.run_dbt(["parse"])


@pytest.mark.skip_profiles("iceberg-databricks")
class TestSeedWithExplicitCatalog:
    """Verify that a seed with an explicit catalog config is written
    to the correct catalog."""

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

        with util.get_connection(project.adapter):
            from dbt.adapters.contracts.relation import RelationType

            local2_relation = project.adapter.Relation.create(
                database="local2",
                schema=project.test_schema,
                identifier="seed_actual",
                type=RelationType.Table,
            )
            default_relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema,
                identifier="seed_actual",
                type=RelationType.Table,
            )

            in_local2 = project.adapter.get_storage_catalog("local2").table_exists(
                local2_relation
            )
            in_default = project.adapter.get_storage_catalog(
                project.database
            ).table_exists(default_relation)

        assert not in_default, "Seed incorrectly written to default catalog"
        assert in_local2, "Seed not found in local2 catalog"
