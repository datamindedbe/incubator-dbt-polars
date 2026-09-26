import csv as csv_module
import io
from pathlib import Path

import pytest
from dbt.tests import util
from dbt.tests.adapter.simple_seed import seeds
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedCustomSchema as DbtBaseSeedCustomSchema,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSeedParsing as DbtBaseSeedParsing,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSimpleSeedEnabledViaConfig as DbtBaseSimpleSeedEnabledViaConfig,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseSimpleSeedWithBOM as DbtBaseSimpleSeedWithBOM,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseTestEmptySeed,
    SeedTestBase,
)

import polars as pl
from dbt.adapters.polars.testing.mixin import (
    PolarsProfileMixin,
    PolarsTestMixin,
)


def parse_seed_expected_df() -> pl.DataFrame:
    rows = list(csv_module.DictReader(io.StringIO(seeds.seed__actual_csv)))
    return (
        pl.DataFrame(rows)
        .with_columns(pl.col(pl.String).replace("", None))
        .with_columns(
            pl.col("seed_id").cast(pl.Int64),
            pl.col("birthday").str.to_datetime(format="%Y-%m-%d %H:%M:%S"),
        )
    )


def write_seed_expected(project) -> None:
    """Write seed_expected via the adapter's catalog."""
    df = parse_seed_expected_df()
    with util.get_connection(project.adapter):
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="seed_expected",
        )
        catalog = project.adapter.get_storage_catalog(project.database)
        catalog.create_schema(relation)
        catalog.write_relation(relation, df, [])


class BaseBasicSeedTests(SeedTestBase, PolarsProfileMixin):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        write_seed_expected(project)


class BaseSeedCustomSchema(DbtBaseSeedCustomSchema, PolarsProfileMixin):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        write_seed_expected(project)


class BaseSimpleSeedWithBOM(DbtBaseSimpleSeedWithBOM, PolarsProfileMixin):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        write_seed_expected(project)
        util.copy_file(
            str(Path(seeds.__file__).parent),
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


class BaseEmptySeed(BaseTestEmptySeed, PolarsProfileMixin):
    pass


class BaseSimpleSeedEnabledViaConfig(
    PolarsTestMixin, DbtBaseSimpleSeedEnabledViaConfig
):
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


class BaseSeedParsing(DbtBaseSeedParsing, PolarsProfileMixin):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        pass

    def test_dbt_run_skips_seeds(self, project):
        pass


class BaseSeedWithExplicitCatalogParsing(PolarsProfileMixin):
    """Manifest parsing must not raise DbtCatalogIntegrationNotFoundError
    when seeds carry an explicit +catalog config. Requires a `secondary` catalog."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_actual.csv": seeds.seed__actual_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "+catalog": "secondary",
            },
        }

    def test_parse_with_explicit_catalog(self, project):
        util.run_dbt(["parse"])


class BaseSeedWithExplicitCatalog(PolarsProfileMixin):
    """A seed with an explicit catalog config is written to that catalog.
    Requires a `secondary` catalog."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_actual.csv": seeds.seed__actual_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "+catalog": "secondary",
            },
        }

    def test_seed_uses_explicit_catalog(self, project):
        from dbt.adapters.contracts.relation import RelationType

        result = util.run_dbt(["seed"])
        assert len(result) == 1

        with util.get_connection(project.adapter):
            secondary_relation = project.adapter.Relation.create(
                database="secondary",
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

            in_secondary = project.adapter.get_storage_catalog(
                "secondary"
            ).table_exists(secondary_relation)
            in_default = project.adapter.get_storage_catalog(
                project.database
            ).table_exists(default_relation)

        assert not in_default, "Seed incorrectly written to default catalog"
        assert in_secondary, "Seed not found in secondary catalog"
