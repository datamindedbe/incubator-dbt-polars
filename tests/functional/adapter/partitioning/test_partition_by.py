import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import run_dbt

from dbt.adapters.polars.testing.data_operations import (
    BasePartitionByIncremental,
    BasePartitionByIncrementalGuard,
    BasePartitionBySeed,
    BasePartitionByTable,
    BasePartitionByTableRepartition,
)
from dbt.adapters.polars.testing.data_operations.partitioning import (
    models__partitioned_table_sql,
)
from dbt.adapters.polars.testing.mixin import PolarsTestMixin

models__partition_missing_column_sql = """
{{ config(materialized='table', partition_by=['does_not_exist']) }}
select 1 as id
"""


@pytest.mark.require_configs("default")
class TestPartitionByTable(BasePartitionByTable):
    pass


@pytest.mark.require_configs("default")
class TestPartitionByTableRepartition(BasePartitionByTableRepartition):
    pass


@pytest.mark.require_configs("default")
class TestPartitionByIncremental(BasePartitionByIncremental):
    pass


@pytest.mark.require_configs("default")
class TestPartitionByIncrementalGuard(BasePartitionByIncrementalGuard):
    pass


class TestPartitionBySeed(BasePartitionBySeed):
    pass


@pytest.mark.skip_configs("default")
class TestPartitionByFileFormatError(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"partitioned_table.sql": models__partitioned_table_sql}

    def test__partition_by_unsupported_for_file_format(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert results[0].status == RunStatus.Error
        assert "partition_by" in results[0].message
        assert "delta" in results[0].message


@pytest.mark.require_profiles("iceberg")
class TestPartitionByMissingColumn(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"partition_missing_column.sql": models__partition_missing_column_sql}

    def test__partition_by_missing_column_errors(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert results[0].status == RunStatus.Error
        assert "does_not_exist" in results[0].message
