import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import run_dbt, write_file
from tests.conftest import PolarsTestMixin
from tests.utils import polars_read_relation, polars_relation_partition_columns

models__partitioned_table_sql = """
{{ config(materialized='table', partition_by=['color']) }}
select 1 as id, 'blue' as color
union all
select 2 as id, 'red' as color
union all
select 3 as id, 'blue' as color
"""

models__partitioned_python_py = """
def model(dbt, session):
    import polars as pl
    dbt.config(materialized='table', partition_by=['color'])
    return pl.DataFrame({"id": [1, 2, 3], "color": ["blue", "red", "blue"]})
"""

models__repartition_table_v1_sql = """
{{ config(materialized='table', partition_by=['color']) }}
select 1 as id, 'blue' as color
union all
select 2 as id, 'red' as color
"""

models__repartition_table_v2_sql = """
{{ config(materialized='table', partition_by=['id']) }}
select 1 as id, 'blue' as color
union all
select 2 as id, 'red' as color
"""

models__partitioned_incremental_v1_sql = """
{{ config(materialized='incremental', partition_by=['color']) }}
select 1 as id, 'blue' as color
union all
select 2 as id, 'red' as color
"""

models__partitioned_incremental_v2_sql = """
{{ config(materialized='incremental', partition_by=['id']) }}
select 1 as id, 'blue' as color
union all
select 2 as id, 'red' as color
"""

models__partition_missing_column_sql = """
{{ config(materialized='table', partition_by=['does_not_exist']) }}
select 1 as id
"""

seeds__partitioned_csv = """id,color
1,blue
2,red
3,blue
"""


@pytest.mark.require_configs("default")
class TestPartitionByTable(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "partitioned_table.sql": models__partitioned_table_sql,
            "partitioned_python.py": models__partitioned_python_py,
        }

    def test__partition_by_table(self, project):
        run_dbt(["run"])
        assert polars_relation_partition_columns(
            project.adapter, "partitioned_table"
        ) == ["color"]
        assert polars_relation_partition_columns(
            project.adapter, "partitioned_python"
        ) == ["color"]
        rows = polars_read_relation(
            project.adapter, "partitioned_table", ["id", "color"], order_by="id"
        )
        assert rows == [(1, "blue"), (2, "red"), (3, "blue")]


@pytest.mark.require_configs("default")
class TestPartitionByTableRepartition(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"repartition_table.sql": models__repartition_table_v1_sql}

    def test__table_repartition_on_config_change(self, project):
        run_dbt(["run"])
        assert polars_relation_partition_columns(
            project.adapter, "repartition_table"
        ) == ["color"]

        write_file(
            models__repartition_table_v2_sql,
            project.project_root,
            "models",
            "repartition_table.sql",
        )
        run_dbt(["run"])  # plain run, no --full-refresh
        assert polars_relation_partition_columns(
            project.adapter, "repartition_table"
        ) == ["id"]
        rows = polars_read_relation(
            project.adapter, "repartition_table", ["id", "color"], order_by="id"
        )
        assert rows == [(1, "blue"), (2, "red")]


@pytest.mark.require_configs("default")
class TestPartitionByIncremental(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"partitioned_incremental.sql": models__partitioned_incremental_v1_sql}

    def test__incremental_preserves_partitioning(self, project):
        run_dbt(["run"])
        run_dbt(["run"])  # second run: append/merge path, same partition_by
        assert polars_relation_partition_columns(
            project.adapter, "partitioned_incremental"
        ) == ["color"]


@pytest.mark.require_configs("default")
class TestPartitionByIncrementalGuard(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"partitioned_incremental.sql": models__partitioned_incremental_v1_sql}

    def test__incremental_partition_change_requires_full_refresh(self, project):
        run_dbt(["run"])
        run_dbt(["run"])  # establish the append/merge path with partition_by=['color']

        write_file(
            models__partitioned_incremental_v2_sql,
            project.project_root,
            "models",
            "partitioned_incremental.sql",
        )
        results = run_dbt(["run"], expect_pass=False)
        assert results[0].status == RunStatus.Error
        assert "--full-refresh" in results[0].message

        run_dbt(["run", "--full-refresh"])
        assert polars_relation_partition_columns(
            project.adapter, "partitioned_incremental"
        ) == ["id"]


class TestPartitionBySeed(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"partitioned_seed.csv": seeds__partitioned_csv}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "partitioned_seed": {"+partition_by": ["color"]},
                },
                "quote_columns": False,
            },
        }

    def test__partition_by_seed(self, project):
        run_dbt(["seed"])
        assert polars_relation_partition_columns(
            project.adapter, "partitioned_seed"
        ) == ["color"]


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
