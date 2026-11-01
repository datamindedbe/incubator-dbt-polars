import pytest
from dbt.tests.util import run_dbt

from dbt.adapters.polars.testing.data_operations import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeAppend,
)
from dbt.adapters.polars.testing.mixin import PolarsTestMixin


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


@pytest.mark.require_profiles("local")
class TestIncrementalOnSchemaChangeParquet(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+file_format": "parquet"}}


class TestIncrementalOnSchemaChangeAppend(BaseIncrementalOnSchemaChangeAppend):
    pass


@pytest.mark.skip_configs("csv")  # CSV doesn't store column types
class TestIncrementalOnSchemaChangeFailWithTimestamp(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_timestamp.sql": (
                "{{ config(materialized='incremental', on_schema_change='fail') }}\n"
                "select 1 as id, cast('2024-01-01T00:00:00' as timestamp) as loaded_at"
            )
        }

    def test_unchanged_timestamp_column_is_not_a_type_change(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
