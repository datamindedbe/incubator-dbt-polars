"""Relations referenced outside a materialization resolve their config from the
manifest: run_query in a run-operation, and models run with --no-populate-cache."""

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from dbt.adapters.polars.testing.utils import polars_read_relation
from tests.functional.adapter.sources.test_sources import (
    _SCHEMA_YML,
    SourceSetupMixin,
)

parquet_model_sql = """
{{ config(materialized='table', file_format='parquet') }}
select 1 as id union all select 2 as id
"""

downstream_model_sql = """
{{ config(materialized='table') }}
select * from {{ ref('parquet_model') }}
"""

peek_macros_sql = """
{% macro peek_ref() %}
  {% set result = run_query("select count(*) as n from " ~ ref('parquet_model')) %}
  {% do log("ref rows: " ~ result.rows[0][0], info=True) %}
{% endmacro %}

{% macro peek_source() %}
  {% set relation = source('raw', 'people_csv') %}
  {% set result = run_query("select count(*) as n from " ~ relation) %}
  {% do log("source rows: " ~ result.rows[0][0], info=True) %}
{% endmacro %}
"""


class TestRunQueryInRunOperation(SourceSetupMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": _SCHEMA_YML, "parquet_model.sql": parquet_model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"peek.sql": peek_macros_sql}

    def test_run_query_on_ref(self, project):
        run_dbt(["run"])
        _, log_output = run_dbt_and_capture(["run-operation", "peek_ref"])
        assert "ref rows: 2" in log_output

    def test_run_query_on_source(self, project):
        _, log_output = run_dbt_and_capture(["run-operation", "peek_source"])
        assert "source rows: 3" in log_output


class TestRunWithoutPopulateCache(SourceSetupMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "parquet_model.sql": parquet_model_sql,
            "downstream_model.sql": downstream_model_sql,
        }

    def test_ref_resolves_without_populate_cache(self, project):
        run_dbt(["--no-populate-cache", "run"])
        rows = polars_read_relation(
            project.adapter, "downstream_model", ["id"], order_by="id"
        )
        assert rows == [(1,), (2,)]
