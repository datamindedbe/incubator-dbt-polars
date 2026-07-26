"""Tests that cover all execution paths where a dbt source is consumed.

Each test class pre-writes sources (CSV and delta) and exercises a different
kind of consumer to verify that _source_configs correctly propagates
file_format and catalog across SQL models, Python models, ephemeral models,
Python tests, and SQL tests.

The delta source has no config: key at all — this tests that the default
file_format ("delta") is applied when a source carries no explicit config.

TestSourceCrossCatalog additionally writes a source into the secondary local2
catalog and verifies a model in the default local catalog can read from it,
exercising the cross-catalog lookup path.
"""

import polars as pl
import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.util import get_connection, run_dbt

from dbt.adapters.polars.relation import PolarsRelation
from tests.conftest import PolarsTestMixin
from tests.utils import polars_read_relation

_SOURCE_DATA = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

_SCHEMA_YML = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people_csv
        config:
          file_format: csv
      - name: people_delta
"""

_SCHEMA_YML_CROSS_CATALOG = """
version: 2
sources:
  - name: external
    database: local2
    schema: "{{ target.schema }}"
    tables:
      - name: people_csv
        config:
          file_format: csv
"""


def _write_relation(
    project, catalog_name: str, identifier: str, file_format: str
) -> None:
    relation = PolarsRelation.create(
        database=catalog_name,
        schema=project.test_schema,
        identifier=identifier,
        type=RelationType.Table,
        catalog=catalog_name,
        file_format=file_format,
    )
    with get_connection(project.adapter):
        catalog = project.adapter.get_storage_catalog(catalog_name)
        catalog.create_schema(relation)
        catalog.write_relation(relation, _SOURCE_DATA, [], {})


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class SourceSetupMixin(PolarsTestMixin):
    """Writes a CSV source and a delta source (no config key) into the local catalog."""

    @pytest.fixture(scope="class", autouse=True)
    def write_source_data(self, project):
        _write_relation(project, "local", "people_csv", "csv")
        _write_relation(project, "local", "people_delta", "delta")


class TestSourceInSqlModel(SourceSetupMixin):
    """SQL models reading from CSV and delta sources return the correct rows."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _SCHEMA_YML,
            "from_csv.sql": """
{{ config(materialized='table') }}
SELECT * FROM {{ source('raw', 'people_csv') }}
""",
            "from_delta.sql": """
{{ config(materialized='table') }}
SELECT * FROM {{ source('raw', 'people_delta') }}
""",
        }

    def test_sql_model_reads_csv_source(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "from_csv", ["id", "name"], order_by="id"
        )
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]

    def test_sql_model_reads_delta_source(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "from_delta", ["id", "name"], order_by="id"
        )
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]


class TestSourceInPythonModel(SourceSetupMixin):
    """Python models using dbt.source() reach _source_configs for file_format."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _SCHEMA_YML,
            "from_csv.py": """
def model(dbt, _):
    dbt.config(materialized="table")
    return dbt.source("raw", "people_csv")
""",
            "from_delta.py": """
def model(dbt, _):
    dbt.config(materialized="table")
    return dbt.source("raw", "people_delta")
""",
        }

    def test_python_model_reads_csv_source(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "from_csv", ["id", "name"], order_by="id"
        )
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]

    def test_python_model_reads_delta_source(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "from_delta", ["id", "name"], order_by="id"
        )
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]


class TestSourceInEphemeralModel(SourceSetupMixin):
    """Sources inside ephemeral models are resolved via _source_configs when
    the ephemeral body is inlined as a CTE into the downstream model."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _SCHEMA_YML,
            "csv_ephemeral.sql": """
{{ config(materialized='ephemeral') }}
SELECT * FROM {{ source('raw', 'people_csv') }}
""",
            "delta_ephemeral.sql": """
{{ config(materialized='ephemeral') }}
SELECT * FROM {{ source('raw', 'people_delta') }}
""",
            "from_csv_ephemeral.sql": """
{{ config(materialized='table') }}
SELECT * FROM {{ ref('csv_ephemeral') }}
""",
            "from_delta_ephemeral.sql": """
{{ config(materialized='table') }}
SELECT * FROM {{ ref('delta_ephemeral') }}
""",
        }

    def test_ephemeral_wrapping_csv_source(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "from_csv_ephemeral", ["id", "name"], order_by="id"
        )
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]

    def test_ephemeral_wrapping_delta_source(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "from_delta_ephemeral", ["id", "name"], order_by="id"
        )
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]


class TestSourceInPythonTest(SourceSetupMixin):
    """Python singular tests can access both CSV and delta sources via dbt.source()."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": _SCHEMA_YML}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "csv_source_no_nulls.py": """
def test(dbt, pl):
    df = dbt.source("raw", "people_csv")
    return df.filter(pl.col("name").is_null())
""",
            "delta_source_no_nulls.py": """
def test(dbt, pl):
    df = dbt.source("raw", "people_delta")
    return df.filter(pl.col("name").is_null())
""",
            "csv_source_has_rows_failing.py": """
def test(dbt, pl):
    df = dbt.source("raw", "people_csv")
    return df.filter(pl.col("id") > 0)
""",
        }

    def test_python_tests_can_read_sources(self, project):
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 3
        by_name = {r.node.name: r for r in results}
        assert by_name["csv_source_no_nulls"].status == "pass"
        assert by_name["delta_source_no_nulls"].status == "pass"
        assert by_name["csv_source_has_rows_failing"].status == "fail"


class TestSourceInSqlTest(SourceSetupMixin):
    """SQL singular tests can reference CSV and delta sources via {{ source() }},
    exercising _build_sql_context → _source_configs."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": _SCHEMA_YML}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "csv_source_no_null_ids.sql": """
SELECT * FROM {{ source('raw', 'people_csv') }} WHERE id IS NULL
""",
            "delta_source_no_null_ids.sql": """
SELECT * FROM {{ source('raw', 'people_delta') }} WHERE id IS NULL
""",
            "csv_source_returns_rows_failing.sql": """
SELECT * FROM {{ source('raw', 'people_csv') }}
""",
        }

    def test_sql_tests_can_read_sources(self, project):
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 3
        by_name = {r.node.name: r for r in results}
        assert by_name["csv_source_no_null_ids"].status == "pass"
        assert by_name["delta_source_no_null_ids"].status == "pass"
        assert by_name["csv_source_returns_rows_failing"].status == "fail"


@pytest.mark.require_profiles("local")
class TestSourceCrossCatalog(PolarsTestMixin):
    """A source declared with database: local2 is read by a model in the default
    local catalog, verifying that _source_configs uses the source's own catalog
    for the lookup key rather than the executing model's catalog."""

    @pytest.fixture(scope="class", autouse=True)
    def write_source_data(self, project):
        _write_relation(project, "local2", "people_csv", "csv")

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _SCHEMA_YML_CROSS_CATALOG,
            "from_local2.sql": """
{{ config(materialized='table') }}
SELECT * FROM {{ source('external', 'people_csv') }}
""",
        }

    def test_model_reads_source_from_secondary_catalog(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "from_local2", ["id", "name"], order_by="id"
        )
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
