import polars as pl
import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_quoted_reserved_word_column_names import (
    BaseUnitTestQuotedReservedWordColumnNames,
)
from dbt.tests.adapter.unit_testing.test_types import (
    BaseUnitTestingTypes,
    BaseUnitTestingVarcharFixtureNoTruncation,
)
from dbt.tests.util import get_connection, run_dbt, run_dbt_and_capture

from dbt.adapters.polars.relation import PolarsRelation
from dbt.adapters.polars.testing.mixin import PolarsTestMixin


class TestUnitTestingTypes(BaseUnitTestingTypes, PolarsTestMixin):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["1.5", "1.5"],
            ["cast(7.77 as decimal(10, 2))", "7.77"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00'", "2013-11-03 00:00:00"],
        ]


class TestUnitTestingVarcharFixtureNoTruncation(
    BaseUnitTestingVarcharFixtureNoTruncation, PolarsTestMixin
):
    pass


class TestUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity, PolarsTestMixin):
    pass


class TestUnitTestInvalidInput(BaseUnitTestInvalidInput, PolarsTestMixin):
    pass


class TestUnitTestQuotedReservedWordColumnNames(
    BaseUnitTestQuotedReservedWordColumnNames, PolarsTestMixin
):
    pass


upstream_sql = """
select 1 as id, 1.5 as amount
"""

# Time zones and lists have no Polars SQL cast, the fixture must still build
upstream_python = """
from datetime import datetime, timezone

import polars as pl


def model(dbt, session):
    return pl.DataFrame(
        {
            "id": [1],
            "event_at": [datetime(2024, 1, 1, 10, tzinfo=timezone.utc)],
            "tags": [["a", "b"]],
        }
    )
"""

joined_sql = """
select s.id, s.amount * 2 as double_amount, p.event_at
from {{ ref('upstream_sql') }} s
join {{ ref('upstream_python') }} p on s.id = p.id
"""

doubled_sql = """
select id, amount * 2 as double_amount from {{ ref('upstream_sql') }}
"""

unit_tests_yml = """
unit_tests:
  - name: test_joined
    model: joined
    given:
      - input: ref('upstream_sql')
        rows:
          - {id: 1, amount: 2}
          - {id: 2, amount: 3.5}
      - input: ref('upstream_python')
        rows:
          - {id: 1, event_at: "2024-01-01 10:00:00+00:00"}
          - {id: 2}
    expect:
      rows:
        - {id: 1, double_amount: 4, event_at: "2024-01-01 10:00:00+00:00"}
        - {id: 2, double_amount: 7}

  - name: test_joined_wrong_expectation
    model: joined
    given:
      - input: ref('upstream_sql')
        rows:
          - {id: 1, amount: 2}
      - input: ref('upstream_python')
        rows:
          - {id: 1}
    expect:
      rows:
        - {id: 1, double_amount: 5}

  - name: test_empty_rows
    model: doubled
    given:
      - input: ref('upstream_sql')
        rows: []
    expect:
      rows: []

  - name: test_sql_format
    model: doubled
    given:
      - input: ref('upstream_sql')
        format: sql
        rows: |
          select 1 as id, 1.5 as amount
    expect:
      format: sql
      rows: |
        select 1 as id, 3.0 as double_amount

  - name: test_csv_format
    model: doubled
    given:
      - input: ref('upstream_sql')
        format: csv
        rows: |
          id,amount
          1,1.5
    expect:
      format: csv
      rows: |
        id,double_amount
        1,3
"""


@pytest.mark.skip_configs("csv")  # CSV can't store the list column of upstream_python
class TestUnitTestFixtures(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_sql.sql": upstream_sql,
            "upstream_python.py": upstream_python,
            "joined.sql": joined_sql,
            "doubled.sql": doubled_sql,
            "unit_tests.yml": unit_tests_yml,
        }

    def test_unit_test_fixtures(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4

        for test_name in (
            "test_joined",
            "test_empty_rows",
            "test_sql_format",
            "test_csv_format",
        ):
            results = run_dbt(["test", "--select", f"test_name:{test_name}"])
            assert len(results) == 1

        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_joined_wrong_expectation"],
            expect_pass=False,
        )
        assert "differs from" in out


sources_yml = """
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: people

unit_tests:
  - name: test_from_source
    model: from_source
    given:
      - input: source('raw', 'people')
        rows:
          - {id: 1, name: alice}
    expect:
      rows:
        - {id: 1, name: ALICE}

  - name: test_python_model_from_source
    model: python_from_source
    given:
      - input: source('raw', 'people')
        rows:
          - {id: 1}
    expect:
      rows:
        - {id: 1}

  - name: test_python_model_from_ref
    model: python_from_ref
    given:
      - input: ref('from_source')
        rows:
          - {id: 1, name: alice}
          - {id: 2, name: bob}
    expect:
      rows:
        - {id: 2, name: alice}
        - {id: 4, name: bob}

  - name: test_python_model_incremental
    model: python_incremental
    overrides:
      macros:
        is_incremental: true
    given:
      - input: ref('from_source')
        rows:
          - {id: 1}
          - {id: 2}
    expect:
      rows:
        - {id: 2}

models:
  - name: python_from_ref
    config:
      meta:
        multiplier: 2
"""

from_source_sql = """
select id, upper(name) as name from {{ source('raw', 'people') }}
"""

python_from_source = """
def model(dbt, session):
    return dbt.source("raw", "people")
"""

python_from_ref = """
import polars as pl


def model(dbt, session):
    multiplier = dbt.config.get("meta")["multiplier"]
    return dbt.ref("from_source").select(pl.col("id") * multiplier, "name")
"""

python_incremental = """
import polars as pl


def model(dbt, session):
    dbt.config(materialized="incremental")
    people = dbt.ref("from_source").select("id")
    if dbt.is_incremental:
        return people.filter(pl.col("id") > 1)
    return people
"""


class TestUnitTestSourcesAndPythonModels(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "from_source.sql": from_source_sql,
            "python_from_source.py": python_from_source,
            "python_from_ref.py": python_from_ref,
            "python_incremental.py": python_incremental,
            "schema.yml": sources_yml,
        }

    @pytest.fixture(scope="class", autouse=True)
    def write_source(self, project):
        relation = PolarsRelation.create(
            database="primary",
            schema=project.test_schema,
            identifier="people",
            type=RelationType.Table,
            catalog="primary",
        )
        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog("primary")
            catalog.create_schema(relation)
            catalog.write_relation(
                relation, pl.DataFrame({"id": [1], "name": ["a"]}), [], {}
            )

    def test_source_input(self, project):
        run_dbt(["test", "--select", "test_name:test_from_source"])

    def test_python_model_from_source(self, project):
        run_dbt(["test", "--select", "test_name:test_python_model_from_source"])

    def test_python_model_from_ref(self, project):
        run_dbt(["run", "--select", "from_source"])
        run_dbt(["test", "--select", "test_name:test_python_model_from_ref"])

    def test_python_model_incremental(self, project):
        run_dbt(["run", "--select", "from_source"])
        run_dbt(["test", "--select", "test_name:test_python_model_incremental"])


ephemeral_sql = """
{{ config(materialized='ephemeral') }}
select 1 as id
"""

ephemeral_python = """
import polars as pl


def model(dbt, session):
    dbt.config(materialized="ephemeral")
    return pl.DataFrame({"id": [1]})
"""

sql_on_ephemeral_sql = "select id * 10 as id from {{ ref('ephemeral_sql') }}"
sql_on_ephemeral_python = "select id * 10 as id from {{ ref('ephemeral_python') }}"

python_on_ephemeral_sql = """
import polars as pl


def model(dbt, session):
    return dbt.ref("ephemeral_sql").select(pl.col("id") * 10)
"""

python_on_ephemeral_python = """
import polars as pl


def model(dbt, session):
    return dbt.ref("ephemeral_python").select(pl.col("id") * 10)
"""


def ephemeral_unit_test(model: str, parent: str) -> str:
    return f"""
  - name: test_{model}
    model: {model}
    given:
      - input: ref('{parent}')
        format: sql
        rows: |
          select 2 as id
    expect:
      rows:
        - {{id: 20}}
"""


ephemeral_unit_tests_yml = (
    "unit_tests:"
    + ephemeral_unit_test("sql_on_ephemeral_sql", "ephemeral_sql")
    + ephemeral_unit_test("sql_on_ephemeral_python", "ephemeral_python")
    + ephemeral_unit_test("python_on_ephemeral_sql", "ephemeral_sql")
    + ephemeral_unit_test("python_on_ephemeral_python", "ephemeral_python")
    + """
  - name: test_ephemeral_input_as_rows
    model: sql_on_ephemeral_sql
    given:
      - input: ref('ephemeral_sql')
        rows:
          - {id: 2}
    expect:
      rows:
        - {id: 20}
"""
)


class TestUnitTestEphemeralInputs(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_sql.sql": ephemeral_sql,
            "ephemeral_python.py": ephemeral_python,
            "sql_on_ephemeral_sql.sql": sql_on_ephemeral_sql,
            "sql_on_ephemeral_python.sql": sql_on_ephemeral_python,
            "python_on_ephemeral_sql.py": python_on_ephemeral_sql,
            "python_on_ephemeral_python.py": python_on_ephemeral_python,
            "unit_tests.yml": ephemeral_unit_tests_yml,
        }

    @pytest.mark.parametrize(
        "model",
        [
            "sql_on_ephemeral_sql",
            "sql_on_ephemeral_python",
            "python_on_ephemeral_sql",
            "python_on_ephemeral_python",
        ],
    )
    def test_ephemeral_input_as_sql(self, project, model):
        run_dbt(["test", "--select", f"test_name:test_{model}"])

    def test_ephemeral_input_as_rows(self, project):
        _, out = run_dbt_and_capture(
            ["test", "--select", "test_name:test_ephemeral_input_as_rows"],
            expect_pass=False,
        )
        # Same as dbt-core: an ephemeral input has no table to read column types from
        assert "because the relation doesn't exist" in out
