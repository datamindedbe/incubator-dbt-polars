import json

import polars as pl
import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.util import get_connection, run_dbt

from dbt.adapters.polars.relation import PolarsRelation
from tests.conftest import PolarsTestMixin
from tests.utils import polars_read_relation

_SOURCE_MODEL = """
{{ config(materialized='table', file_format='parquet') }}

SELECT 1 AS id, 'hello' AS value
"""

_DOWNSTREAM_MODEL = """
{{ config(materialized='table') }}

SELECT * FROM {{ ref('source_model') }}
"""


class TestNodeConfigForNonExecutedModel(PolarsTestMixin):
    """Verify that _node_configs carries file_format not in the execution set.

    When a model references another model via ref(), the adapter must look up the
    referenced model's config (including file_format) from the manifest even if
    that model is not part of the current dbt run. Without this, the adapter would
    default to delta and fail to read a parquet file.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_model.sql": _SOURCE_MODEL,
            "downstream.sql": _DOWNSTREAM_MODEL,
        }

    def test_downstream_reads_non_executed_model_config(self, project):
        run_dbt(["run", "--select", "source_model"])

        # Only downstream is executed — source_model is not re-run but must be read
        run_dbt(["run", "--select", "downstream"])

        rows = polars_read_relation(project.adapter, "downstream", ["id", "value"])
        assert rows == [(1, "hello")]


_CROSS_FORMAT_CSV = """
{{ config(materialized='table', file_format='csv') }}
SELECT 1 AS id, 'alpha' AS label
UNION ALL
SELECT 2 AS id, 'beta' AS label
"""

_CROSS_FORMAT_DELTA = """
{{ config(materialized='table', file_format='delta') }}
SELECT * FROM {{ ref('csv_upstream') }}
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestCrossFormatRef(PolarsTestMixin):
    """A delta model can ref a CSV model — formats can be mixed freely."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "csv_upstream.sql": _CROSS_FORMAT_CSV,
            "delta_downstream.sql": _CROSS_FORMAT_DELTA,
        }

    def test_delta_reads_csv_upstream(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "delta_downstream", ["id", "label"], order_by="id"
        )
        assert rows == [(1, "alpha"), (2, "beta")]


_SEMICOLON_MODEL = """
{{ config(
    materialized='table',
    file_format='csv',
    write_options={'separator': ';'},
    read_options={'separator': ';'}
) }}
SELECT 1 AS id, 'hello' AS value
UNION ALL
SELECT 2 AS id, 'world' AS value
"""

_SEMICOLON_DOWNSTREAM = """
{{ config(materialized='table') }}
SELECT * FROM {{ ref('semicolon_csv') }}
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestCustomDelimiterCsv(PolarsTestMixin):
    """write_options writes a ';'-delimited CSV; read_options lets downstream ref it."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "semicolon_csv.sql": _SEMICOLON_MODEL,
            "downstream.sql": _SEMICOLON_DOWNSTREAM,
        }

    def test_custom_delimiter_write_and_read(self, project):
        run_dbt(["run"])

        # downstream reads semicolon_csv via ref — uses the model's read_options
        rows = polars_read_relation(
            project.adapter, "downstream", ["id", "value"], order_by="id"
        )
        assert rows == [(1, "hello"), (2, "world")]

        # verify the physical file actually uses ';' — scanning with ',' must fail
        # to produce the correct two-column schema
        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            relation = PolarsRelation.create(
                database=project.database,
                schema=project.test_schema,
                identifier="semicolon_csv",
                type=RelationType.Table,
                catalog=project.database,
                file_format="csv",
            )
            path = catalog._get_path(relation)

        raw = pl.read_csv(path, separator=",")
        assert raw.width == 1, (
            "file must use ';' not ',': comma scan should yield 1 column"
        )


_SOURCE_SCHEMA = """
version: 2
sources:
  - name: external_data
    schema: "{{ target.schema }}"
    tables:
      - name: raw_semicolon
        config:
          file_format: csv
          read_options:
            separator: ";"
"""

_SOURCE_READER = """
{{ config(materialized='table') }}
SELECT * FROM {{ source('external_data', 'raw_semicolon') }}
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestSourceReadOptions(PolarsTestMixin):
    """A pre-written ';'-delimited CSV is readable via a source with read_options."""

    @pytest.fixture(scope="class", autouse=True)
    def write_raw_file(self, project):
        relation = PolarsRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="raw_semicolon",
            type=RelationType.Table,
            catalog=project.database,
            file_format="csv",
        )
        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            catalog.create_schema(relation)
            catalog.write_relation(
                relation,
                pl.DataFrame({"id": [1, 2], "value": ["hello", "world"]}),
                [],
                {"write_options": {"separator": ";"}},
            )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _SOURCE_SCHEMA,
            "reader.sql": _SOURCE_READER,
        }

    def test_source_read_options(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter, "reader", ["id", "value"], order_by="id"
        )
        assert rows == [(1, "hello"), (2, "world")]


_MERGE_CSV_MODEL = """
{{ config(
    materialized='incremental',
    file_format='csv',
    unique_key='id',
    incremental_strategy='merge',
    write_options={'separator': ';'},
    read_options={'separator': ';'}
) }}
SELECT 1 AS id, 'hello' AS value
UNION ALL
SELECT 2 AS id, 'world' AS value
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestMergeDropsWriteOptions(PolarsTestMixin):
    """Regression: FileFormat.merge hardcodes model_config={} in its final write,
    so write_options (e.g. CSV separator) are silently dropped after a merge.
    The file is re-written with the default comma separator, but subsequent reads
    use read_options={'separator': ';'} and parse the file incorrectly."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_csv.sql": _MERGE_CSV_MODEL}

    def test_merge_preserves_write_options(self, project):
        run_dbt(["run"])  # initial full write — file uses ';'
        run_dbt(["run"])  # merge run — re-writes file, drops write_options (bug)

        rows = polars_read_relation(
            project.adapter, "merge_csv", ["id", "value"], order_by="id"
        )
        assert rows == [(1, "hello"), (2, "world")]


_DELETE_INSERT_CSV_MODEL = """
{{ config(
    materialized='incremental',
    file_format='csv',
    unique_key='id',
    incremental_strategy='delete+insert',
    write_options={'separator': ';'},
    read_options={'separator': ';'}
) }}
SELECT 1 AS id, 'hello' AS value
UNION ALL
SELECT 2 AS id, 'world' AS value
"""


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestDeleteInsertDropsWriteOptions(PolarsTestMixin):
    """Regression: FileFormat.delete_matched hardcodes model_config={} in its write,
    corrupting the file separator before append_relation reads it back."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"delete_insert_csv.sql": _DELETE_INSERT_CSV_MODEL}

    def test_delete_insert_preserves_write_options(self, project):
        run_dbt(["run"])  # initial full write — file uses ';'
        run_dbt(["run"])  # delete+insert — delete_matched writes with ',' (bug)

        rows = polars_read_relation(
            project.adapter, "delete_insert_csv", ["id", "value"], order_by="id"
        )
        assert rows == [(1, "hello"), (2, "world")]

        # Selecting columns by name masks a corrupted schema — also assert the file
        # has exactly the right columns (the bug produces a spurious 'id,value' column).
        relation = PolarsRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="delete_insert_csv",
            type=RelationType.Table,
            catalog=project.database,
            file_format="csv",
            read_options={"separator": ";"},
        )
        with get_connection(project.adapter):
            df = (
                project.adapter.get_storage_catalog(project.database)
                .get_relation(relation)
                .collect()
            )
        assert df.columns == ["id", "value"]


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestApplySnapshotDropsWriteOptions(PolarsTestMixin):
    """Regression: both branches of apply_snapshot_delta for file formats hardcode
    model_config={} in FileFormat.write, dropping write_options like CSV separator."""

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def _make_relation(self, project):
        return PolarsRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="snapshot_csv",
            type=RelationType.Table,
            catalog=project.database,
            file_format="csv",
            read_options={"separator": ";"},
        )

    def _model_config(self):
        return {"write_options": {"separator": ";"}}

    def test_apply_snapshot_preserves_write_options(self, project):
        relation = self._make_relation(project)
        initial = pl.DataFrame({"dbt_scd_id": [1, 2], "value": ["old1", "old2"]})
        rows_to_close = pl.DataFrame({"dbt_scd_id": [1], "value": ["updated1"]})
        rows_to_insert = pl.DataFrame({"dbt_scd_id": [3], "value": ["new3"]})

        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            catalog.create_schema(relation)
            catalog.write_relation(relation, initial, [], self._model_config())
            catalog.apply_snapshot_delta(
                relation,
                rows_to_close,
                rows_to_insert,
                model_config=self._model_config(),
            )

        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            df = catalog.get_relation(relation).collect()

        rows = sorted(df.select(["dbt_scd_id", "value"]).rows())
        assert rows == [(1, "updated1"), (2, "old2"), (3, "new3")]

    def test_apply_snapshot_append_only_preserves_write_options(self, project):
        relation = PolarsRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="snapshot_csv_append",
            type=RelationType.Table,
            catalog=project.database,
            file_format="csv",
            read_options={"separator": ";"},
        )
        initial = pl.DataFrame({"dbt_scd_id": [1], "value": ["existing"]})
        rows_to_insert = pl.DataFrame({"dbt_scd_id": [2], "value": ["new"]})

        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            catalog.create_schema(relation)
            catalog.write_relation(relation, initial, [], self._model_config())
            # rows_to_close is empty → takes the FileFormat.append branch
            catalog.apply_snapshot_delta(
                relation,
                pl.DataFrame(schema=initial.schema),
                rows_to_insert,
                model_config=self._model_config(),
            )

        with get_connection(project.adapter):
            catalog = project.adapter.get_storage_catalog(project.database)
            df = catalog.get_relation(relation).collect()

        rows = sorted(df.select(["dbt_scd_id", "value"]).rows())
        assert rows == [(1, "existing"), (2, "new")]


@pytest.mark.require_profiles("local")
@pytest.mark.require_configs("default")
class TestDocsGenerateDropsReadOptions(PolarsTestMixin):
    """Bug: list_relations_without_caching builds PolarsRelation with read_options={}.
    When docs generate then calls get_columns_in_relation on the discovered relation,
    it reads the CSV with the default comma separator instead of ';', producing
    one mangled column instead of two correct ones."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"semicolon_csv.sql": _SEMICOLON_MODEL}

    def test_docs_generate_discovers_correct_columns(self, project):
        run_dbt(["run"])
        run_dbt(["docs", "generate"])

        with open("target/catalog.json") as f:
            catalog = json.load(f)

        node = catalog["nodes"]["model.test.semicolon_csv"]
        columns = list(node["columns"].keys())
        assert columns == ["id", "value"]
