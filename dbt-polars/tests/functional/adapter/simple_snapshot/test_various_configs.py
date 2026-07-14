import datetime

import polars as pl
import pytest
from dbt.tests.util import (
    get_connection,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    update_config_file,
)

from tests.functional.adapter.simple_snapshot import common, seeds

MODEL_FACT_SQL = """
{{ config(materialized="table") }}
select * from {{ ref('seed') }}
where id between 1 and 20
"""

META_COL_NAMES = {
    "dbt_valid_to": "test_valid_to",
    "dbt_valid_from": "test_valid_from",
    "dbt_scd_id": "test_scd_id",
    "dbt_updated_at": "test_updated_at",
}

SNAPSHOT_CUSTOM_COLS_SQL = """
{% snapshot snapshot %}
    {{ config(
        strategy='timestamp',
        updated_at='updated_at',
        unique_key='id',
        snapshot_meta_column_names={
            'dbt_valid_to': 'test_valid_to',
            'dbt_valid_from': 'test_valid_from',
            'dbt_scd_id': 'test_scd_id',
            'dbt_updated_at': 'test_updated_at',
        }
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

SNAPSHOT_NO_CUSTOM_COLS_SQL = """
{% snapshot snapshot %}
    {{ config(
        strategy='timestamp',
        updated_at='updated_at',
        unique_key='id',
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

SNAPSHOT_MULTI_KEY_SQL = """
{% snapshot snapshot %}
    {{ config(
        strategy='timestamp',
        updated_at='updated_at',
        unique_key=['id', 'first_name'],
        snapshot_meta_column_names={
            'dbt_valid_to': 'test_valid_to',
            'dbt_valid_from': 'test_valid_from',
            'dbt_scd_id': 'test_scd_id',
            'dbt_updated_at': 'test_updated_at',
        }
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

SNAPSHOT_VALID_TO_CURRENT_SQL = """
{% snapshot snapshot %}
    {{ config(
        strategy='timestamp',
        updated_at='updated_at',
        unique_key='id',
        dbt_valid_to_current="date('2099-12-31')",
        snapshot_meta_column_names={
            'dbt_valid_to': 'test_valid_to',
            'dbt_valid_from': 'test_valid_from',
            'dbt_scd_id': 'test_scd_id',
            'dbt_updated_at': 'test_updated_at',
        }
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""

SNAPSHOT_IS_DELETED_COL_SQL = """
{% snapshot snapshot %}
    {{ config(
        strategy='timestamp',
        updated_at='updated_at',
        unique_key='id',
        hard_deletes='new_record',
        snapshot_meta_column_names={
            'dbt_is_deleted': 'is_row_deleted',
        }
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""


def _get_snapshot_df(project) -> pl.DataFrame:
    with get_connection(project.adapter):
        relation = relation_from_name(project.adapter, "snapshot")
        return (
            project.adapter.get_storage_catalog(relation.database)
            .get_relation(relation)
            .collect()
        )


class BaseSnapshotWithCustomCols:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds.SEED_CSV}

    @pytest.fixture(scope="class")
    def models(self):
        return {"fact.sql": MODEL_FACT_SQL}

    @pytest.fixture(scope="class", autouse=True)
    def _setup_class(self, project):
        run_dbt(["seed"])

    @pytest.fixture(scope="function", autouse=True)
    def _setup_method(self, project):
        self.project = project
        common.clone_table(project, "fact", "seed", "*", "id between 1 and 20")
        run_dbt(["snapshot"])
        yield
        common.delete_records(project, "snapshot")
        common.delete_records(project, "fact")


class TestSnapshotColumnNames(BaseSnapshotWithCustomCols):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_CUSTOM_COLS_SQL}

    def test_meta_column_names_used(self, project):
        df = _get_snapshot_df(project)
        for internal, external in META_COL_NAMES.items():
            assert external in df.columns, f"expected column {external!r}"
            assert internal not in df.columns, f"column {internal!r} should be renamed"

    def test_updates_captured_with_custom_cols(self, project):
        common.update_records(
            project,
            "fact",
            {"updated_at": "updated_at + interval '1 day'"},
            "id between 16 and 20",
        )
        run_dbt(["snapshot"])
        df = _get_snapshot_df(project)
        for internal, external in META_COL_NAMES.items():
            assert external in df.columns
            assert internal not in df.columns
        assert df.filter(pl.col("test_valid_to").is_null()).shape[0] == 20
        assert df.filter(pl.col("test_valid_to").is_not_null()).shape[0] == 5


class TestSnapshotColumnNamesFromDbtProject(BaseSnapshotWithCustomCols):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_NO_CUSTOM_COLS_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"snapshots": {"test": {"+snapshot_meta_column_names": META_COL_NAMES}}}

    def test_meta_column_names_from_project(self, project):
        df = _get_snapshot_df(project)
        for internal, external in META_COL_NAMES.items():
            assert external in df.columns, f"expected column {external!r}"
            assert internal not in df.columns, f"column {internal!r} should be renamed"


class TestSnapshotInvalidColumnNames(BaseSnapshotWithCustomCols):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_NO_CUSTOM_COLS_SQL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"snapshots": {"test": {"+snapshot_meta_column_names": META_COL_NAMES}}}

    def test_mismatched_column_names_raise_error(self, project):
        # Table has test_valid_to, test_valid_from, test_scd_id, test_updated_at.
        # Switch to a partial mapping — dbt_valid_from and dbt_scd_id become unremapped
        # (expected as "dbt_valid_from"/"dbt_scd_id") but table still has "test_*".
        update_config_file(
            {
                "snapshots": {
                    "test": {
                        "+snapshot_meta_column_names": {
                            "dbt_valid_to": "test_valid_to",
                            "dbt_updated_at": "test_updated_at",
                        }
                    }
                }
            },
            "dbt_project.yml",
        )
        results, log_output = run_dbt_and_capture(["snapshot"], expect_pass=False)
        assert len(results) == 1
        assert "Snapshot target is missing configured columns" in log_output


class TestSnapshotDbtValidToCurrent(BaseSnapshotWithCustomCols):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_VALID_TO_CURRENT_SQL}

    def test_open_rows_use_sentinel(self, project):
        sentinel = datetime.datetime(2099, 12, 31, 0, 0, 0)
        df = _get_snapshot_df(project)
        for internal, external in META_COL_NAMES.items():
            assert external in df.columns
            assert internal not in df.columns
        assert df.filter(pl.col("test_valid_to") == sentinel).shape[0] == 20
        assert df.filter(pl.col("test_valid_to").is_null()).is_empty()

    def test_closed_rows_have_real_timestamp(self, project):
        sentinel = datetime.datetime(2099, 12, 31, 0, 0, 0)
        common.update_records(
            project,
            "fact",
            {"updated_at": "updated_at + interval '1 day'"},
            "id between 16 and 20",
        )
        run_dbt(["snapshot"])
        df = _get_snapshot_df(project)
        assert df.filter(pl.col("test_valid_to") == sentinel).shape[0] == 20
        not_sentinel = pl.col("test_valid_to") != sentinel
        closed = df.filter(pl.col("test_valid_to").is_not_null() & not_sentinel)
        assert closed.shape[0] == 5


class TestSnapshotMultiUniqueKey(BaseSnapshotWithCustomCols):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_MULTI_KEY_SQL}

    def test_multi_key_initial_snapshot(self, project):
        df = _get_snapshot_df(project)
        for internal, external in META_COL_NAMES.items():
            assert external in df.columns
            assert internal not in df.columns
        assert df.filter(pl.col("test_valid_to").is_null()).shape[0] == 20

    def test_multi_key_updates_captured(self, project):
        common.update_records(
            project,
            "fact",
            {"updated_at": "updated_at + interval '1 day'"},
            "id between 16 and 20",
        )
        run_dbt(["snapshot"])
        df = _get_snapshot_df(project)
        assert df.filter(pl.col("test_valid_to").is_null()).shape[0] == 20
        assert df.filter(pl.col("test_valid_to").is_not_null()).shape[0] == 5


class TestSnapshotIsDeletedColumnName(BaseSnapshotWithCustomCols):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": SNAPSHOT_IS_DELETED_COL_SQL}

    def test_is_deleted_column_uses_configured_name(self, project):
        df = _get_snapshot_df(project)
        assert "is_row_deleted" in df.columns
        assert "dbt_is_deleted" not in df.columns
        assert df.filter(pl.col("is_row_deleted")).is_empty()

    def test_deleted_markers_use_configured_name(self, project):
        common.delete_records(project, "fact", "id between 16 and 20")
        run_dbt(["snapshot"])
        df = _get_snapshot_df(project)
        assert "is_row_deleted" in df.columns
        assert "dbt_is_deleted" not in df.columns
        markers = df.filter(pl.col("is_row_deleted"))
        assert markers.shape[0] == 5
