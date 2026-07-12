import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.basic.files import (
    cc_all_snapshot_sql,
    cc_date_snapshot_sql,
    cc_name_snapshot_sql,
    seeds_added_csv,
    seeds_base_csv,
)
from tests.utils import polars_relation_row_count, polars_update_rows


class BaseSnapshotCheckCols:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_strategy_check_cols"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "cc_all_snapshot.sql": cc_all_snapshot_sql,
            "cc_date_snapshot.sql": cc_date_snapshot_sql,
            "cc_name_snapshot.sql": cc_name_snapshot_sql,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_snapshot_check_cols(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # snapshot command
        results = run_dbt(["snapshot"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        assert polars_relation_row_count(project.adapter, "cc_all_snapshot") == 10
        assert polars_relation_row_count(project.adapter, "cc_name_snapshot") == 10
        assert polars_relation_row_count(project.adapter, "cc_date_snapshot") == 10

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(
            ["--no-partial-parse", "snapshot", "--vars", "seed_name: added"]
        )
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        assert polars_relation_row_count(project.adapter, "cc_all_snapshot") == 20
        assert polars_relation_row_count(project.adapter, "cc_name_snapshot") == 20
        assert polars_relation_row_count(project.adapter, "cc_date_snapshot") == 20

        # update some timestamps in the "added" seed so
        # the snapshot sees 10 more new rows
        update_rows_config = {
            "name": "added",
            "dst_col": "some_date",
            "clause": {"src_col": "some_date", "type": "add_timestamp"},
            "where": "id > 10 and id < 21",
        }
        polars_update_rows(project.adapter, update_rows_config)

        # re-run snapshots, using "added'
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        assert polars_relation_row_count(project.adapter, "cc_all_snapshot") == 30
        assert polars_relation_row_count(project.adapter, "cc_date_snapshot") == 30
        # unchanged: only the timestamp changed
        assert polars_relation_row_count(project.adapter, "cc_name_snapshot") == 20

        # Update the name column
        update_rows_config = {
            "name": "added",
            "dst_col": "name",
            "clause": {
                "src_col": "name",
                "type": "add_string",
                "value": "_updated",
            },
            "where": "id < 11",
        }
        polars_update_rows(project.adapter, update_rows_config)

        # re-run snapshots, using "added'
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        assert polars_relation_row_count(project.adapter, "cc_all_snapshot") == 40
        assert polars_relation_row_count(project.adapter, "cc_name_snapshot") == 30
        # does not see name updates
        assert polars_relation_row_count(project.adapter, "cc_date_snapshot") == 30


class TestSnapshotCheckCols(BaseSnapshotCheckCols):
    pass
