import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.basic.files import (
    seeds_added_csv,
    seeds_base_csv,
    seeds_newcolumns_csv,
    ts_snapshot_sql,
)
from tests.utils import polars_relation_row_count, polars_update_rows


class BaseSnapshotTimestamp:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "newcolumns.csv": seeds_newcolumns_csv,
            "added.csv": seeds_added_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "ts_snapshot.sql": ts_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_strategy_timestamp"}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_snapshot_timestamp(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 3

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # snapshot has 10 rows
        assert polars_relation_row_count(project.adapter, "ts_snapshot") == 10

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])

        # snapshot now has 20 rows
        assert polars_relation_row_count(project.adapter, "ts_snapshot") == 20

        # update some timestamps in the "added" seed
        # so the snapshot sees 10 more new rows
        update_rows_config = {
            "name": "added",
            "dst_col": "some_date",
            "clause": {
                "src_col": "some_date",
                "type": "add_timestamp",
            },
            "where": "id > 10 and id < 21",
        }
        polars_update_rows(project.adapter, update_rows_config)

        results = run_dbt(["snapshot", "--vars", "seed_name: added"])

        # snapshot now has 30 rows
        assert polars_relation_row_count(project.adapter, "ts_snapshot") == 30

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

        results = run_dbt(["snapshot", "--vars", "seed_name: added"])

        # snapshot still has 30 rows because timestamp not updated
        assert polars_relation_row_count(project.adapter, "ts_snapshot") == 30


class TestSnapshotTimestamp(BaseSnapshotTimestamp):
    pass
