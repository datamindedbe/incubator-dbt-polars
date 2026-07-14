import threading
import time

import pytest
from dbt.tests.util import get_connection, run_dbt
from tests.conftest import PolarsTestMixin


class TestNoAtomicTableSwap(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    # @pytest.mark.xfail(
    #     strict=True,
    #     reason=(
    #         "dbt-polars drops then writes (no atomic swap); "
    #         "table is inaccessible during the write window"
    #     ),
    # )
    def test_table_always_accessible_during_refresh(self, project):
        run_dbt(["run"])  # initial creation

        adapter = project.adapter
        with get_connection(adapter):
            catalog = adapter.get_storage_catalog(project.database)
        relation = adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="my_model",
            catalog=project.database,
        )

        gap_start: list[float] = []
        gap_end: list[float] = []
        stop = threading.Event()

        def poll():
            while not stop.is_set():
                now = time.monotonic()
                if not catalog.table_exists(relation):
                    if not gap_start:
                        gap_start.append(now)
                    gap_end.clear()
                    gap_end.append(now)
                time.sleep(0.001)

        t = threading.Thread(target=poll, daemon=True)
        t.start()
        try:
            run_dbt(["run"])  # second run triggers the downtime window
        finally:
            stop.set()
            t.join()

        if gap_start:
            gap_ms = (gap_end[0] - gap_start[0]) * 1000
            pytest.fail(
                f"Table 'my_model' was inaccessible for ~{gap_ms:.1f} ms "
                "during the second run."
            )
