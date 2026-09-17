import threading
import time

import pytest
from dbt.tests.util import get_connection, run_dbt

from tests.conftest import PolarsTestMixin

# A single table_exists() check is discarded as inconclusive if it takes longer
# than this to return - under load, a slow response reflects scheduling/network
# contention, not necessarily the table's real state at that instant.
_MAX_RELIABLE_CHECK_SECONDS = 0.25
# Only fail once consecutive "missing" results span at least this long - filters
# single-sample noise while still catching a real, order-of-magnitude-larger gap.
_MIN_REPORTABLE_GAP_SECONDS = 0.05


@pytest.mark.require_configs("default")
class TestNoAtomicTableSwap(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

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
                start = time.monotonic()
                exists = catalog.table_exists(relation)
                elapsed = time.monotonic() - start
                if elapsed > _MAX_RELIABLE_CHECK_SECONDS:
                    # Too slow to trust as a snapshot of "the table's state right
                    # now" - discard, and don't let it bridge two unrelated gaps.
                    gap_start.clear()
                    gap_end.clear()
                elif exists:
                    gap_start.clear()
                    gap_end.clear()
                else:
                    if not gap_start:
                        gap_start.append(start)
                    gap_end.clear()
                    gap_end.append(start)
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
            if gap_ms >= _MIN_REPORTABLE_GAP_SECONDS * 1000:
                pytest.fail(
                    f"Table 'my_model' was inaccessible for ~{gap_ms:.1f} ms "
                    "during the second run."
                )
