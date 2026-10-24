import threading
import traceback

import pytest
from dbt.tests.util import run_dbt

import polars as pl
from dbt.adapters.polars.compute_slots import ComputeSlots, compute_slots
from dbt.adapters.polars.testing.mixin import PolarsTestMixin

seeds__seed_csv = """id,name
1,Easton
2,Lillian
3,Jeremiah
""".lstrip()

models__table_sql = """
{{ config(materialized="table") }}
select * from {{ ref("seed") }}
"""

models__incremental_merge_sql = """
{{ config(materialized="incremental", unique_key="id") }}
select * from {{ ref("seed") }}
"""

models__incremental_delete_insert_sql = """
{{ config(
    materialized="incremental", unique_key="id", incremental_strategy="delete+insert"
) }}
select * from {{ ref("seed") }}
"""

models__lazy_python = """
import polars as pl


def model(dbt, session):
    return dbt.ref("seed").with_columns(doubled=pl.col("id") * 2)
"""

models__eager_python = """
def model(dbt, session):
    return dbt.ref("seed").collect()
"""

models__schema_yml = """
version: 2
models:
  - name: table_model
    columns:
      - name: id
        data_tests:
          - not_null
"""

snapshots__seed_snapshot_sql = """
{% snapshot seed_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref("seed") }}
{% endsnapshot %}
"""


class SlotUsage:
    """Records how many models held a compute slot at once, and Polars work
    done in a dbt thread without one."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.holders = 0
        self.max_holders = 0
        self.unslotted: list[str] = []


class CountingSemaphore:
    def __init__(self, limit: int, usage: SlotUsage) -> None:
        self.semaphore = threading.Semaphore(limit)
        self.usage = usage

    def acquire(self, blocking: bool = True) -> bool:
        acquired = self.semaphore.acquire(blocking)
        if acquired:
            with self.usage.lock:
                self.usage.holders += 1
                self.usage.max_holders = max(self.usage.max_holders, self.usage.holders)
        return acquired

    def release(self) -> None:
        with self.usage.lock:
            self.usage.holders -= 1
        self.semaphore.release()


class BaseComputeThreads(PolarsTestMixin):
    """With compute_threads: 1, models run in parallel threads but Polars only
    computes in a thread that holds the compute slot."""

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        target = {**dbt_profile_target, "threads": 4, "compute_threads": 1}
        return {"test": {"outputs": {"default": target}, "target": "default"}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": models__table_sql,
            "incremental_merge.sql": models__incremental_merge_sql,
            "incremental_delete_insert.sql": models__incremental_delete_insert_sql,
            "lazy_python.py": models__lazy_python,
            "eager_python.py": models__eager_python,
            "schema.yml": models__schema_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"seed_snapshot.sql": snapshots__seed_snapshot_sql}

    @pytest.fixture
    def slot_usage(self, monkeypatch) -> SlotUsage:
        usage = SlotUsage()
        configure = ComputeSlots.configure

        def configure_counting(slots: ComputeSlots, limit: int | None) -> None:
            configure(slots, limit)
            if limit:
                slots.semaphore = CountingSemaphore(limit, usage)  # type: ignore[assignment]

        def recorded(method):
            def wrapper(*args, **kwargs):
                thread = threading.current_thread()
                if thread is not threading.main_thread() and not (
                    compute_slots.is_holding()
                ):
                    stack = "".join(traceback.format_stack(limit=12))
                    usage.unslotted.append(f"{thread.name} {method.__name__}\n{stack}")
                return method(*args, **kwargs)

            return wrapper

        monkeypatch.setattr(ComputeSlots, "configure", configure_counting)
        for name in ("collect", "collect_batches"):
            monkeypatch.setattr(
                pl.LazyFrame, name, recorded(getattr(pl.LazyFrame, name))
            )
        return usage

    def test_polars_only_computes_in_a_thread_with_the_slot(self, project, slot_usage):
        run_dbt(["seed"])
        for _ in range(2):
            run_dbt(["run"])
            run_dbt(["snapshot"])
        run_dbt(["test"])

        assert slot_usage.max_holders == 1
        assert slot_usage.unslotted == [], slot_usage.unslotted[0]
