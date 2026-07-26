import tempfile
import threading
import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from dbt.tests.util import run_dbt
from deltalake import write_deltalake

from tests.conftest import PolarsTestMixin
from tests.utils import polars_relation_row_count

# A single 5M-row Parquet file (40MB) is scanned 50× by the model, producing
# a 250M-row (2GB) lazy plan.
#
# Peak memory delta above baseline (measured empirically on a 16-CPU machine):
#   sink_delta  (streaming):  ~850 MB  — batch-size × Rayon threads, bounded
#   collect + write_delta:    ~2+ GB   — 2 GB DataFrame always resident
#
# 1.5 GB threshold sits between them.
_SOURCE_ROWS = 5_000_000
_SOURCE_COPIES = 50
_OOM_ROWS = _SOURCE_ROWS * _SOURCE_COPIES  # 250 M rows = 2 GB int64
_MEMORY_THRESHOLD_BYTES = int(1.5 * 1024**3)  # 1.5 GB


_large_python_model_template = """\
import polars as pl

def model(dbt, _):
    dbt.config(materialized='table')
    return (
        pl.concat([pl.scan_parquet("{source_path}") for _ in range({copies})])
        .with_columns(((pl.col("id") - pl.col("id").mean()) /
        pl.col("id").std()).alias("id"))
    )
"""

_large_python_model_eager_template = """\
import polars as pl

def model(dbt, _):
    dbt.config(materialized='table', write_mode='eager')
    return (
        pl.concat([pl.scan_parquet("{source_path}") for _ in range({copies})])
        .with_columns(((pl.col("id") - pl.col("id").mean()) /
        pl.col("id").std()).alias("id"))
    )
"""


def _proc_memory() -> int:
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise OSError("VmRSS not found in /proc/self/status")


def _measure_peak_memory_delta(project) -> int:
    baseline = _proc_memory()
    peak = [baseline]
    stop = [False]

    def _monitor():
        while not stop[0]:
            try:
                m = _proc_memory()
                if m > peak[0]:
                    peak[0] = m
            except OSError:
                break
            time.sleep(0.05)

    monitor_thread = threading.Thread(target=_monitor, daemon=True)
    monitor_thread.start()
    try:
        run_dbt(["run"])
    finally:
        stop[0] = True
        monitor_thread.join()
    return peak[0] - baseline


def _assert_streaming_memory(project):
    if not Path("/proc/self/status").exists():
        pytest.skip("Memory monitoring requires /proc/self/status (Linux only)")
    memory_delta = _measure_peak_memory_delta(project)
    assert memory_delta < _MEMORY_THRESHOLD_BYTES, (
        f"Peak memory grew by {memory_delta // (1024**2)} MB above baseline, "
        f"exceeding the {_MEMORY_THRESHOLD_BYTES // (1024**2)} MB threshold."
    )
    assert polars_relation_row_count(project.adapter, "large_model") == _OOM_ROWS


def _assert_eager_oom(project):
    if not Path("/proc/self/status").exists():
        pytest.skip("Memory monitoring requires /proc/self/status (Linux only)")
    memory_delta = _measure_peak_memory_delta(project)
    assert memory_delta >= _MEMORY_THRESHOLD_BYTES, (
        f"Expected eager write to exceed {_MEMORY_THRESHOLD_BYTES // (1024**2)} MB "
        f"but peak was only {memory_delta // (1024**2)} MB above baseline."
    )


@pytest.mark.require_configs("default")
@pytest.mark.skip_profiles("iceberg", "iceberg-databricks")
class TestPythonModelOOM(PolarsTestMixin):
    """
    Verify sink_delta (LocalCatalog) streams 250M rows without loading all 2 GB
    into memory at once.

    Peak memory delta above pre-run baseline (16-CPU machine):
      sink_delta  (streaming):  ~850 MB  — batch-size x Rayon threads, bounded
      collect + write_delta:    ~2+ GB   — 2 GB DataFrame always resident

    The 1.5 GB threshold sits between them.
    """

    @pytest.fixture(scope="class")
    def large_source_path(self, tmp_path_factory):
        path = tmp_path_factory.mktemp("large_source") / "data.parquet"
        pl.DataFrame({"id": pl.arange(0, _SOURCE_ROWS, eager=True)}).write_parquet(
            str(path)
        )
        return str(path)

    @pytest.fixture(scope="class")
    def models(self, large_source_path):
        return {
            "large_model.py": _large_python_model_template.format(
                source_path=large_source_path,
                copies=_SOURCE_COPIES,
            )
        }

    def test_large_dataset_fits_in_bounded_memory(self, project):
        if not Path("/proc/self/status").exists():
            pytest.skip("Memory monitoring requires /proc/self/status (Linux only)")

        # Warm up the delta-rs Rayon thread pool before measuring baseline memory.
        # The first write_deltalake call starts its thread pool; without this
        # the thread-stack memory would be charged against the budget we measure.
        with tempfile.TemporaryDirectory() as warm_dir:
            write_deltalake(warm_dir, pa.table({"x": pa.array([1])}), mode="overwrite")

        _assert_streaming_memory(project)


@pytest.mark.require_profiles("iceberg", "iceberg-databricks")
class TestIcebergPythonModelOOM(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def large_source_path(self, tmp_path_factory):
        path = tmp_path_factory.mktemp("large_source_iceberg") / "data.parquet"
        pl.DataFrame({"id": pl.arange(0, _SOURCE_ROWS, eager=True)}).write_parquet(
            str(path)
        )
        return str(path)

    @pytest.fixture(scope="class")
    def models(self, large_source_path):
        return {
            "large_model.py": _large_python_model_template.format(
                source_path=large_source_path,
                copies=_SOURCE_COPIES,
            )
        }

    def test_large_dataset_fits_in_bounded_memory(self, project):
        _assert_streaming_memory(project)


@pytest.mark.require_configs("default")
@pytest.mark.skip_profiles("iceberg", "iceberg-databricks")
class TestPythonModelEagerOOM(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def large_source_path(self, tmp_path_factory):
        path = tmp_path_factory.mktemp("large_source_eager") / "data.parquet"
        pl.DataFrame({"id": pl.arange(0, _SOURCE_ROWS, eager=True)}).write_parquet(
            str(path)
        )
        return str(path)

    @pytest.fixture(scope="class")
    def models(self, large_source_path):
        return {
            "large_model.py": _large_python_model_eager_template.format(
                source_path=large_source_path,
                copies=_SOURCE_COPIES,
            )
        }

    def test_eager_write_exceeds_memory_threshold(self, project):
        _assert_eager_oom(project)


@pytest.mark.require_profiles("iceberg")
class TestIcebergPythonModelEagerOOM(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def large_source_path(self, tmp_path_factory):
        path = tmp_path_factory.mktemp("large_source_iceberg_eager") / "data.parquet"
        pl.DataFrame({"id": pl.arange(0, _SOURCE_ROWS, eager=True)}).write_parquet(
            str(path)
        )
        return str(path)

    @pytest.fixture(scope="class")
    def models(self, large_source_path):
        return {
            "large_model.py": _large_python_model_eager_template.format(
                source_path=large_source_path,
                copies=_SOURCE_COPIES,
            )
        }

    def test_eager_write_exceeds_memory_threshold(self, project):
        _assert_eager_oom(project)
