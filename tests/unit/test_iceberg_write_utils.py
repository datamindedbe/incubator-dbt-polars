import polars as pl
import pytest

from dbt.adapters.polars.catalogs.iceberg_write_utils import (
    _append_in_single_snapshot,
    _iter_arrow_batches,
    _overwrite_scd_ids_and_append,
    _replace_in_single_snapshot,
    _sync_partition_spec,
    _sync_schema,
)

# ---------------------------------------------------------------------------
# Fixture: a real pyiceberg table backed by SQLite + local filesystem.
# Tests that use this fixture are skipped when pyiceberg is not installed.
# ---------------------------------------------------------------------------


@pytest.fixture
def iceberg_table(tmp_path):
    pyiceberg = pytest.importorskip("pyiceberg")  # noqa: F841
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "test",
        **{
            "type": "sql",
            "uri": f"sqlite:///{tmp_path}/test.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )
    catalog.create_namespace(("ns",))
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("val", pa.string())])
    return catalog.create_table("ns.t", schema=schema)


@pytest.fixture
def scd_table(tmp_path):
    """Table with a dbt_scd_id column, pre-loaded with three rows."""
    pytest.importorskip("pyiceberg")
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "test",
        **{
            "type": "sql",
            "uri": f"sqlite:///{tmp_path}/scd.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )
    catalog.create_namespace(("ns",))
    schema = pa.schema(
        [pa.field("dbt_scd_id", pa.string()), pa.field("val", pa.string())]
    )
    tbl = catalog.create_table("ns.scd", schema=schema)
    tbl.append(
        pa.table({"dbt_scd_id": ["id-1", "id-2", "id-3"], "val": ["a", "b", "c"]})
    )
    return tbl


# ---------------------------------------------------------------------------
# _iter_arrow_batches — pure Polars, no pyiceberg required
# ---------------------------------------------------------------------------


def test_dataframe_yields_single_batch():
    df = pl.DataFrame({"x": [1, 2, 3]})
    batches = list(_iter_arrow_batches(df, {}))
    assert len(batches) == 1
    assert batches[0].num_rows == 3


def test_empty_dataframe_yields_one_empty_batch():
    df = pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)})
    batches = list(_iter_arrow_batches(df, {}))
    assert len(batches) == 1
    assert batches[0].num_rows == 0


def test_lazyframe_default_mode_streams_in_batches():
    # With a small batch_size, a LazyFrame is yielded as multiple Arrow batches
    # instead of one large block — keeps peak memory bounded for large datasets.
    lf = pl.LazyFrame({"x": list(range(10))})
    config = {"write_options": {"batch_size": 3}}
    batches = list(_iter_arrow_batches(lf, config))
    assert len(batches) > 1
    assert sum(b.num_rows for b in batches) == 10


def test_lazyframe_eager_mode_yields_one_batch():
    # write_mode="eager" collects everything first, so there is always
    # exactly one batch regardless of data size or batch_size setting.
    lf = pl.LazyFrame({"x": list(range(10))})
    config = {"write_mode": "eager", "write_options": {"batch_size": 3}}
    batches = list(_iter_arrow_batches(lf, config))
    assert len(batches) == 1
    assert batches[0].num_rows == 10


# ---------------------------------------------------------------------------
# _append_in_single_snapshot
# ---------------------------------------------------------------------------


def test_append_writes_rows_to_empty_table(iceberg_table):
    data = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
    with iceberg_table.transaction() as txn:
        _append_in_single_snapshot(txn, iceberg_table.io, data, {})

    result = iceberg_table.scan().to_arrow()
    assert result.num_rows == 2


def test_append_does_not_remove_existing_rows(iceberg_table):
    # Each call appends; the table accumulates rows from all calls.
    first = pl.DataFrame({"id": [1], "val": ["a"]})
    second = pl.DataFrame({"id": [2], "val": ["b"]})

    with iceberg_table.transaction() as txn:
        _append_in_single_snapshot(txn, iceberg_table.io, first, {})
    with iceberg_table.transaction() as txn:
        _append_in_single_snapshot(txn, iceberg_table.io, second, {})

    result = iceberg_table.scan().to_arrow()
    assert result.num_rows == 2


# ---------------------------------------------------------------------------
# _replace_in_single_snapshot
# ---------------------------------------------------------------------------


def test_replace_overwrites_existing_rows(iceberg_table):
    # After a replace the table contains only the new rows, not old + new.
    original = pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    with iceberg_table.transaction() as txn:
        _append_in_single_snapshot(txn, iceberg_table.io, original, {})

    replacement = pl.DataFrame({"id": [99], "val": ["z"]})
    with iceberg_table.transaction() as txn:
        _replace_in_single_snapshot(txn, iceberg_table.io, replacement, {})

    result = pl.from_arrow(iceberg_table.scan().to_arrow())
    assert result["id"].to_list() == [99]


def test_replace_on_empty_table_writes_rows(iceberg_table):
    data = pl.DataFrame({"id": [7], "val": ["x"]})
    with iceberg_table.transaction() as txn:
        _replace_in_single_snapshot(txn, iceberg_table.io, data, {})

    result = iceberg_table.scan().to_arrow()
    assert result.num_rows == 1


# ---------------------------------------------------------------------------
# _overwrite_scd_ids_and_append
# ---------------------------------------------------------------------------


def test_matched_rows_are_removed_and_new_rows_inserted(scd_table):
    # Closing row "id-2": remove its old version, write a new version alongside
    # any brand-new rows.  "id-1" and "id-3" are untouched.
    new_rows = pl.DataFrame(
        {"dbt_scd_id": ["id-2", "id-4"], "val": ["b_closed", "d_new"]}
    )
    with scd_table.transaction() as txn:
        _overwrite_scd_ids_and_append(scd_table, txn, scd_table.io, ["id-2"], new_rows)

    result = pl.from_arrow(scd_table.scan().to_arrow()).sort("dbt_scd_id")
    assert result["dbt_scd_id"].to_list() == ["id-1", "id-2", "id-3", "id-4"]
    assert result.filter(pl.col("dbt_scd_id") == "id-2")["val"].to_list() == [
        "b_closed"
    ]


def test_unmatched_rows_are_preserved(scd_table):
    # Rows not in scd_ids must survive unchanged.
    new_rows = pl.DataFrame({"dbt_scd_id": ["id-1_closed"], "val": ["a_closed"]})
    with scd_table.transaction() as txn:
        _overwrite_scd_ids_and_append(scd_table, txn, scd_table.io, ["id-1"], new_rows)

    result = pl.from_arrow(scd_table.scan().to_arrow())
    surviving_ids = set(result["dbt_scd_id"].to_list())
    assert "id-2" in surviving_ids
    assert "id-3" in surviving_ids
    assert "id-1" not in surviving_ids


def test_empty_scd_ids_only_appends(scd_table):
    # With no rows to overwrite the function only appends — existing rows are kept.
    new_rows = pl.DataFrame({"dbt_scd_id": ["id-99"], "val": ["new"]})
    with scd_table.transaction() as txn:
        _overwrite_scd_ids_and_append(scd_table, txn, scd_table.io, [], new_rows)

    result = scd_table.scan().to_arrow()
    assert result.num_rows == 4  # 3 original + 1 new


# ---------------------------------------------------------------------------
# _sync_schema
# ---------------------------------------------------------------------------


def test_new_column_is_added(iceberg_table):
    import pyarrow as pa

    new_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("val", pa.string()),
            pa.field("extra", pa.int32()),
        ]
    )
    with iceberg_table.transaction() as txn:
        _sync_schema(txn, iceberg_table.schema(), new_schema)

    field_names = [f.name for f in iceberg_table.schema().fields]
    assert "extra" in field_names


def test_removed_column_is_dropped(iceberg_table):
    import pyarrow as pa

    schema_without_val = pa.schema([pa.field("id", pa.int64())])
    with iceberg_table.transaction() as txn:
        _sync_schema(txn, iceberg_table.schema(), schema_without_val)

    field_names = [f.name for f in iceberg_table.schema().fields]
    assert "val" not in field_names
    assert "id" in field_names


# ---------------------------------------------------------------------------
# _sync_partition_spec
# ---------------------------------------------------------------------------


def test_new_partition_field_is_added(iceberg_table):
    with iceberg_table.transaction() as txn:
        _sync_partition_spec(txn, ["id"])

    spec_names = {f.name for f in iceberg_table.spec().fields}
    assert "id" in spec_names


def test_stale_partition_field_is_removed(iceberg_table):
    # First add a partition, then sync to a spec without it.
    with iceberg_table.transaction() as txn:
        _sync_partition_spec(txn, ["id"])
    with iceberg_table.transaction() as txn:
        _sync_partition_spec(txn, [])

    spec_names = {f.name for f in iceberg_table.spec().fields}
    assert "id" not in spec_names


def test_no_op_when_spec_already_matches(iceberg_table):
    # When the spec is already correct, commit the transaction and verify
    # no new snapshot is added (snapshot count stays the same).
    with iceberg_table.transaction() as txn:
        _sync_partition_spec(txn, ["id"])

    snapshot_count_before = len(iceberg_table.history())

    with iceberg_table.transaction() as txn:
        _sync_partition_spec(txn, ["id"])  # no-op

    assert len(iceberg_table.history()) == snapshot_count_before
