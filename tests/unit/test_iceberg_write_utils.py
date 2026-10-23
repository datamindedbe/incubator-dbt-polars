import polars as pl
import pytest

from dbt.adapters.polars.catalogs.iceberg_write_utils import (
    _append_in_single_snapshot,
    _iter_arrow_batches,
    _replace_in_single_snapshot,
    _sync_partition_spec,
    _sync_schema,
    key_prune_filter,
    overwrite_keys_and_append,
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
# overwrite_keys_and_append
# ---------------------------------------------------------------------------


def scd_ids(*ids: str) -> pl.DataFrame:
    return pl.DataFrame({"dbt_scd_id": pl.Series(ids, dtype=pl.String)})


def test_matched_rows_are_removed_and_new_rows_inserted(scd_table):
    # Closing row "id-2": remove its old version, write a new version alongside
    # any brand-new rows.  "id-1" and "id-3" are untouched.
    new_rows = pl.DataFrame(
        {"dbt_scd_id": ["id-2", "id-4"], "val": ["b_closed", "d_new"]}
    )
    with scd_table.transaction() as txn:
        overwrite_keys_and_append(
            scd_table, txn, scd_table.io, ["dbt_scd_id"], scd_ids("id-2"), new_rows
        )

    result = pl.from_arrow(scd_table.scan().to_arrow()).sort("dbt_scd_id")
    assert result["dbt_scd_id"].to_list() == ["id-1", "id-2", "id-3", "id-4"]
    assert result.filter(pl.col("dbt_scd_id") == "id-2")["val"].to_list() == [
        "b_closed"
    ]


def test_unmatched_rows_are_preserved(scd_table):
    # Rows not in scd_ids must survive unchanged.
    new_rows = pl.DataFrame({"dbt_scd_id": ["id-1_closed"], "val": ["a_closed"]})
    with scd_table.transaction() as txn:
        overwrite_keys_and_append(
            scd_table, txn, scd_table.io, ["dbt_scd_id"], scd_ids("id-1"), new_rows
        )

    result = pl.from_arrow(scd_table.scan().to_arrow())
    surviving_ids = set(result["dbt_scd_id"].to_list())
    assert "id-2" in surviving_ids
    assert "id-3" in surviving_ids
    assert "id-1" not in surviving_ids


def test_empty_scd_ids_only_appends(scd_table):
    # With no rows to overwrite the function only appends — existing rows are kept.
    new_rows = pl.DataFrame({"dbt_scd_id": ["id-99"], "val": ["new"]})
    with scd_table.transaction() as txn:
        overwrite_keys_and_append(
            scd_table, txn, scd_table.io, ["dbt_scd_id"], scd_ids(), new_rows
        )

    result = scd_table.scan().to_arrow()
    assert result.num_rows == 4  # 3 original + 1 new


@pytest.fixture
def composite_table(tmp_path):
    """Table keyed on (k1, k2), with k1 stored as int32, pre-loaded in one file."""
    pytest.importorskip("pyiceberg")
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "test",
        **{
            "type": "sql",
            "uri": f"sqlite:///{tmp_path}/composite.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )
    catalog.create_namespace(("ns",))
    schema = pa.schema(
        [
            pa.field("k1", pa.int32()),
            pa.field("k2", pa.string()),
            pa.field("val", pa.string()),
        ]
    )
    tbl = catalog.create_table("ns.composite", schema=schema)
    tbl.append(
        pa.table(
            {
                "k1": pa.array([1, 1, 2, 2, None], pa.int32()),
                "k2": ["a", "b", "a", "b", "a"],
                "val": ["1a", "1b", "2a", "2b", "null_a"],
            },
            schema=schema,
        )
    )
    return tbl


def remaining_vals(tbl) -> list[str]:
    return sorted(pl.from_arrow(tbl.scan().to_arrow())["val"].to_list())


def test_composite_key_deletes_exact_tuples_not_cartesian_product(composite_table):
    keys = pl.DataFrame({"k1": [1, 2], "k2": ["a", "b"]})
    with composite_table.transaction() as txn:
        overwrite_keys_and_append(
            composite_table, txn, composite_table.io, ["k1", "k2"], keys, keys.clear()
        )

    assert remaining_vals(composite_table) == ["1b", "2a", "null_a"]


def test_null_key_never_matches(composite_table):
    keys = pl.DataFrame({"k1": pl.Series([None], dtype=pl.Int64), "k2": ["a"]})
    with composite_table.transaction() as txn:
        overwrite_keys_and_append(
            composite_table, txn, composite_table.io, ["k1", "k2"], keys, keys.clear()
        )

    assert remaining_vals(composite_table) == ["1a", "1b", "2a", "2b", "null_a"]


def test_delete_and_append_commit_one_snapshot(composite_table):
    snapshots_before = len(composite_table.snapshots())
    keys = pl.DataFrame({"k1": [1], "k2": ["a"]})
    new_rows = pl.DataFrame(
        {"k1": pl.Series([1], dtype=pl.Int32), "k2": ["a"], "val": ["1a_new"]}
    )
    with composite_table.transaction() as txn:
        overwrite_keys_and_append(
            composite_table, txn, composite_table.io, ["k1", "k2"], keys, new_rows
        )

    assert len(composite_table.snapshots()) == snapshots_before + 1
    assert remaining_vals(composite_table) == ["1a_new", "1b", "2a", "2b", "null_a"]


def test_files_without_matching_keys_are_not_rewritten(composite_table):
    import pyarrow as pa

    composite_table.append(
        pa.table(
            {
                "k1": pa.array([3], pa.int32()),
                "k2": ["a"],
                "val": ["3a"],
            },
            schema=composite_table.schema().as_arrow(),
        )
    )
    files_before = {task.file.file_path for task in composite_table.scan().plan_files()}

    # (1, "z") passes the In() pruning of the first file but matches no row in it.
    keys = pl.DataFrame({"k1": [1, 3], "k2": ["z", "a"]})
    with composite_table.transaction() as txn:
        overwrite_keys_and_append(
            composite_table, txn, composite_table.io, ["k1", "k2"], keys, keys.clear()
        )

    files_after = {task.file.file_path for task in composite_table.scan().plan_files()}
    assert len(files_before - files_after) == 1
    assert remaining_vals(composite_table) == ["1a", "1b", "2a", "2b", "null_a"]


def test_large_composite_key_batch_completes(composite_table):
    import time

    keys = pl.DataFrame({"k1": list(range(20_000)), "k2": ["a"] * 20_000})
    start = time.perf_counter()
    with composite_table.transaction() as txn:
        overwrite_keys_and_append(
            composite_table, txn, composite_table.io, ["k1", "k2"], keys, keys.clear()
        )

    assert time.perf_counter() - start < 10
    assert remaining_vals(composite_table) == ["1b", "2b", "null_a"]


# ---------------------------------------------------------------------------
# key_prune_filter
# ---------------------------------------------------------------------------


def test_prune_filter_is_one_in_per_key_over_distinct_values():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import And, In

    keys = pl.DataFrame({"k1": [1, 2, 2, 3], "k2": ["a", "a", "b", None]})
    result = key_prune_filter(["k1", "k2"], keys)

    assert isinstance(result, And)
    assert isinstance(result.left, In) and isinstance(result.right, In)
    assert {lit.value for lit in result.left.literals} == {1, 2, 3}
    assert {lit.value for lit in result.right.literals} == {"a", "b"}


def test_prune_filter_all_null_key_matches_nothing():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import AlwaysFalse

    keys = pl.DataFrame({"k1": pl.Series([None, None], dtype=pl.Int64)})
    assert key_prune_filter(["k1"], keys) == AlwaysFalse()


def test_prune_filter_skips_float_keys():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import AlwaysTrue

    keys = pl.DataFrame({"k1": [1.0, float("nan")]})
    assert key_prune_filter(["k1"], keys) == AlwaysTrue()


def test_prune_filter_uses_range_above_in_predicate_limit():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

    keys = pl.DataFrame({"k1": list(range(10, 1010))})
    result = key_prune_filter(["k1"], keys)

    assert result == And(GreaterThanOrEqual("k1", 10), LessThanOrEqual("k1", 1009))


def test_prune_filter_skips_enum_keys_above_in_predicate_limit():
    # Enum min/max follow category order, not the lexical order Iceberg uses.
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import AlwaysTrue

    categories = [f"v{i:04d}" for i in range(1000)][::-1]
    keys = pl.DataFrame({"k1": pl.Series(categories, dtype=pl.Enum(categories))})
    assert key_prune_filter(["k1"], keys) == AlwaysTrue()


@pytest.mark.parametrize(
    "ids, expected_files",
    [
        # Few values: In() prunes the file in the gap between them.
        ([5, 2500], {0, 2}),
        # Above IN_PREDICATE_LIMIT: the range still prunes on file min/max stats.
        (list(range(1000, 1500)), {1}),
    ],
)
def test_prune_filter_skips_files_outside_the_batch_keys(
    iceberg_table, ids, expected_files
):
    def data_file_paths() -> set[str]:
        return {task.file.file_path for task in iceberg_table.scan().plan_files()}

    file_index_by_path = {}
    for file_index in range(3):
        file_ids = list(range(file_index * 1000, (file_index + 1) * 1000))
        data = pl.DataFrame({"id": file_ids, "val": [str(file_index)] * 1000})
        paths_before = data_file_paths()
        with iceberg_table.transaction() as txn:
            _append_in_single_snapshot(txn, iceberg_table.io, data, {})
        for path in data_file_paths() - paths_before:
            file_index_by_path[path] = file_index

    keys = pl.DataFrame({"id": ids})
    tasks = iceberg_table.scan(row_filter=key_prune_filter(["id"], keys)).plan_files()

    assert {file_index_by_path[task.file.file_path] for task in tasks} == expected_files


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
