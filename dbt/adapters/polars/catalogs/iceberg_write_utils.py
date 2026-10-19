from __future__ import annotations

import itertools
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.io import FileIO
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table, Transaction
    from pyiceberg.table.update.snapshot import _SnapshotProducer

_DEFAULT_BATCH_SIZE = 50_000_000


def _iter_arrow_batches(
    data: pl.DataFrame | pl.LazyFrame, model_config: dict
) -> Iterator[pa.Table]:
    if isinstance(data, pl.LazyFrame):
        if model_config.get("write_mode", "lazy") == "lazy":
            batch_size = model_config.get("write_options", {}).get(
                "batch_size", _DEFAULT_BATCH_SIZE
            )
            for batch in data.collect_batches(
                chunk_size=batch_size, engine="streaming", maintain_order=False
            ):
                yield batch.to_arrow()
        else:
            yield data.collect().to_arrow()
    else:
        yield data.to_arrow()


def _delete_current_data_files(
    snapshot_writer: _SnapshotProducer[Any], transaction: Transaction, io: FileIO
) -> None:
    snapshot = transaction.table_metadata.current_snapshot()
    if snapshot is None:
        return
    for manifest in snapshot.manifests(io):
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            snapshot_writer.delete_data_file(entry.data_file)


def _write_data_files(
    snapshot_writer: _SnapshotProducer[Any],
    transaction: Transaction,
    io: FileIO,
    data: pl.DataFrame | pl.LazyFrame,
    model_config: dict,
    counter: itertools.count | None = None,
) -> None:
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    counter = counter or itertools.count(0)
    for batch in _iter_arrow_batches(data, model_config):
        if batch.shape[0] == 0:
            continue
        for data_file in _dataframe_to_data_files(
            table_metadata=transaction.table_metadata,
            write_uuid=snapshot_writer.commit_uuid,
            df=batch,
            io=io,
            counter=counter,
        ):
            snapshot_writer.append_data_file(data_file)


def _append_in_single_snapshot(
    transaction: Transaction,
    io: FileIO,
    data: pl.DataFrame | pl.LazyFrame,
    model_config: dict,
) -> None:
    """One snapshot for the whole write: transaction.append per batch would stage one
    snapshot per batch, and some catalogs (Databricks Unity) reject commits
    containing multiple snapshots."""
    with transaction.update_snapshot().fast_append() as snapshot_writer:
        _write_data_files(snapshot_writer, transaction, io, data, model_config)


def _replace_in_single_snapshot(
    transaction: Transaction,
    io: FileIO,
    data: pl.DataFrame | pl.LazyFrame,
    model_config: dict,
) -> None:
    """Like _append_in_single_snapshot, but the same snapshot also deletes all
    current data files, so the table contents are replaced atomically."""
    with transaction.update_snapshot().overwrite() as snapshot_writer:
        _delete_current_data_files(snapshot_writer, transaction, io)
        _write_data_files(snapshot_writer, transaction, io, data, model_config)


def _overwrite_scd_ids_and_append(
    tbl: Table,
    transaction: Transaction,
    io: FileIO,
    scd_ids: list,
    new_rows: pl.DataFrame,
    scd_id_col: str = "dbt_scd_id",
) -> None:
    """Single-snapshot alternative to Transaction.delete(In(...)) + append().

    Transaction.delete() + append() stages 2-3 Iceberg snapshots; Databricks
    Unity Catalog rejects commits with more than one snapshot. This function
    combines both operations into one update_snapshot().overwrite() context.
    """
    from pyiceberg.expressions import AlwaysTrue, In
    from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files

    schema = transaction.table_metadata.schema()
    file_counter = itertools.count(0)
    with transaction.update_snapshot().overwrite() as snapshot_writer:
        scd_id_filter = In(scd_id_col, scd_ids)  # type: ignore[misc,call-arg,arg-type]
        for task in tbl.scan(row_filter=scd_id_filter).plan_files():
            all_rows = pl.DataFrame(
                ArrowScan(
                    table_metadata=transaction.table_metadata,
                    io=io,
                    projected_schema=schema,
                    row_filter=AlwaysTrue(),
                ).to_table(tasks=[task])
            )
            kept_rows = all_rows.filter(~pl.col(scd_id_col).is_in(scd_ids))
            if len(kept_rows) < len(all_rows):
                snapshot_writer.delete_data_file(task.file)
                if kept_rows.shape[0] > 0:
                    for data_file in _dataframe_to_data_files(
                        table_metadata=transaction.table_metadata,
                        write_uuid=snapshot_writer.commit_uuid,
                        df=kept_rows.to_arrow(),
                        io=io,
                        counter=file_counter,
                    ):
                        snapshot_writer.append_data_file(data_file)
        _write_data_files(
            snapshot_writer, transaction, io, new_rows, {}, counter=file_counter
        )


def _key_prune_filter(keys: list[str], key_rows: pl.DataFrame):
    """One In() per key column over its distinct values: a superset of the
    matching rows, used only to skip files. Its size is the sum of the distinct
    values per column, where an Or of one And per row grows with the batch and
    does not scale."""
    from pyiceberg.expressions import And, In

    filters = [
        In(k, key_rows[k].drop_nulls().unique().to_list())  # type: ignore[misc,call-arg,arg-type]
        for k in keys
    ]
    return filters[0] if len(filters) == 1 else And(*filters)


def _overwrite_keys_and_append(
    tbl: Table,
    transaction: Transaction,
    io: FileIO,
    keys: list[str],
    key_rows: pl.DataFrame,
    new_rows: pl.DataFrame,
) -> None:
    """Delete the rows whose `keys` match a row of `key_rows`, then append
    `new_rows`, in ONE overwrite snapshot (copy-on-write).

    Files are pruned with `_key_prune_filter`; each candidate file is read and
    anti-joined on the keys in Polars, and rewritten only if it loses rows. A NULL
    key never matches, as in SQL. The same shape as `_overwrite_scd_ids_and_append`,
    for any number of key columns.
    """
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files

    schema = transaction.table_metadata.schema()
    file_counter = itertools.count(0)
    matched = key_rows.select(keys).unique()
    with transaction.update_snapshot().overwrite() as snapshot_writer:
        if not matched.is_empty():
            for task in tbl.scan(
                row_filter=_key_prune_filter(keys, matched)
            ).plan_files():
                all_rows = pl.DataFrame(
                    ArrowScan(
                        table_metadata=transaction.table_metadata,
                        io=io,
                        projected_schema=schema,
                        row_filter=AlwaysTrue(),
                    ).to_table(tasks=[task])
                )
                # Batch key dtypes can differ from the stored ones (Int64 vs Int32).
                probe = matched.cast({k: all_rows.schema[k] for k in keys})
                kept_rows = all_rows.join(probe, on=keys, how="anti")
                if len(kept_rows) < len(all_rows):
                    snapshot_writer.delete_data_file(task.file)
                    if kept_rows.shape[0] > 0:
                        for data_file in _dataframe_to_data_files(
                            table_metadata=transaction.table_metadata,
                            write_uuid=snapshot_writer.commit_uuid,
                            df=kept_rows.to_arrow(),
                            io=io,
                            counter=file_counter,
                        ):
                            snapshot_writer.append_data_file(data_file)
        _write_data_files(
            snapshot_writer, transaction, io, new_rows, {}, counter=file_counter
        )


def _sync_schema(
    transaction: Transaction, current_schema: Schema, arrow_schema: pa.Schema
) -> None:
    removed_columns = [
        field.name
        for field in current_schema.fields
        if field.name not in arrow_schema.names
    ]
    # allow_incompatible_changes is required for column deletion
    with transaction.update_schema(allow_incompatible_changes=True) as update:
        update.union_by_name(arrow_schema)
        for column in removed_columns:
            update.delete_column(column)


def _sync_partition_spec(transaction: Transaction, partition_by: list[str]) -> None:
    current_fields = {field.name for field in transaction.table_metadata.spec().fields}
    fields_to_remove = [name for name in current_fields if name not in partition_by]
    fields_to_add = [col for col in partition_by if col not in current_fields]
    if not fields_to_remove and not fields_to_add:
        return
    with transaction.update_spec() as update:
        for name in fields_to_remove:
            update.remove_field(name)
        for col in fields_to_add:
            update.add_identity(col)
