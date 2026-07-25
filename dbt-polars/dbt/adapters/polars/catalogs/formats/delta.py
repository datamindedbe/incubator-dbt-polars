from pathlib import Path

from dbt.adapters.polars.catalogs.baseCatalog import get_write_options
from deltalake import DeltaTable

import polars as pl


class DeltaFormat:
    @staticmethod
    def _write(
        path: str,
        data: pl.DataFrame | pl.LazyFrame,
        mode: str,
        model_config: dict,
        adapter_delta_opts: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        if (
            isinstance(data, pl.LazyFrame)
            and model_config.get("write_mode", "lazy") == "lazy"
        ):
            kwargs = get_write_options(
                pl.LazyFrame.sink_delta,
                model_config,
                ignore={"mode", "target", "storage_options"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            data.sink_delta(path, mode=mode, storage_options=storage_options, **kwargs)  # type: ignore[call-overload]
        else:
            df = data.collect() if isinstance(data, pl.LazyFrame) else data
            kwargs = get_write_options(
                pl.DataFrame.write_delta,
                model_config,
                ignore={"mode", "target", "storage_options"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            df.write_delta(path, mode=mode, storage_options=storage_options, **kwargs)  # type: ignore[call-overload]

    @staticmethod
    def write(
        path: str | Path,
        data: pl.DataFrame | pl.LazyFrame,
        mode: str,
        model_config: dict,
        partition_by: list[str] | None = None,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        adapter_delta_opts: dict = {
            "schema_mode": "overwrite" if mode == "overwrite" else "merge"
        }
        if partition_by:
            adapter_delta_opts["partition_by"] = partition_by
        DeltaFormat._write(
            str(path),
            data,
            mode,
            model_config,
            adapter_delta_opts,
            storage_options=storage_options,
        )

    @staticmethod
    def read(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> pl.LazyFrame:
        return pl.scan_delta(str(path), storage_options=storage_options)

    @staticmethod
    def append(
        path: str | Path,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool,
        model_config: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        adapter_delta_opts = {"schema_mode": "merge"} if allow_schema_evolution else {}
        DeltaFormat._write(
            str(path),
            data,
            "append",
            model_config,
            adapter_delta_opts,
            storage_options=storage_options,
        )

    @staticmethod
    def merge(
        path: str | Path,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None,
        incremental_predicates: list[str] | None,
        allow_schema_evolution: bool,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        predicate = " AND ".join(
            f"DBT_INTERNAL_SOURCE.{k} = DBT_INTERNAL_DEST.{k}" for k in keys
        )
        if incremental_predicates:
            predicate += " AND " + " AND ".join(incremental_predicates)
        dt = DeltaTable(str(path), storage_options=storage_options)
        (
            dt.merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
            )
            .when_matched_update_all(except_cols=except_cols)
            .when_not_matched_insert_all()
            .execute()
        )

    @staticmethod
    def delete_matched(
        path: str | Path,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        predicate = " AND ".join(
            f"DBT_INTERNAL_SOURCE.{k} = DBT_INTERNAL_DEST.{k}" for k in keys
        )
        if incremental_predicates:
            predicate += " AND " + " AND ".join(incremental_predicates)
        dt = DeltaTable(str(path), storage_options=storage_options)
        (
            dt.merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
            )
            .when_matched_delete()
            .execute()
        )

    @staticmethod
    def truncate(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        DeltaTable(str(path), storage_options=storage_options).delete()

    @staticmethod
    def apply_snapshot(
        path: str | Path,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        staging = pl.concat([rows_to_close, rows_to_insert])
        dt = DeltaTable(str(path), storage_options=storage_options)
        (
            dt.merge(
                staging.to_arrow(),
                f"target.{scd_id_col} = source.{scd_id_col}",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    @staticmethod
    def get_partition_columns(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> list[str]:
        return (
            DeltaTable(str(path), storage_options=storage_options)
            .metadata()
            .partition_columns
        )

    @staticmethod
    def set_relation_comment(
        path: str | Path,
        comment: str,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        DeltaTable(
            str(path), storage_options=storage_options
        ).alter.set_table_description(comment)

    @staticmethod
    def get_relation_comment(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> str | None:
        return (
            DeltaTable(str(path), storage_options=storage_options)
            .metadata()
            .description
        )

    @staticmethod
    def set_column_comments(
        path: str | Path,
        comments: dict[str, str],
        storage_options: dict[str, str] | None = None,
    ) -> None:
        dt = DeltaTable(str(path), storage_options=storage_options)
        for column, comment in comments.items():
            dt.alter.set_column_metadata(column, {"comment": comment})

    @staticmethod
    def get_column_comments(
        path: str | Path,
        storage_options: dict[str, str] | None = None,
    ) -> dict[str, str]:
        dt = DeltaTable(str(path), storage_options=storage_options)
        return {
            field.name: field.metadata["comment"]
            for field in dt.schema().fields
            if field.metadata.get("comment")
        }
