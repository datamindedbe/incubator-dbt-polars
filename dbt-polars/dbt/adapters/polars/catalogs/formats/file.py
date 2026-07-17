import inspect
from pathlib import Path

from dbt.adapters.polars.catalogs.baseCatalog import get_write_options
from dbt.adapters.polars.catalogs.formats import FILE_FORMATS
import polars as pl


class FileFormat:
    @staticmethod
    def write(
        path: str | Path,
        data: pl.DataFrame | pl.LazyFrame,
        fmt: str,
        model_config: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        spec = FILE_FORMATS[fmt]
        kwargs = get_write_options(
            spec.sink, model_config, ignore={"path", "storage_options"}
        )
        lf = data if isinstance(data, pl.LazyFrame) else data.lazy()
        spec.sink(lf, path, storage_options=storage_options, **kwargs)

    @staticmethod
    def read(
        path: str | Path,
        fmt: str,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
    ) -> pl.LazyFrame:
        spec = FILE_FORMATS[fmt]
        valid = frozenset(inspect.signature(spec.scan).parameters) - {"source", "path"}
        kwargs = {k: v for k, v in read_options.items() if k in valid}
        kwargs["storage_options"] = storage_options
        return spec.scan(path, **kwargs)

    @staticmethod
    def append(
        path: str | Path,
        fmt: str,
        data: pl.DataFrame | pl.LazyFrame,
        model_config: dict,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        existing = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).collect()
        new = data.collect() if isinstance(data, pl.LazyFrame) else data
        if FILE_FORMATS[fmt].auto_cast:
            cast_exprs = [
                pl.col(c).cast(existing.schema[c])
                for c in new.columns
                if c in existing.schema and new.schema[c] != existing.schema[c]
            ]
            if cast_exprs:
                new = new.with_columns(cast_exprs)
        FileFormat.write(
            path,
            pl.concat([existing, new], how="diagonal_relaxed").lazy(),
            fmt,
            model_config,
            storage_options=storage_options,
        )

    @staticmethod
    def merge(
        path: str | Path,
        fmt: str,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None,
        _incremental_predicates: list[str] | None,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        existing = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).collect()

        if FILE_FORMATS[fmt].auto_cast:
            align_casts = [
                pl.col(c).cast(existing.schema[c])
                for c in df.columns
                if c in existing.schema and df.schema[c] != existing.schema[c]
            ]
            if align_casts:
                df = df.with_columns(align_casts)

        # Only update columns present in df (handles removed columns gracefully)
        update_cols = [
            c
            for c in existing.columns
            if c not in keys
            and (not except_cols or c not in except_cols)
            and c in df.columns
        ]
        # New columns from df not yet in existing (schema evolution)
        new_schema_cols = [c for c in df.columns if c not in existing.columns]

        # Update matched rows: override update_cols, keep except_cols unchanged
        updated = existing.update(df.select([*keys, *update_cols]), on=keys, how="left")
        # Attach new schema columns for matched rows via join (null for unmatched)
        if new_schema_cols:
            updated = updated.join(
                df.select([*keys, *new_schema_cols]), on=keys, how="left"
            )

        # Insert rows from df whose key is not in existing
        new_rows = df.join(existing.select(keys), on=keys, how="anti")
        # Fill columns that exist in existing but not in df with null
        for col in existing.columns:
            if col not in new_rows.columns:
                new_rows = new_rows.with_columns(
                    pl.lit(None).cast(existing.schema[col]).alias(col)
                )

        all_cols = [*existing.columns, *new_schema_cols]
        merged = pl.concat([updated.select(all_cols), new_rows.select(all_cols)])
        FileFormat.write(path, merged.lazy(), fmt, {}, storage_options=storage_options)

    @staticmethod
    def delete_matched(
        path: str | Path,
        fmt: str,
        df: pl.DataFrame,
        keys: list[str],
        _incremental_predicates: list[str] | None,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        existing = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).collect()
        result = existing.join(df.select(keys), on=keys, how="anti")
        FileFormat.write(path, result.lazy(), fmt, {}, storage_options=storage_options)

    @staticmethod
    def truncate(
        path: str | Path,
        fmt: str,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        schema = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).schema
        FileFormat.write(
            path, pl.LazyFrame(schema=schema), fmt, {}, storage_options=storage_options
        )

    @staticmethod
    def apply_snapshot(
        path: str | Path,
        fmt: str,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        existing = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).collect()
        updated = existing.update(rows_to_close, on=scd_id_col, how="left")
        result = pl.concat([updated, rows_to_insert])
        FileFormat.write(path, result.lazy(), fmt, {}, storage_options=storage_options)
