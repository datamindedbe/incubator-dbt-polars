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
        if storage_options is not None:
            kwargs["storage_options"] = storage_options
        lf = data if isinstance(data, pl.LazyFrame) else data.lazy()
        spec.sink(lf, path, **kwargs)

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
        if storage_options is not None:
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
        read_options: dict,
        storage_options: dict[str, str] | None = None,
        model_config: dict | None = None,
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

        update_cols = [
            c
            for c in existing.columns
            if c not in keys
            and (not except_cols or c not in except_cols)
            and c in df.columns
        ]
        new_schema_cols = [c for c in df.columns if c not in existing.columns]

        # how="left" means existing value wins for columns not in the update frame,
        # which is what keeps except_cols unchanged.
        updated = existing.update(df.select([*keys, *update_cols]), on=keys, how="left")
        if new_schema_cols:
            updated = updated.join(
                df.select([*keys, *new_schema_cols]), on=keys, how="left"
            )

        new_rows = df.join(existing.select(keys), on=keys, how="anti")
        # Align new_rows to all_cols so concat doesn't produce ragged frames.
        for col in existing.columns:
            if col not in new_rows.columns:
                new_rows = new_rows.with_columns(
                    pl.lit(None).cast(existing.schema[col]).alias(col)
                )

        all_cols = [*existing.columns, *new_schema_cols]
        merged = pl.concat([updated.select(all_cols), new_rows.select(all_cols)])
        FileFormat.write(
            path,
            merged.lazy(),
            fmt,
            model_config or {},
            storage_options=storage_options,
        )

    @staticmethod
    def delete_matched(
        path: str | Path,
        fmt: str,
        df: pl.DataFrame,
        keys: list[str],
        _incremental_predicates: list[str] | None,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
        model_config: dict | None = None,
    ) -> None:
        existing = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).collect()
        result = existing.join(df.select(keys), on=keys, how="anti")
        FileFormat.write(
            path,
            result.lazy(),
            fmt,
            model_config or {},
            storage_options=storage_options,
        )

    @staticmethod
    def truncate(
        path: str | Path,
        fmt: str,
        read_options: dict,
        storage_options: dict[str, str] | None = None,
        model_config: dict | None = None,
    ) -> None:
        schema = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).schema
        FileFormat.write(
            path,
            pl.LazyFrame(schema=schema),
            fmt,
            model_config or {},
            storage_options=storage_options,
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
        model_config: dict | None = None,
    ) -> None:
        existing = FileFormat.read(
            path, fmt, read_options, storage_options=storage_options
        ).collect()
        updated = existing.update(rows_to_close, on=scd_id_col, how="left")
        result = pl.concat([updated, rows_to_insert])
        FileFormat.write(
            path,
            result.lazy(),
            fmt,
            model_config or {},
            storage_options=storage_options,
        )
