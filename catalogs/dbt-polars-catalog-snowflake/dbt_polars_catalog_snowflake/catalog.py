from typing import TypeVar, cast

import polars as pl
from dbt.adapters.polars.catalogs import (
    IcebergCatalog,
    PolarsRelation,
    reject_file_format,
)
from dbt_common.exceptions import DbtRuntimeError

from dbt_polars_catalog_snowflake.config import SnowflakeCatalogConfig

FILE_FORMAT_HINT = "the snowflake catalog always writes Iceberg tables."

Frame = TypeVar("Frame", bound=pl.DataFrame | pl.LazyFrame)


def column_names(data: pl.DataFrame | pl.LazyFrame) -> list[str]:
    if isinstance(data, pl.LazyFrame):
        return data.collect_schema().names()
    return data.columns


def rename_columns(data: Frame, rename, description: str) -> Frame:
    """Rename every column with `rename`, failing on names that would collide."""
    names = column_names(data)
    renamed = [rename(name) for name in names]
    if len(set(renamed)) != len(renamed):
        collisions = sorted({n for n in renamed if renamed.count(n) > 1})
        raise DbtRuntimeError(
            f"Columns {', '.join(collisions)} only differ in case, which {description}."
        )
    return cast(Frame, data.rename(dict(zip(names, renamed, strict=True))))


class SnowflakeCatalog(IcebergCatalog):
    """Snowflake-managed Iceberg tables through Snowflake Horizon Catalog.

    Uses Horizon's Iceberg REST endpoint with credentials vended by Snowflake,
    so no Snowflake warehouse is needed. Schema and table names, and by default
    column names, are stored in UPPERCASE, as Snowflake does for unquoted names,
    and lower-cased again for dbt.
    """

    config: SnowflakeCatalogConfig  # type: ignore[assignment]

    def __init__(self, config: SnowflakeCatalogConfig, project_root: str) -> None:
        super().__init__(config.iceberg_config(), project_root)
        self.config = config

    def catalog_name(self, name: str) -> str:
        return name.upper()

    def dbt_name(self, name: str) -> str:
        return name.lower()

    # ── column names ────────────────────────────────────────────────────────────

    def column_name(self, name: str) -> str:
        """The name of a dbt column in Snowflake."""
        return name.upper() if self.config.uppercase_column_names else name

    def to_snowflake(self, data: Frame) -> Frame:
        if not self.config.uppercase_column_names:
            return data
        return rename_columns(data, str.upper, "Snowflake stores as one column")

    def to_dbt(self, data: Frame) -> Frame:
        if not self.config.uppercase_column_names:
            return data
        return rename_columns(data, str.lower, "dbt-polars reads as one column")

    # ── reads ───────────────────────────────────────────────────────────────────

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return self.to_dbt(super().get_relation(relation))

    def get_partition_columns(self, relation: PolarsRelation) -> list[str]:
        columns = super().get_partition_columns(relation)
        if not self.config.uppercase_column_names:
            return columns
        return [column.lower() for column in columns]

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        comments = super().get_column_comments(relation)
        if not self.config.uppercase_column_names:
            return comments
        return {column.lower(): comment for column, comment in comments.items()}

    # ── writes ──────────────────────────────────────────────────────────────────

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        reject_file_format(relation, self.config.type, FILE_FORMAT_HINT)
        super().write_relation(
            relation,
            self.to_snowflake(data),
            [self.column_name(column) for column in partition_by],
            model_config,
        )

    def validate_incremental_relation(
        self, relation: PolarsRelation, model_config: dict
    ) -> None:
        reject_file_format(relation, self.config.type, FILE_FORMAT_HINT)

    def append_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        super().append_relation(
            relation, self.to_snowflake(data), allow_schema_evolution, model_config
        )

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None = None,
        incremental_predicates: list[str] | None = None,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        super().merge_relation(
            relation,
            self.to_snowflake(df),
            [self.column_name(key) for key in keys],
            None
            if except_cols is None
            else [self.column_name(column) for column in except_cols],
            incremental_predicates,
            allow_schema_evolution,
            model_config,
        )

    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
        model_config: dict | None = None,
    ) -> None:
        super().delete_matched_relation(
            relation,
            self.to_snowflake(df),
            [self.column_name(key) for key in keys],
            incremental_predicates,
            model_config,
        )

    def apply_snapshot_delta(
        self,
        relation: PolarsRelation,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str = "dbt_scd_id",
        model_config: dict | None = None,
    ) -> None:
        reject_file_format(relation, self.config.type, FILE_FORMAT_HINT)
        super().apply_snapshot_delta(
            relation,
            self.to_snowflake(rows_to_close),
            self.to_snowflake(rows_to_insert),
            self.column_name(scd_id_col),
            model_config,
        )

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        super().set_column_comments(
            relation,
            {self.column_name(column): comment for column, comment in comments.items()},
        )
