from __future__ import annotations

from dbt.adapters.polars.formats.baseFormat import BaseFormat
from dbt.adapters.polars.relation import TableFormat
from dbt.adapters.polars.types import TableColumnName
from deltalake import DeltaTable as _DeltaTable

import polars as pl


class DeltaLakeFormat(BaseFormat):
    """Delta Lake table format backed by ``delta-rs`` and ``polars``.

    *storage_options* follows the ``delta-rs`` convention and is the sole
    mechanism for supplying cloud credentials — the same class works for every
    storage backend:

    * **Local filesystem** — omit or pass ``None``
    * **AWS S3** — ``{"AWS_REGION": "eu-west-1"}`` (credentials from env /
      instance profile)
    * **Google Cloud Storage** — ``{"GOOGLE_SERVICE_ACCOUNT": "/path/to/sa.json"}``
    * **Azure Blob Storage** — ``{"AZURE_STORAGE_ACCOUNT_NAME": "…",
      "AZURE_STORAGE_ACCESS_KEY": "…"}``

    No credentials are stored here; pass them via environment variables or
    the dict above and let ``delta-rs`` resolve them through the standard chain.
    """

    table_format = TableFormat.delta

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open(self, uri: str) -> _DeltaTable:
        return _DeltaTable(uri, storage_options=self._storage_options)

    # ------------------------------------------------------------------
    # BaseFormat implementation
    # ------------------------------------------------------------------

    def is_table(self, uri: str) -> bool:
        return _DeltaTable.is_deltatable(uri, storage_options=self._storage_options)

    def scan(self, uri: str) -> pl.LazyFrame:
        return pl.scan_delta(uri, storage_options=self._storage_options)

    def write(self, uri: str, df: pl.DataFrame) -> None:
        df.write_delta(uri, mode="overwrite", storage_options=self._storage_options)

    def truncate(self, uri: str) -> None:
        self._open(uri).delete()

    def append(
        self,
        uri: str,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None:
        delta_write_options = (
            {"schema_mode": "merge"} if allow_schema_evolution else None
        )
        df.write_delta(
            uri,
            mode="append",
            delta_write_options=delta_write_options,
            storage_options=self._storage_options,
        )

    def merge(
        self,
        uri: str,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[TableColumnName] | None = None,
    ) -> None:
        (
            self._open(uri)
            .merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
            )
            .when_matched_update_all(except_cols=except_cols)
            .when_not_matched_insert_all()
            .execute()
        )

    def delete_matched(
        self,
        uri: str,
        df: pl.DataFrame,
        predicate: str,
    ) -> None:
        (
            self._open(uri)
            .merge(
                df.to_arrow(),
                predicate,
                source_alias="DBT_INTERNAL_SOURCE",
                target_alias="DBT_INTERNAL_DEST",
            )
            .when_matched_delete()
            .execute()
        )

    def set_table_comment(self, uri: str, comment: str) -> None:
        self._open(uri).alter.set_table_description(comment)

    def get_table_comment(self, uri: str) -> str | None:
        return self._open(uri).metadata().description

    def set_column_comments(
        self, uri: str, comments: dict[TableColumnName, str]
    ) -> None:
        dt = self._open(uri)
        for column, comment in comments.items():
            dt.alter.set_column_metadata(column, {"comment": comment})

    def get_column_comments(self, uri: str) -> dict[TableColumnName, str]:
        return {
            field.name: field.metadata["comment"]
            for field in self._open(uri).schema().fields
            if field.metadata.get("comment")
        }
