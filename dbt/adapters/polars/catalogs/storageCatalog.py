from abc import abstractmethod
from typing import NoReturn

from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

import polars as pl
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog
from dbt.adapters.polars.catalogs.formats.delta import DeltaFormat
from dbt.adapters.polars.catalogs.formats.file import FileFormat
from dbt.adapters.polars.relation import PolarsRelation

logger = AdapterLogger("polars")


def _unsupported_for_format(method: str, fmt: str) -> NoReturn:
    raise DbtRuntimeError(
        f"{method} is only supported for delta file_format, got '{fmt}'"
    )


class StorageCatalog(BaseCatalog):
    """Intermediate ABC that owns format dispatch and storage_options injection.

    Subclasses implement _get_uri, _get_storage_options, and all schema-management
    methods (create_schema, drop_schema, list_schemas, table_exists, drop_relation,
    list_relations_without_caching). Data-path methods are provided here.
    """

    @abstractmethod
    def _get_uri(self, relation: PolarsRelation) -> str: ...

    @abstractmethod
    def _get_storage_options(self, uri: str) -> dict[str, str] | None: ...

    # ── schema management — abstract, each backend implements ─────────────────

    @abstractmethod
    def create_schema(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def drop_schema(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def list_schemas(self) -> list[str]: ...

    @abstractmethod
    def table_exists(self, relation: PolarsRelation) -> bool: ...

    @abstractmethod
    def drop_relation(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]: ...

    # ── data-path methods ─────────────────────────────────────────────────────

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            return DeltaFormat.read(uri, storage_options=opts)
        return FileFormat.read(
            uri, relation.file_format, relation.read_options, storage_options=opts
        )

    def get_partition_columns(self, relation: PolarsRelation) -> list[str]:
        if relation.file_format != "delta":
            return []
        uri = self._get_uri(relation)
        return DeltaFormat.get_partition_columns(
            uri, storage_options=self._get_storage_options(uri)
        )

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        if partition_by and relation.file_format != "delta":
            raise DbtRuntimeError(
                "partition_by is not supported "
                + f"for file_format='{relation.file_format}'. "
                + "Use file_format='delta' to enable partitioning."
            )
        logger.debug(
            f"Writing table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            DeltaFormat.write(
                uri,
                data,
                "overwrite",
                model_config,
                partition_by or None,
                storage_options=opts,
            )
        else:
            FileFormat.write(
                uri, data, relation.file_format, model_config, storage_options=opts
            )

    def truncate_relation(
        self, relation: PolarsRelation, model_config: dict | None = None
    ) -> None:
        logger.debug(
            "Truncating table "
            + f"{relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            DeltaFormat.truncate(uri, storage_options=opts)
        else:
            FileFormat.truncate(
                uri,
                relation.file_format,
                relation.read_options,
                storage_options=opts,
                model_config=model_config,
            )

    def append_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            DeltaFormat.append(
                uri, data, allow_schema_evolution, model_config, storage_options=opts
            )
        else:
            FileFormat.append(
                uri,
                relation.file_format,
                data,
                model_config,
                relation.read_options,
                storage_options=opts,
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
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            DeltaFormat.merge(
                uri,
                df,
                keys,
                except_cols,
                incremental_predicates,
                allow_schema_evolution,
                storage_options=opts,
            )
        else:
            if incremental_predicates:
                raise DbtRuntimeError(
                    "FileFormats do not support incremental predicates. Switch to "
                    "delta format to start using incremental predicates."
                )
            FileFormat.merge(
                uri,
                relation.file_format,
                df,
                keys,
                except_cols,
                relation.read_options,
                storage_options=opts,
                model_config=model_config,
            )

    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
        model_config: dict | None = None,
    ) -> None:
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            DeltaFormat.delete_matched(
                uri, df, keys, incremental_predicates, storage_options=opts
            )
        else:
            FileFormat.delete_matched(
                uri,
                relation.file_format,
                df,
                keys,
                incremental_predicates,
                relation.read_options,
                storage_options=opts,
                model_config=model_config,
            )

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        if relation.file_format != "delta":
            _unsupported_for_format("set_relation_comment", relation.file_format)
        uri = self._get_uri(relation)
        DeltaFormat.set_relation_comment(
            uri, comment, storage_options=self._get_storage_options(uri)
        )

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        if relation.file_format != "delta":
            _unsupported_for_format("set_column_comments", relation.file_format)
        uri = self._get_uri(relation)
        DeltaFormat.set_column_comments(
            uri, comments, storage_options=self._get_storage_options(uri)
        )

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        if relation.file_format != "delta":
            return None
        uri = self._get_uri(relation)
        return DeltaFormat.get_relation_comment(
            uri, storage_options=self._get_storage_options(uri)
        )

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        if relation.file_format != "delta":
            return {}
        uri = self._get_uri(relation)
        return DeltaFormat.get_column_comments(
            uri, storage_options=self._get_storage_options(uri)
        )

    def apply_snapshot_delta(
        self,
        relation: PolarsRelation,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str = "dbt_scd_id",
        model_config: dict | None = None,
    ) -> None:
        if rows_to_close.is_empty() and rows_to_insert.is_empty():
            return
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            if rows_to_close.is_empty():
                DeltaFormat.append(uri, rows_to_insert, False, {}, storage_options=opts)
                return
            DeltaFormat.apply_snapshot(
                uri, rows_to_close, rows_to_insert, scd_id_col, storage_options=opts
            )
        else:
            if rows_to_close.is_empty():
                FileFormat.append(
                    uri,
                    relation.file_format,
                    rows_to_insert,
                    model_config or {},
                    relation.read_options,
                    storage_options=opts,
                )
                return
            FileFormat.apply_snapshot(
                uri,
                relation.file_format,
                rows_to_close,
                rows_to_insert,
                scd_id_col,
                relation.read_options,
                storage_options=opts,
                model_config=model_config,
            )
