import shutil
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.catalogs.formats import FILE_FORMATS
from dbt.adapters.polars.catalogs.formats.delta import DeltaFormat
from dbt.adapters.polars.catalogs.formats.file import FileFormat
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable

import polars as pl

logger = AdapterLogger("polars")

_FILE_FORMATS = frozenset(FILE_FORMATS)


def _unsupported_for_format(method: str, fmt: str) -> None:
    raise DbtRuntimeError(
        f"{method} is only supported for delta file_format, got '{fmt}'"
    )


@dataclass
class LocalCatalogConfig(CatalogConfig):
    name: str
    type: str
    root: str

    def unique_field(self) -> str:
        return self.root

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "root")


class LocalCatalog(BaseCatalog):
    config: LocalCatalogConfig

    def __init__(self, config: LocalCatalogConfig, project_root: str):
        root = Path(config.root)
        if not root.is_absolute():
            root = Path(project_root) / root
        absolute_root = root.resolve()
        if " " in str(absolute_root):
            raise DbtRuntimeError(
                f"LocalCatalog root resolves to '{absolute_root}', which contains "
                "a space. polars' Delta scanner (pl.scan_delta) cannot read tables "
                "whose path contains a space, see "
                "https://github.com/pola-rs/polars/issues/20944. Use a root path "
                "that resolves to an absolute path without spaces."
            )
        super().__init__(config, project_root)
        self.absolute_root = absolute_root

    def _schema_path(self, schema: str) -> Path:
        return self.absolute_root / schema

    def _relation_path(self, relation: PolarsRelation) -> Path:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
        return self._schema_path(relation.schema) / relation.identifier

    def _get_path(self, relation: PolarsRelation) -> Path:
        stem = self._relation_path(relation)
        if relation.file_format == "delta":
            return stem
        return stem.with_suffix(f".{relation.file_format}")

    def create_schema(self, relation: PolarsRelation) -> None:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        logger.debug(f"Creating schema {relation.catalog}/{relation.schema}")
        self._schema_path(relation.schema).mkdir(parents=True, exist_ok=True)

    def drop_schema(self, relation: PolarsRelation) -> None:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        logger.debug(f"Dropping schema {relation.catalog}/{relation.schema}")
        shutil.rmtree(self._schema_path(relation.schema), ignore_errors=True)

    def list_schemas(self) -> list[str]:
        if not self.absolute_root.exists():
            return []
        return [p.name for p in self.absolute_root.iterdir() if p.is_dir()]

    def table_exists(self, relation: PolarsRelation) -> bool:
        return self._get_path(relation).exists()

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        path = self._get_path(relation)
        if relation.file_format == "delta":
            return DeltaFormat.read(path)
        return FileFormat.read(path, relation.file_format, relation.read_options)

    def get_partition_columns(self, relation: PolarsRelation) -> list[str]:
        if relation.file_format != "delta":
            return []
        return DeltaFormat.get_partition_columns(self._get_path(relation))

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
                f"partition_by is not supported for file_format='{relation.file_format}'. "
                "Use file_format='delta' to enable partitioning."
            )
        logger.debug(
            f"Writing table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        path = self._get_path(relation)
        if relation.file_format == "delta":
            DeltaFormat.write(
                path, data, "overwrite", model_config, partition_by or None
            )
        else:
            FileFormat.write(path, data, relation.file_format, model_config)

    def drop_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Dropping table if exists {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        path = self._get_path(relation)
        if relation.file_format == "delta":
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()

    def truncate_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Truncating table {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        path = self._get_path(relation)
        if relation.file_format == "delta":
            DeltaFormat.truncate(path)
        else:
            FileFormat.truncate(path, relation.file_format, relation.read_options)

    def append_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        path = self._get_path(relation)
        if relation.file_format == "delta":
            DeltaFormat.append(path, data, allow_schema_evolution, model_config)
        else:
            FileFormat.append(
                path, relation.file_format, data, model_config, relation.read_options
            )

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None = None,
        incremental_predicates: list[str] | None = None,
        allow_schema_evolution: bool = False,
    ) -> None:
        path = self._get_path(relation)
        if relation.file_format == "delta":
            DeltaFormat.merge(
                path,
                df,
                keys,
                except_cols,
                incremental_predicates,
                allow_schema_evolution,
            )
        else:
            FileFormat.merge(
                path,
                relation.file_format,
                df,
                keys,
                except_cols,
                incremental_predicates,
                relation.read_options,
            )

    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
    ) -> None:
        path = self._get_path(relation)
        if relation.file_format == "delta":
            DeltaFormat.delete_matched(path, df, keys, incremental_predicates)
        else:
            FileFormat.delete_matched(
                path,
                relation.file_format,
                df,
                keys,
                incremental_predicates,
                relation.read_options,
            )

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        if relation.file_format != "delta":
            _unsupported_for_format("set_relation_comment", relation.file_format)
        DeltaFormat.set_relation_comment(self._get_path(relation), comment)

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        if relation.file_format != "delta":
            _unsupported_for_format("set_column_comments", relation.file_format)
        DeltaFormat.set_column_comments(self._get_path(relation), comments)

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        if relation.file_format != "delta":
            return None
        return DeltaFormat.get_relation_comment(self._get_path(relation))

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        if relation.file_format != "delta":
            return {}
        return DeltaFormat.get_column_comments(self._get_path(relation))

    def apply_snapshot_delta(
        self,
        relation: PolarsRelation,
        rows_to_close: pl.DataFrame,
        rows_to_insert: pl.DataFrame,
        scd_id_col: str = "dbt_scd_id",
    ) -> None:
        if rows_to_close.is_empty() and rows_to_insert.is_empty():
            return
        path = self._get_path(relation)
        if relation.file_format == "delta":
            if rows_to_close.is_empty():
                DeltaFormat.append(path, rows_to_insert, False, {})
                return
            DeltaFormat.apply_snapshot(path, rows_to_close, rows_to_insert, scd_id_col)
        else:
            if rows_to_close.is_empty():
                FileFormat.append(
                    path,
                    relation.file_format,
                    rows_to_insert,
                    {},
                    relation.read_options,
                )
                return
            FileFormat.apply_snapshot(
                path,
                relation.file_format,
                rows_to_close,
                rows_to_insert,
                scd_id_col,
                relation.read_options,
            )

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        schema_path = self._schema_path(schema_relation.schema)
        if not schema_path.exists():
            return []

        relations = []
        for p in schema_path.iterdir():
            if p.is_dir() and DeltaTable.is_deltatable(str(p)):
                relations.append(
                    schema_relation.create(
                        database=schema_relation.database,
                        schema=schema_relation.schema,
                        identifier=p.name,
                        type=RelationType.Table,
                        catalog=schema_relation.catalog,
                        file_format="delta",
                    )
                )
            elif p.is_file() and p.suffix.lstrip(".") in _FILE_FORMATS:
                relations.append(
                    schema_relation.create(
                        database=schema_relation.database,
                        schema=schema_relation.schema,
                        identifier=p.stem,
                        type=RelationType.Table,
                        catalog=schema_relation.catalog,
                        file_format=p.suffix.lstrip("."),
                    )
                )
        return relations
