import shutil
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import (
    BaseCatalog,
    CatalogConfig,
    get_write_options,
)
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable

import polars as pl

logger = AdapterLogger("polars")


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

    def __init__(self, config: LocalCatalogConfig):
        absolute_root = str(Path(config.root).resolve())
        if " " in absolute_root:
            raise DbtRuntimeError(
                f"LocalCatalog root resolves to '{absolute_root}', which contains "
                "a space. polars' Delta scanner (pl.scan_delta) cannot read tables "
                "whose path contains a space, see "
                "https://github.com/pola-rs/polars/issues/20944. Use a root path "
                "that resolves to an absolute path without spaces."
            )
        super().__init__(config)

    def _schema_path(self, schema: str) -> Path:
        return Path(self.config.root) / schema

    def _relation_path(self, relation: PolarsRelation) -> Path:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
        return self._schema_path(relation.schema) / relation.identifier

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
        root = Path(self.config.root)
        if not root.exists():
            return []
        return [p.name for p in root.iterdir() if p.is_dir()]

    def table_exists(self, relation: PolarsRelation) -> bool:
        return DeltaTable.is_deltatable(str(self._relation_path(relation)))

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return pl.scan_delta(str(self._relation_path(relation)))

    def get_partition_columns(self, relation: PolarsRelation) -> list[str]:
        dt = DeltaTable(str(self._relation_path(relation)))
        return dt.metadata().partition_columns

    def write_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        logger.debug(
            f"Writing table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        path = str(self._relation_path(relation))

        adapter_delta_opts: dict = {"schema_mode": "overwrite"}
        if partition_by:
            adapter_delta_opts["partition_by"] = partition_by
        write_mode = model_config.get("write_mode", "lazy")
        if isinstance(data, pl.LazyFrame) and write_mode == "lazy":
            kwargs = get_write_options(
                pl.LazyFrame.sink_delta,
                model_config,
                ignore={"mode", "target"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            data.sink_delta(path, mode="overwrite", **kwargs)
        else:
            df = data.collect() if isinstance(data, pl.LazyFrame) else data
            kwargs = get_write_options(
                pl.DataFrame.write_delta,
                model_config,
                ignore={"mode", "target"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            df.write_delta(path, mode="overwrite", **kwargs)

    def drop_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Dropping table if exists {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        shutil.rmtree(self._relation_path(relation), ignore_errors=True)

    def truncate_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Truncating table {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        DeltaTable(str(self._relation_path(relation))).delete()

    def append_relation(
        self,
        relation: PolarsRelation,
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
    ) -> None:
        model_config = model_config or {}
        path = str(self._relation_path(relation))
        adapter_delta_opts = {"schema_mode": "merge"} if allow_schema_evolution else {}
        write_mode = model_config.get("write_mode", "lazy")
        if isinstance(data, pl.LazyFrame) and write_mode == "lazy":
            kwargs = get_write_options(
                pl.LazyFrame.sink_delta,
                model_config,
                ignore={"mode", "target"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            data.sink_delta(path, mode="append", **kwargs)
        else:
            df = data.collect() if isinstance(data, pl.LazyFrame) else data
            kwargs = get_write_options(
                pl.DataFrame.write_delta,
                model_config,
                ignore={"mode", "target"},
                merge={"delta_write_options": adapter_delta_opts},
            )
            df.write_delta(path, mode="append", **kwargs)

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None = None,
        incremental_predicates: list[str] | None = None,
        allow_schema_evolution: bool = False,
    ) -> None:
        predicate = " AND ".join(
            f"DBT_INTERNAL_SOURCE.{k} = DBT_INTERNAL_DEST.{k}" for k in keys
        )
        if incremental_predicates:
            predicate += " AND " + " AND ".join(incremental_predicates)
        dt = DeltaTable(str(self._relation_path(relation)))
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

    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
    ) -> None:
        predicate = " AND ".join(
            f"DBT_INTERNAL_SOURCE.{k} = DBT_INTERNAL_DEST.{k}" for k in keys
        )
        if incremental_predicates:
            predicate += " AND " + " AND ".join(incremental_predicates)
        dt = DeltaTable(str(self._relation_path(relation)))
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

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        DeltaTable(str(self._relation_path(relation))).alter.set_table_description(
            comment
        )

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None:
        dt = DeltaTable(str(self._relation_path(relation)))
        for column, comment in comments.items():
            dt.alter.set_column_metadata(column, {"comment": comment})

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        return DeltaTable(str(self._relation_path(relation))).metadata().description

    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]:
        dt = DeltaTable(str(self._relation_path(relation)))
        return {
            field.name: field.metadata["comment"]
            for field in dt.schema().fields
            if field.metadata.get("comment")
        }

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        schema_path = self._schema_path(schema_relation.schema)
        if not schema_path.exists():
            return []
        return [
            schema_relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=p.name,
                type=RelationType.Table,
                catalog=schema_relation.catalog,
            )
            for p in schema_path.iterdir()
            if p.is_dir() and DeltaTable.is_deltatable(str(p))
        ]
