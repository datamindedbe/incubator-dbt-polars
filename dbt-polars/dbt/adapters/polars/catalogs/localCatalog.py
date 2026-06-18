import shutil
from pathlib import Path, PosixPath
from dataclasses import dataclass
from dbt.adapters.base import BaseRelation
from dbt.adapters.exceptions.connection import DbtRuntimeError
from dbt.adapters.events.logging import AdapterLogger

import polars as pl
from deltalake import DeltaTable

from dbt.adapters.base.relation import RelationType
from dbt.adapters.polars.catalogs.baseCatalog import CatalogConfig, BaseCatalog
from dbt.adapters.polars.relation import PolarsRelation, TableFormat

logger = AdapterLogger("polars")


@dataclass
class LocalCatalogConfig(CatalogConfig):
    name: str
    type: str
    root: str

    def unique_field(self) -> str:
        return self.root_folder

    def connection_keys(self) -> tuple:
        return ("name", "root")


def _identify_table_format(path: Path) -> TableFormat:
    if not next(path.iterdir(), None):
        return TableFormat.empty

    if DeltaTable.is_deltatable(str(path)):
        return TableFormat.delta

    raise DbtRuntimeError(f"Unable to identify format of table {path}.")


class LocalCatalog(BaseCatalog):
    config: LocalCatalogConfig

    def __init__(self, config: LocalCatalogConfig):
        super().__init__(config)

    def _schema_path(self, schema: str) -> PosixPath:
        return Path(self.config.root) / schema

    def _relation_path(self, relation: PolarsRelation) -> Path:
        return self._schema_path(relation.schema) / relation.identifier

    def create_schema(self, relation: PolarsRelation) -> None:
        logger.debug(f"Creating schema {relation.catalog}/{relation.schema}")
        self._schema_path(relation.schema).mkdir(parents=True, exist_ok=True)

    def drop_schema(self, relation: PolarsRelation) -> None:
        logger.debug(f"Dropping schema {relation.catalog}/{relation.schema}")
        shutil.rmtree(self._schema_path(relation.schema), ignore_errors=True)

    def list_schemas(self) -> list[str]:
        root = Path(self.config.root)
        if not root.exists():
            return []
        return [p.name for p in root.iterdir() if p.is_dir()]

    def table_exists(self, relation: PolarsRelation) -> bool:
        path = self._relation_path(relation)
        result = path.is_dir()
        return result

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return pl.scan_delta(str(self._relation_path(relation)))

    def write_relation(self, relation: PolarsRelation, df: pl.DataFrame) -> None:
        logger.debug(
            f"Writing table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        df.write_delta(str(self._relation_path(relation)), mode="overwrite")

    def drop_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Dropping table if exists {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        shutil.rmtree(self._relation_path(relation), ignore_errors=True)

    def truncate_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Truncating table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        DeltaTable(str(self._relation_path(relation))).delete()

    def append_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None:
        delta_write_options = (
            {"schema_mode": "merge"} if allow_schema_evolution else None
        )
        df.write_delta(
            str(self._relation_path(relation)),
            mode="append",
            delta_write_options=delta_write_options,
        )

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[str] | None = None,
    ) -> None:
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
        self, relation: PolarsRelation, df: pl.DataFrame, predicate: str
    ) -> None:
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

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        schema_path = self._schema_path(schema_relation.schema)
        if not schema_path.exists():
            return []
        return [
            schema_relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=p.name,
                type=RelationType.Table,
                format=_identify_table_format(p),
            )
            for p in schema_path.iterdir()
            if p.is_dir()
        ]
