import shutil
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import CatalogConfig
from dbt.adapters.polars.catalogs.formats import FILE_FORMATS
from dbt.adapters.polars.catalogs.storageCatalog import StorageCatalog
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable

logger = AdapterLogger("polars")

_FILE_FORMATS = frozenset(FILE_FORMATS)


@dataclass
class LocalCatalogConfig(CatalogConfig):
    name: str
    type: str
    root: str

    def unique_field(self) -> str:
        return self.root

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "root")


class LocalCatalog(StorageCatalog):
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

    def _get_uri(self, relation: PolarsRelation) -> str:
        return str(self._get_path(relation))

    def _get_storage_options(self, _uri: str) -> None:
        return None

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
