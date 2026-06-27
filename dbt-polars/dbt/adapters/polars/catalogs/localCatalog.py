from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.formats import FORMAT_REGISTRY, TableFormatStr
from dbt.adapters.polars.relation import PolarsRelation, TableFormat
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("polars")


@dataclass
class LocalCatalogConfig(CatalogConfig):
    name: str
    type: str

    root: str
    table_format: TableFormatStr = "delta"  # default for new tables

    def unique_field(self) -> str:
        return self.root

    def connection_keys(self) -> tuple[str, str]:
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
        # Instantiate ALL registered formats (local paths need no storage_options).
        # This enables per-table format detection during listing — a schema may
        # contain Delta tables alongside Parquet or CSV tables written by other tools.
        self._formats = {k: cls() for k, cls in FORMAT_REGISTRY.items()}
        if config.table_format not in self._formats:
            raise DbtRuntimeError(
                f"Unknown table_format {config.table_format!r}. "
                f"Valid options: {list(FORMAT_REGISTRY)}"
            )
        self._format = self._formats[config.table_format]  # default for new tables

    # ------------------------------------------------------------------
    # Storage topology
    # ------------------------------------------------------------------

    def _schema_path(self, schema: str) -> Path:
        return Path(self.config.root) / schema

    def _relation_uri(self, relation: PolarsRelation) -> str:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
        return str(self._schema_path(relation.schema) / relation.identifier)

    # ------------------------------------------------------------------
    # Storage topology
    # ------------------------------------------------------------------

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

    def drop_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Dropping table if exists {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        shutil.rmtree(self._relation_uri(relation), ignore_errors=True)

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        schema_path = self._schema_path(schema_relation.schema)
        if not schema_path.exists():
            return []
        relations: list[PolarsRelation] = []
        for p in schema_path.iterdir():
            if not p.is_dir():
                continue
            if not next(p.iterdir(), None):
                fmt = TableFormat.empty
            else:
                # Probe all registered formats to detect which one this table uses.
                # This allows a single schema to hold mixed-format tables (e.g. some
                # Delta, some Parquet) written by different tools or migrations.
                detected = next(
                    (
                        f.table_format
                        for f in self._formats.values()
                        if f.is_table(str(p))
                    ),
                    None,
                )
                if detected is None:
                    raise DbtRuntimeError(
                        f"Unable to identify format of table {p}. "
                        f"Registered formats: {list(self._formats)}"
                    )
                fmt = detected
            relations.append(
                schema_relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=p.name,
                    type=RelationType.Table,
                    format=fmt,
                    catalog=schema_relation.catalog,
                )
            )
        return relations
