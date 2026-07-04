from abc import ABC, abstractmethod

from dbt.adapters.polars.relation import PolarsRelation

import polars as pl


class CatalogConfig(ABC):
    name: str
    type: str

    @abstractmethod
    def unique_field(self) -> str: ...

    @abstractmethod
    def connection_keys(self) -> tuple[str, ...]: ...


class BaseCatalog(ABC):
    """Abstract base class for Polars catalog backends.

    Subclasses implement schema and relation management for a specific storage target.
    """

    def __init__(self, config: CatalogConfig):
        self.config = config

    @abstractmethod
    def create_schema(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def drop_schema(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def list_schemas(self) -> list[str]: ...

    @abstractmethod
    def drop_relation(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame: ...

    @abstractmethod
    def table_exists(self, relation: PolarsRelation) -> bool: ...

    @abstractmethod
    def write_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        partition_by: list[str],
    ) -> None: ...

    @abstractmethod
    def get_partition_columns(self, relation: PolarsRelation) -> list[str]: ...

    @abstractmethod
    def truncate_relation(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]: ...

    @abstractmethod
    def append_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None: ...

    @abstractmethod
    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        except_cols: list[str] | None = None,
        incremental_predicates: list[str] | None = None,
        allow_schema_evolution: bool = False,
    ) -> None: ...

    @abstractmethod
    def delete_matched_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        keys: list[str],
        incremental_predicates: list[str] | None = None,
    ) -> None: ...

    @abstractmethod
    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None: ...

    @abstractmethod
    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[str, str]
    ) -> None: ...

    @abstractmethod
    def get_relation_comment(self, relation: PolarsRelation) -> str | None: ...

    @abstractmethod
    def get_column_comments(self, relation: PolarsRelation) -> dict[str, str]: ...

    def expand_column_types(
        self, goal: PolarsRelation, current: PolarsRelation
    ) -> None:
        # TODO: Test if this function needs to be implemented for the local adapter
        # to enable adding columns in seeds
        pass
