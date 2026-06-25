from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from dbt.adapters.base.relation import BaseRelation

import polars as pl

R = TypeVar("R", bound=BaseRelation)


class CatalogConfig(ABC):
    name: str
    type: str

    @abstractmethod
    def unique_field(self) -> str: ...

    @abstractmethod
    def connection_keys(self) -> tuple[str, ...]: ...


class BaseCatalog(ABC, Generic[R]):
    def __init__(self, config: CatalogConfig):
        self.config = config

    @abstractmethod
    def create_schema(self, relation: R) -> None: ...

    @abstractmethod
    def drop_schema(self, relation: R) -> None: ...

    @abstractmethod
    def list_schemas(self) -> list[str]: ...

    @abstractmethod
    def drop_relation(self, relation: R) -> None: ...

    @abstractmethod
    def get_relation(self, relation: R) -> pl.LazyFrame: ...

    @abstractmethod
    def table_exists(self, relation: R) -> bool: ...

    @abstractmethod
    def write_relation(self, relation: R, df: pl.DataFrame) -> None: ...

    @abstractmethod
    def truncate_relation(self, relation: R) -> None: ...

    @abstractmethod
    def list_relations_without_caching(self, schema_relation: R) -> list[R]: ...

    @abstractmethod
    def append_relation(
        self,
        relation: R,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None: ...

    @abstractmethod
    def merge_relation(
        self,
        relation: R,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[str] | None = None,
    ) -> None: ...

    @abstractmethod
    def delete_matched_relation(
        self, relation: R, df: pl.DataFrame, predicate: str
    ) -> None: ...

    @abstractmethod
    def set_relation_comment(self, relation: R, comment: str) -> None: ...

    @abstractmethod
    def set_column_comments(self, relation: R, comments: dict[str, str]) -> None: ...

    @abstractmethod
    def get_relation_comment(self, relation: R) -> str | None: ...

    @abstractmethod
    def get_column_comments(self, relation: R) -> dict[str, str]: ...

    def expand_column_types(self, goal: R, current: R) -> None:
        # TODO: Test if this function needs to be implemented for the local adapter
        # to enable adding columns in seeds
        pass
