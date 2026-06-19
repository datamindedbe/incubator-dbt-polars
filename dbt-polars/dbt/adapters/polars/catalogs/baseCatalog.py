import abc
from abc import abstractmethod
from typing import Optional

import polars as pl

from dbt.adapters.base import BaseRelation


class CatalogConfig(abc.ABC):
    name: str
    type: str

    @abc.abstractmethod
    def unique_field(self) -> str: ...

    @abc.abstractmethod
    def connection_keys(self) -> str: ...


class BaseCatalog(abc.ABC):

    def __init__(self, config: CatalogConfig):
        self.config = config

    @abstractmethod
    def create_schema(self, relation: BaseRelation) -> None: ...

    @abstractmethod
    def drop_schema(self, relation: BaseRelation) -> None: ...

    @abstractmethod
    def list_schemas(self) -> list[str]: ...

    @abstractmethod
    def drop_relation(self, relation: BaseRelation) -> None: ...

    @abstractmethod
    def get_relation(self, relation: BaseRelation) -> pl.LazyFrame: ...

    @abstractmethod
    def table_exists(self, relation: BaseRelation) -> bool: ...

    @abstractmethod
    def write_relation(self, relation: BaseRelation, df: pl.DataFrame) -> None: ...

    @abstractmethod
    def truncate_relation(self, relation: BaseRelation) -> None: ...

    @abstractmethod
    def list_relations_without_caching(
        self, schema_relation: BaseRelation
    ) -> list[BaseRelation]: ...

    @abstractmethod
    def append_relation(self, relation: BaseRelation, df: pl.DataFrame, allow_schema_evolution: bool = False) -> None: ...

    @abstractmethod
    def merge_relation(
        self,
        relation: BaseRelation,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[str] | None = None,
    ) -> None: ...

    @abstractmethod
    def delete_matched_relation(self, relation: BaseRelation, df: pl.DataFrame, predicate: str) -> None: ...

    @abstractmethod
    def set_relation_comment(self, relation: BaseRelation, comment: str) -> None: ...

    @abstractmethod
    def set_column_comments(self, relation: BaseRelation, comments: dict[str, str]) -> None: ...

    @abstractmethod
    def get_relation_comment(self, relation: BaseRelation) -> Optional[str]: ...

    @abstractmethod
    def get_column_comments(self, relation: BaseRelation) -> dict[str, str]: ...

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        # TODO: Test if this function needs to be implemented for the local adapter
        # to enable adding columns in seeds
        pass
