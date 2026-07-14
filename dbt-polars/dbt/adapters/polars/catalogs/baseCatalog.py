import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable

from dbt.adapters.polars.relation import PolarsRelation

import polars as pl


def get_write_options(
    method: Callable,
    model_config: dict,
    *,
    ignore: set[str] | None = None,
    merge: dict[str, dict] | None = None,
) -> dict:
    """Return kwargs for `method` sourced from model_config["write_options"].

    ignore: param names to always exclude (e.g. "mode", "target")
    merge:  params whose value is a dict merged with adapter-supplied values;
            adapter values win on conflict. Included even if user omits the key,
            as long as the adapter dict is non-empty.
    """
    ignore = set() if ignore is None else ignore
    merge = {} if merge is None else merge

    params = frozenset(inspect.signature(method).parameters) - {"self"} - ignore
    write_opts = model_config.get("write_options", {})

    result = {}
    for key in params:
        if key in merge:
            combined = {**(write_opts.get(key) or {}), **merge[key]}
            if combined:
                result[key] = combined
        elif key in write_opts:
            result[key] = write_opts[key]
    return result


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
        data: pl.DataFrame | pl.LazyFrame,
        partition_by: list[str],
        model_config: dict | None = None,
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
        data: pl.DataFrame | pl.LazyFrame,
        allow_schema_evolution: bool = False,
        model_config: dict | None = None,
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
