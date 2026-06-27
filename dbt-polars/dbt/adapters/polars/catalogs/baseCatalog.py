from __future__ import annotations

from abc import ABC, abstractmethod

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.formats.baseFormat import BaseFormat
from dbt.adapters.polars.relation import PolarsRelation, TableFormat
from dbt.adapters.polars.types import TableColumnName

import polars as pl

logger = AdapterLogger("polars")


class CatalogConfig(ABC):
    """Serialisable configuration for a catalog backend.

    Subclasses are plain dataclasses whose fields map 1-to-1 to the keys
    in a ``profiles.yml`` catalog entry.
    """

    name: str  # dbt catalog identifier (used in model paths and refs)
    type: str  # registry key — selects the concrete CatalogConfig subclass

    @abstractmethod
    def unique_field(self) -> str:
        """A single string that uniquely identifies this catalog instance.

        Used by dbt to compare two credentials and decide whether they share
        a connection.  Typically the storage root or bucket URI.
        """
        ...

    @abstractmethod
    def connection_keys(self) -> tuple[str, ...]:
        """Names of config fields shown in ``dbt debug`` connection info."""
        ...


class BaseCatalog(ABC):
    """Abstract base class for Polars catalog backends.

    Provides ready-made format-delegating bodies for every catalog operation.
    Subclasses only need to implement the storage-topology methods (where data
    lives) — not the data I/O (how data is read/written).

    **Format selection per relation**

    Each subclass ``__init__`` must populate two attributes:

    ``_formats``
        A dict mapping every registered format key (``"delta"``, ``"parquet"``,
        etc.) to a fully-constructed :class:`~BaseFormat` instance with the
        correct ``storage_options`` for this backend.

    ``_format``
        The *default* format instance, used when creating new tables or when
        an existing relation's format tag is unknown.  Typically
        ``self._formats[config.table_format]``.

    :meth:`_format_for` then resolves the right format for every operation by
    inspecting ``relation.format``.  This means a single catalog directory can
    hold Delta tables alongside Parquet tables — they are read and written with
    the correct engine automatically.

    **Comment storage**

    Comment calls are forwarded directly to the format.  :class:`~BaseFormat`
    provides no-op defaults; formats with native metadata storage (e.g. Delta
    Lake's transaction log) override them.  Catalog subclasses can override
    ``set_relation_comment`` for storage-level metadata (S3 tags, GCS metadata,
    Azure blob properties, etc.).
    """

    #: Default format for new tables; set to ``_formats[config.table_format]``.
    _format: BaseFormat
    #: All format instances keyed by registry string; populated in __init__.
    _formats: dict[str, BaseFormat]

    def __init__(self, config: CatalogConfig) -> None:
        self.config = config

    # ------------------------------------------------------------------
    # Contract: storage topology (must be implemented by each backend)
    # ------------------------------------------------------------------

    @abstractmethod
    def _relation_uri(self, relation: PolarsRelation) -> str:
        """Return the fully-qualified URI (or local path string) for *relation*."""
        ...

    @abstractmethod
    def create_schema(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def drop_schema(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def list_schemas(self) -> list[str]: ...

    @abstractmethod
    def drop_relation(self, relation: PolarsRelation) -> None: ...

    @abstractmethod
    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]: ...

    # ------------------------------------------------------------------
    # Format resolution
    # ------------------------------------------------------------------

    def _format_for(self, relation: PolarsRelation) -> BaseFormat:
        """Return the :class:`~BaseFormat` for *relation*.

        Uses the ``format`` tag stored on the relation (set during
        ``list_relations_without_caching``).  Falls back to ``_format`` when
        the relation is new (``TableFormat.empty``) or the tag is unknown.

        This is the single place where per-table format routing happens —
        all data-I/O and comment methods call it rather than accessing
        ``self._format`` directly.
        """
        fmt = relation.format
        if fmt is None or fmt == TableFormat.empty:
            return self._format
        return self._formats.get(fmt.value, self._format)

    # ------------------------------------------------------------------
    # Format-delegating defaults (override only for backend-specific reasons)
    # ------------------------------------------------------------------

    def table_exists(self, relation: PolarsRelation) -> bool:
        return self._format_for(relation).is_table(self._relation_uri(relation))

    def get_relation(self, relation: PolarsRelation) -> pl.LazyFrame:
        return self._format_for(relation).scan(self._relation_uri(relation))

    def write_relation(self, relation: PolarsRelation, df: pl.DataFrame) -> None:
        logger.debug(
            f"Writing table {relation.catalog}/{relation.schema}/{relation.identifier}"
        )
        self._format_for(relation).write(self._relation_uri(relation), df)

    def truncate_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Truncating table {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        self._format_for(relation).truncate(self._relation_uri(relation))

    def append_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        allow_schema_evolution: bool = False,
    ) -> None:
        self._format_for(relation).append(
            self._relation_uri(relation), df, allow_schema_evolution
        )

    def merge_relation(
        self,
        relation: PolarsRelation,
        df: pl.DataFrame,
        predicate: str,
        except_cols: list[str] | None = None,
    ) -> None:
        self._format_for(relation).merge(
            self._relation_uri(relation), df, predicate, except_cols
        )

    def delete_matched_relation(
        self, relation: PolarsRelation, df: pl.DataFrame, predicate: str
    ) -> None:
        self._format_for(relation).delete_matched(
            self._relation_uri(relation), df, predicate
        )

    # ------------------------------------------------------------------
    # Comment storage
    # ------------------------------------------------------------------

    def set_relation_comment(self, relation: PolarsRelation, comment: str) -> None:
        """Persist a description for *relation*.

        Delegates to the format's comment storage.  Delta Lake stores
        descriptions in its transaction log; flat-file formats are no-ops.
        Override in a catalog subclass for storage-level persistence
        (S3 object tags, GCS metadata, Azure blob properties, etc.).
        """
        self._format_for(relation).set_table_comment(
            self._relation_uri(relation), comment
        )

    def set_column_comments(
        self, relation: PolarsRelation, comments: dict[TableColumnName, str]
    ) -> None:
        self._format_for(relation).set_column_comments(
            self._relation_uri(relation), comments
        )

    def get_relation_comment(self, relation: PolarsRelation) -> str | None:
        return self._format_for(relation).get_table_comment(
            self._relation_uri(relation)
        )

    def get_column_comments(
        self, relation: PolarsRelation
    ) -> dict[TableColumnName, str]:
        return self._format_for(relation).get_column_comments(
            self._relation_uri(relation)
        )

    def expand_column_types(
        self, goal: PolarsRelation, current: PolarsRelation
    ) -> None:
        # TODO: Test if this function needs to be implemented for the local adapter
        # to enable adding columns in seeds
        pass
