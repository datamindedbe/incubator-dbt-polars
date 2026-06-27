from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import HasQuoting, RelationConfig


class TableFormat(str, Enum):
    empty = "empty"
    delta = "delta"
    parquet = "parquet"
    csv = "csv"


@dataclass(frozen=True, eq=False, repr=False)
class PolarsRelation(BaseRelation):
    format: TableFormat = TableFormat.empty

    @classmethod
    def create_from(
        cls: type[PolarsRelation],
        quoting: HasQuoting,
        relation_config: RelationConfig,
        **kwargs: Any,
    ) -> PolarsRelation:
        # This override allows using the catalog as an alias for the database
        # property on all relations

        relation = super().create_from(quoting, relation_config, **kwargs)
        config = relation_config.config

        if not relation.catalog:
            catalog = (
                config.get("catalog") if config else None
            ) or relation_config.database
            if catalog:
                relation = relation.replace(catalog=catalog)

        if config:
            fmt_str = config.get("table_format")
            if fmt_str:
                try:
                    relation = relation.replace(format=TableFormat(fmt_str))
                except ValueError:
                    pass  # unrecognised value; catalog default applies

        return relation
