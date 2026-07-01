from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import HasQuoting, RelationConfig


@dataclass(frozen=True, eq=False, repr=False)
class PolarsRelation(BaseRelation):
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

        if not relation.catalog:
            config = relation_config.config
            catalog = (
                config.get("catalog") if config else None
            ) or relation_config.database
            if catalog:
                return relation.replace(catalog=catalog)
        return relation
