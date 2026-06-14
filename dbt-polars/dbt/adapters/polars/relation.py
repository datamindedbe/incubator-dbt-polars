from dataclasses import dataclass
from enum import Enum

from dbt.adapters.base import BaseRelation


class TableFormat(str, Enum):
    empty = "empty"
    delta = "delta"


@dataclass(frozen=True, eq=False, repr=False)
class PolarsRelation(BaseRelation):
    format: TableFormat = TableFormat.delta
