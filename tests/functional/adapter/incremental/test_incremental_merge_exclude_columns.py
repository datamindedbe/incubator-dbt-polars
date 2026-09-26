from dbt.adapters.polars.testing.data_operations import (
    BaseMergeColumnsMutuallyExclusive,
    BaseMergeExcludeColumns,
    BaseMergeUpdateColumns,
)


class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    pass


class TestMergeUpdateColumns(BaseMergeUpdateColumns):
    pass


class TestMergeColumnsMutuallyExclusive(BaseMergeColumnsMutuallyExclusive):
    pass
