from dbt.adapters.polars.testing.catalog import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)


class TestIncremental(BaseIncremental):
    pass


class TestIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass
