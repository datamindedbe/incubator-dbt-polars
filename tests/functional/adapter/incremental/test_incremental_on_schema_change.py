from dbt.adapters.polars.testing.data_operations import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeAppend,
)


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


class TestIncrementalOnSchemaChangeAppend(BaseIncrementalOnSchemaChangeAppend):
    pass
