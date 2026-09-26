from dbt.adapters.polars.testing.data_operations import (
    BaseIncrementalAppend,
    BaseIncrementalDeleteInsert,
    BaseIncrementalFailOnTypeChange,
    BaseIncrementalFullRefresh,
    BaseIncrementalMerge,
)


class TestIncrementalAppend(BaseIncrementalAppend):
    pass


class TestIncrementalFullRefresh(BaseIncrementalFullRefresh):
    pass


class TestIncrementalMerge(BaseIncrementalMerge):
    pass


class TestIncrementalFailOnTypeChange(BaseIncrementalFailOnTypeChange):
    pass


class TestIncrementalDeleteInsert(BaseIncrementalDeleteInsert):
    pass
