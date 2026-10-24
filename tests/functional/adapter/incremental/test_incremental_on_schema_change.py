import pytest

from dbt.adapters.polars.testing.data_operations import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeAppend,
)


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


@pytest.mark.require_profiles("local")
class TestIncrementalOnSchemaChangeParquet(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+file_format": "parquet"}}


class TestIncrementalOnSchemaChangeAppend(BaseIncrementalOnSchemaChangeAppend):
    pass
