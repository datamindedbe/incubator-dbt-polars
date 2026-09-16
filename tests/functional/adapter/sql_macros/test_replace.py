import pytest

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_replace import (
    models__test_replace_sql,
    models__test_replace_yml,
    seeds__data_replace_csv,
)


class BaseReplace(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_replace.csv": seeds__data_replace_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                models__test_replace_sql, "replace"
            ),
        }


# Polars' str.replace_all rejects an expression pattern/replacement pair whose
# per-row lengths vary within a single execution batch ("dynamic pattern length
# ... not supported yet"). This fixture's seed has both a 1-char and a 7-char
# search string; delta and parquet happen to keep the two rows in separate
# batches, but csv and ndjson reliably scan them together and hit the error.
@pytest.mark.skip_configs("csv", "ndjson")
class TestReplace(BaseReplace):
    pass
