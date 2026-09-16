from datetime import datetime, timedelta, timezone

import pytest
from dbt.exceptions import CompilationError
from dbt.tests.util import relation_from_name, run_dbt

from tests.conftest import PolarsTestMixin

models__current_ts_sql = """
select {{ dbt.current_timestamp() }} as current_ts_column
"""


def is_aware(dt: datetime) -> bool:
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


def is_naive(dt: datetime) -> bool:
    return not is_aware(dt)


class BaseCurrentTimestamp(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "current_ts.sql": models__current_ts_sql,
        }

    @pytest.fixture(scope="class")
    def current_timestamp(self, project):
        run_dbt(["build"])
        relation = relation_from_name(project.adapter, "current_ts")
        result = project.run_sql(
            f"select current_ts_column from {relation}", fetch="one"
        )
        sql_timestamp = result[0] if result is not None else None
        return sql_timestamp

    def test_current_timestamp_matchutilses_utc(self, current_timestamp):
        sql_timestamp = current_timestamp
        now_utc = self.utcnow_matching_type(sql_timestamp)
        # Plenty of wiggle room if clocks aren't perfectly sync'd, etc
        # The clock on the macos image appears to be a few minutes slow in GHA,
        # causing false negatives
        tolerance = timedelta(minutes=5)
        assert (sql_timestamp > (now_utc - tolerance)) and (
            sql_timestamp < (now_utc + tolerance)
        ), (
            f"SQL timestamp {sql_timestamp.isoformat()} is not close enough to "
            f"Python UTC {now_utc.isoformat()}"
        )

    def utcnow_matching_type(self, dt: datetime) -> datetime:
        """
        Current UTC datetime with the same timezone-awareness (or naiveness) as the
        input.
        """
        return (
            datetime.now(timezone.utc)
            if is_aware(dt)
            else datetime.now(timezone.utc).replace(tzinfo=None)
        )


class BaseCurrentTimestampAware(BaseCurrentTimestamp):
    def test_current_timestamp_type(self, current_timestamp):
        assert is_aware(current_timestamp)


class BaseCurrentTimestampNaive(BaseCurrentTimestamp):
    def test_current_timestamp_type(self, current_timestamp):
        assert is_naive(current_timestamp)


# Use either BaseCurrentTimestampAware or BaseCurrentTimestampNaive but not both
@pytest.mark.xfail(
    strict=True,
    raises=CompilationError,
    reason=(
        "dbt-polars intentionally does not support current_timestamp() via SQL — "
        "Polars' SQL engine has no runtime clock function. Revisit if Polars adds "
        "support."
    ),
)
class TestCurrentTimestamp(BaseCurrentTimestampAware):
    pass
