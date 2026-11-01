import pytest
from dbt.compilation import inject_ctes_into_sql
from dbt.contracts.graph.nodes import InjectedCTE

from dbt.adapters.polars.impl import PolarsAdapter

CTE_SQL = " __dbt__cte__ephemeral as (\nselect 1 as id\n)"
WITH_QUERY = "with a as (select 1) select * from a"
PYTHON_CODE = "def model(dbt, pl):\n    # don't\n    return 1"


def strip_injected_ctes(sql: str) -> str:
    injected = inject_ctes_into_sql(sql, [InjectedCTE(id="ephemeral", sql=CTE_SQL)])
    adapter = PolarsAdapter.__new__(PolarsAdapter)
    return adapter._strip_all_ctes(injected, [{"id": "ephemeral", "sql": CTE_SQL}])


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("select * from __dbt__cte__ephemeral", "select * from __dbt__cte__ephemeral"),
        (WITH_QUERY, WITH_QUERY),
        ("-- setup\n" + WITH_QUERY, WITH_QUERY),
        ("/* setup */ " + WITH_QUERY, WITH_QUERY),
        (
            "-- one\n/* two */\n  with a as (select '-- x') select 1",
            "with a as (select '-- x') select 1",
        ),
    ],
)
def test_strip_all_ctes_removes_injected_ctes(sql, expected):
    assert strip_injected_ctes(sql).split() == expected.split()


@pytest.mark.parametrize(
    "code, expected",
    [
        ("  -- a\n/* b */\nselect 1", "select 1"),
        ("select 1 -- trailing", "select 1 -- trailing"),
        ("select 1; select 2", "select 1; select 2"),
        (PYTHON_CODE, PYTHON_CODE),
        ("", ""),
    ],
)
def test_skip_leading_comments_only_removes_leading_comments(code, expected):
    assert PolarsAdapter.skip_leading_comments(code) == expected
