"""pytest plugin for the dbt-level catalog tests.

Add `pytest_plugins = ["dbt.adapters.polars.testing.plugin"]` to your conftest,
provide a `dbt_profile_target` fixture and subclass the `Base*` classes from
`dbt.adapters.polars.testing.catalog` and `dbt.adapters.polars.testing.data_operations`.
"""

try:
    import dbt.tests.util
except ImportError as exc:
    raise ImportError(
        "The dbt-level catalog tests require dbt-tests-adapter: "
        "pip install dbt-tests-adapter"
    ) from exc

import pytest

from dbt.adapters.polars.testing.utils import (
    polars_check_relations_equal,
    resolve_relation,
)

dbt.tests.util.check_relations_equal = polars_check_relations_equal
dbt.tests.util.relation_from_name = resolve_relation

pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def polars_project_config() -> dict:
    return {"models": {"+materialized": "table"}}
