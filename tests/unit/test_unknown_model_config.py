from types import SimpleNamespace

import pytest
from dbt_common.exceptions import CompilationError

from dbt.adapters.polars.impl import PolarsAdapter


def raise_on_unknown_model_config(catalog_keys: frozenset[str], config: dict) -> None:
    catalog = SimpleNamespace(model_config_keys=catalog_keys)
    adapter = SimpleNamespace(get_storage_catalog=lambda name: catalog)
    PolarsAdapter.polars_raise_on_unknown_model_config(
        adapter,  # type: ignore[arg-type]
        SimpleNamespace(catalog="primary"),  # type: ignore[arg-type]
        {"name": "my_model", "config": config},
    )


def test_dbt_core_and_polars_keys_pass():
    raise_on_unknown_model_config(
        frozenset(), {"materialized": "table", "post-hook": [], "partition_by": "k"}
    )


def test_unknown_key_raises():
    with pytest.raises(CompilationError, match="merge_clauses"):
        raise_on_unknown_model_config(frozenset(), {"merge_clauses": {}})


def test_catalog_model_config_keys_pass():
    raise_on_unknown_model_config(frozenset({"my_key"}), {"my_key": 1})


def test_catalog_model_config_keys_do_not_allow_other_keys():
    with pytest.raises(CompilationError, match="other_key"):
        raise_on_unknown_model_config(
            frozenset({"my_key"}), {"my_key": 1, "other_key": 2}
        )
