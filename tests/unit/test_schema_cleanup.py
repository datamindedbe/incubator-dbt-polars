from datetime import datetime, timedelta

import pytest

from dbt.adapters.polars.testing.mixin import (
    is_removable_test_schema,
    new_schema_prefix,
    utc_now,
)

RUN_ID = "3f9a1c2e" + "0" * 24


def timestamp(age: timedelta) -> str:
    created = utc_now() - age - datetime(1970, 1, 1)
    return str(int(created.total_seconds() * 1e6))


@pytest.fixture(autouse=True)
def worker(monkeypatch):
    monkeypatch.setenv("PYTEST_XDIST_TESTRUNUID", RUN_ID)
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw4")


def test_new_prefix_carries_run_and_worker():
    assert new_schema_prefix().startswith("test_3f9a1c2e_gw4_")


def test_own_schemas_are_removable():
    schema = f"{new_schema_prefix()}_test_seed"

    assert is_removable_test_schema(schema)
    assert is_removable_test_schema(f"{schema}_dbt_test__audit")


@pytest.mark.parametrize(
    "owner", ["test_3f9a1c2e_gw5_", "test_aaaaaaaa_gw4_"], ids=["worker", "run"]
)
def test_recent_schemas_of_others_are_kept(owner):
    schema = f"{owner}{timestamp(timedelta(minutes=5))}0042_test_seed"

    assert not is_removable_test_schema(schema)


@pytest.mark.parametrize(
    "owner", ["test_aaaaaaaa_gw5_", "test"], ids=["tagged", "untagged"]
)
def test_stale_test_schemas_are_removable(owner):
    schema = f"{owner}{timestamp(timedelta(hours=25))}0042_test_seed"

    assert is_removable_test_schema(schema)


def test_recent_untagged_schemas_are_kept():
    schema = f"test{timestamp(timedelta(minutes=5))}0042_local_test_seed"

    assert not is_removable_test_schema(schema)


@pytest.mark.parametrize("schema", ["analytics", "test_manual", ""])
def test_other_schemas_are_kept(schema):
    assert not is_removable_test_schema(schema)
