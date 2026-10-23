from types import SimpleNamespace

import pytest

from dbt.adapters.polars.catalogs import icebergCatalog
from dbt.adapters.polars.catalogs.icebergCatalog import (
    IcebergCatalog,
    IcebergCatalogConfig,
)

exceptions = pytest.importorskip("pyiceberg.exceptions")

IDENTIFIER = ("SCHEMA", "TABLE")


def snapshot(snapshot_id: int, parent_id: int | None):
    return SimpleNamespace(snapshot_id=snapshot_id, parent_snapshot_id=parent_id)


class FakeCatalog:
    """Each load_table returns the next table state in `states`."""

    def __init__(self, states: list) -> None:
        self.states = states
        self.loads = 0

    def load_table(self, identifier):
        state = self.states[min(self.loads, len(self.states) - 1)]
        self.loads += 1
        return SimpleNamespace(
            metadata=SimpleNamespace(
                current_snapshot_id=state.snapshot_id if state else None
            ),
            current_snapshot=lambda: state,
        )


def make_catalog(
    monkeypatch, states: list, sleeps: list | None = None
) -> IcebergCatalog:
    monkeypatch.setattr(
        icebergCatalog.time,
        "sleep",
        lambda seconds: sleeps.append(seconds) if sleeps is not None else None,
    )
    catalog = IcebergCatalog(
        IcebergCatalogConfig(name="test", type="iceberg", schema="s"), ""
    )
    catalog._catalog_instance = FakeCatalog(states)
    return catalog


def failing_write(errors: list):
    """A write that raises the given errors in turn, then succeeds."""
    calls = []

    def write(tbl):
        calls.append(tbl)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]

    return write, calls


def test_rejected_commit_is_retried(monkeypatch):
    catalog = make_catalog(monkeypatch, [snapshot(1, None)])
    write, calls = failing_write([exceptions.CommitFailedException("conflict")])

    catalog._commit(IDENTIFIER, write)

    assert len(calls) == 2


def test_unknown_commit_that_was_applied_is_not_retried(monkeypatch):
    # Started at snapshot 1, after the unknown result the table is at 2 on top of 1
    catalog = make_catalog(monkeypatch, [snapshot(1, None), snapshot(2, 1)])
    write, calls = failing_write([exceptions.CommitStateUnknownException("unknown")])

    catalog._commit(IDENTIFIER, write)

    assert len(calls) == 1


def test_unknown_commit_that_was_not_applied_is_retried(monkeypatch):
    catalog = make_catalog(monkeypatch, [snapshot(1, None)])
    write, calls = failing_write([exceptions.CommitStateUnknownException("unknown")])

    catalog._commit(IDENTIFIER, write)

    assert len(calls) == 2


def test_waits_for_the_minimum_interval_between_commits(monkeypatch):
    sleeps: list[float] = []
    catalog = make_catalog(monkeypatch, [snapshot(1, None)], sleeps)
    catalog.min_commit_interval = 2
    write, calls = failing_write([])

    catalog._commit(IDENTIFIER, write)
    assert sleeps == []
    catalog._commit(IDENTIFIER, write)
    assert len(sleeps) == 1 and 0 < sleeps[0] <= 2
    catalog._commit(("SCHEMA", "OTHER"), write)
    assert len(sleeps) == 1


def test_gives_up_after_the_last_attempt(monkeypatch):
    catalog = make_catalog(monkeypatch, [snapshot(1, None)])
    rejected = exceptions.CommitFailedException("conflict")
    write, calls = failing_write([rejected] * icebergCatalog.COMMIT_ATTEMPTS)

    with pytest.raises(exceptions.CommitFailedException):
        catalog._commit(IDENTIFIER, write)
    assert len(calls) == icebergCatalog.COMMIT_ATTEMPTS
