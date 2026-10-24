from types import SimpleNamespace

import pytest

from dbt.adapters.polars.catalogs import icebergCatalog
from dbt.adapters.polars.catalogs.icebergCatalog import (
    IcebergCatalog,
    IcebergCatalogConfig,
)

exceptions = pytest.importorskip("pyiceberg.exceptions")

IDENTIFIER = ("SCHEMA", "TABLE")


class FakeCatalog:
    def load_table(self, identifier):
        return SimpleNamespace(identifier=identifier)


def make_catalog(monkeypatch, sleeps: list | None = None) -> IcebergCatalog:
    monkeypatch.setattr(
        icebergCatalog.time,
        "sleep",
        lambda seconds: sleeps.append(seconds) if sleeps is not None else None,
    )
    catalog = IcebergCatalog(
        IcebergCatalogConfig(name="test", type="iceberg", schema="s"), ""
    )
    catalog._catalog_instance = FakeCatalog()
    return catalog


def failing_write(errors: list):
    """A write that raises the given errors in turn, then succeeds."""
    calls = []

    def write(tbl):
        calls.append(tbl)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]

    return write, calls


def test_rejected_data_commit_is_not_retried(monkeypatch):
    catalog = make_catalog(monkeypatch)
    write, calls = failing_write([exceptions.CommitFailedException("conflict")])

    with pytest.raises(exceptions.CommitFailedException):
        catalog._commit(IDENTIFIER, write)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "error",
    [
        exceptions.CommitFailedException("conflict"),
        exceptions.CommitStateUnknownException("unknown"),
    ],
)
def test_metadata_commit_is_retried(monkeypatch, error):
    catalog = make_catalog(monkeypatch)
    write, calls = failing_write([error])

    catalog._commit_metadata(IDENTIFIER, write)

    assert len(calls) == 2


def test_metadata_commit_gives_up_after_the_last_attempt(monkeypatch):
    catalog = make_catalog(monkeypatch)
    rejected = exceptions.CommitFailedException("conflict")
    write, calls = failing_write([rejected] * icebergCatalog.COMMIT_ATTEMPTS)

    with pytest.raises(exceptions.CommitFailedException):
        catalog._commit_metadata(IDENTIFIER, write)
    assert len(calls) == icebergCatalog.COMMIT_ATTEMPTS


def test_waits_for_the_minimum_interval_between_commits(monkeypatch):
    sleeps: list[float] = []
    catalog = make_catalog(monkeypatch, sleeps)
    catalog.min_commit_interval = 2
    write, _ = failing_write([])

    catalog._commit(IDENTIFIER, write)
    assert sleeps == []
    catalog._commit(IDENTIFIER, write)
    assert len(sleeps) == 1 and 0 < sleeps[0] <= 2
    catalog._commit(("SCHEMA", "OTHER"), write)
    assert len(sleeps) == 1
