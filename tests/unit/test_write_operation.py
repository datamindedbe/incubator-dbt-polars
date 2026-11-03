from contextlib import contextmanager

import polars as pl
import pytest
from dbt.adapters.contracts.relation import RelationType
from dbt_polars_catalog_local import LocalCatalog
from dbt_polars_catalog_local.catalog import LocalCatalogConfig

from dbt.adapters.polars.catalogs import PolarsRelation, WriteOperation


class RecordingCatalog(LocalCatalog):
    def __init__(self, config, project_root):
        super().__init__(config, project_root)
        self.events: list[tuple[str, WriteOperation]] = []

    @contextmanager
    def write_operation(self, relation, operation):
        self.events.append(("before", operation))
        yield
        self.events.append(("after", operation))


@pytest.fixture
def catalog(tmp_path):
    config = LocalCatalogConfig(
        name="local", type="local", schema="main", root=str(tmp_path)
    )
    return RecordingCatalog(config, str(tmp_path))


@pytest.fixture
def relation():
    return PolarsRelation.create(
        database="local",
        schema="main",
        identifier="table",
        type=RelationType.Table,
        catalog="local",
    )


DATA = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})


def written(catalog, relation):
    catalog.create_schema(relation)
    catalog.write_relation(relation, DATA, [])
    catalog.events.clear()
    return catalog


def test_write_relation_is_wrapped_once(catalog, relation):
    catalog.create_schema(relation)
    catalog.write_relation(relation, DATA, [])

    assert catalog.events == [
        ("before", WriteOperation.OVERWRITE),
        ("after", WriteOperation.OVERWRITE),
    ]


@pytest.mark.parametrize(
    "write, operation",
    [
        (lambda c, r: c.truncate_relation(r), WriteOperation.TRUNCATE),
        (lambda c, r: c.append_relation(r, DATA), WriteOperation.APPEND),
        (lambda c, r: c.merge_relation(r, DATA, ["id"]), WriteOperation.MERGE),
        (
            lambda c, r: c.delete_matched_relation(r, DATA, ["id"]),
            WriteOperation.DELETE,
        ),
        (
            lambda c, r: c.apply_snapshot_delta(r, DATA.clear(), DATA),
            WriteOperation.SNAPSHOT,
        ),
        (lambda c, r: c.set_relation_comment(r, "table"), WriteOperation.COMMENT),
        (
            lambda c, r: c.set_column_comments(r, {"id": "key"}),
            WriteOperation.COMMENT,
        ),
        (
            lambda c, r: c.set_comments(r, "table", {"id": "key"}),
            WriteOperation.COMMENT,
        ),
    ],
)
def test_each_write_is_wrapped_once(catalog, relation, write, operation):
    write(written(catalog, relation), relation)

    assert catalog.events == [("before", operation), ("after", operation)]


def test_empty_snapshot_delta_is_not_a_write(catalog, relation):
    written(catalog, relation)
    catalog.apply_snapshot_delta(relation, DATA.clear(), DATA.clear())

    assert catalog.events == []


def test_failed_write_skips_the_code_after_yield(catalog, relation):
    written(catalog, relation)

    with pytest.raises(Exception):
        catalog.append_relation(relation, pl.DataFrame({"id": ["not an int"]}))

    assert catalog.events == [("before", WriteOperation.APPEND)]
