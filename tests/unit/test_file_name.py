import pytest
from dbt.adapters.contracts.relation import RelationType

from dbt.adapters.polars.catalogs import file_name_of
from dbt.adapters.polars.relation import PolarsRelation


def _relation(identifier: str, file_format: str = "csv") -> PolarsRelation:
    return PolarsRelation.create(
        database="db",
        schema="raw",
        identifier=identifier,
        type=RelationType.Table,
        file_format=file_format,
    )


@pytest.mark.parametrize(
    ("identifier", "file_format", "expected"),
    [
        ("people", "csv", "people.csv"),
        ("people.csv", "csv", "people.csv"),
        ("PEOPLE.CSV", "csv", "PEOPLE.CSV"),
        ("*.CSV", "csv", "*.CSV"),
        ("people.CSV", "parquet", "people.CSV.parquet"),
        ("events.v2", "parquet", "events.v2.parquet"),
    ],
)
def test_file_name_of(identifier, file_format, expected):
    assert file_name_of(_relation(identifier, file_format)) == expected
