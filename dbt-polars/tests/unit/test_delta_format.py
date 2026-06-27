"""Unit tests for DeltaLakeFormat.

All tests run against the local filesystem using pytest's ``tmp_path``
fixture — delta-rs handles its own I/O so no cloud credentials or mocking
are required.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from dbt.adapters.polars.formats.table.deltaFormat import DeltaLakeFormat
from dbt.adapters.polars.relation import TableFormat


@pytest.fixture()
def fmt() -> DeltaLakeFormat:
    return DeltaLakeFormat()


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    """An initialised Delta table directory with three rows."""
    d = tmp_path / "tbl"
    pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_delta(
        str(d), mode="overwrite"
    )
    return d


# ---------------------------------------------------------------------------
# table_format class variable
# ---------------------------------------------------------------------------


class TestTableFormat:
    def test_table_format_is_delta(self, fmt: DeltaLakeFormat) -> None:
        assert fmt.table_format == TableFormat.delta

    def test_table_format_is_class_level(self) -> None:
        assert DeltaLakeFormat.table_format == TableFormat.delta


# ---------------------------------------------------------------------------
# is_table
# ---------------------------------------------------------------------------


class TestIsTable:
    def test_true_when_delta_table_exists(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        assert fmt.is_table(str(table)) is True

    def test_false_when_directory_empty(
        self, fmt: DeltaLakeFormat, tmp_path: Path
    ) -> None:
        empty = tmp_path / "empty"
        empty.mkdir()
        assert fmt.is_table(str(empty)) is False

    def test_false_when_directory_missing(
        self, fmt: DeltaLakeFormat, tmp_path: Path
    ) -> None:
        assert fmt.is_table(str(tmp_path / "nonexistent")) is False

    def test_false_when_only_plain_parquet(
        self, fmt: DeltaLakeFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "other"
        d.mkdir()
        pl.DataFrame({"a": [1]}).write_parquet(str(d / "data.parquet"))
        assert fmt.is_table(str(d)) is False


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


class TestScan:
    def test_returns_lazyframe(self, fmt: DeltaLakeFormat, table: Path) -> None:
        assert isinstance(fmt.scan(str(table)), pl.LazyFrame)

    def test_returns_correct_data(self, fmt: DeltaLakeFormat, table: Path) -> None:
        result = fmt.scan(str(table)).collect()
        assert sorted(result["id"].to_list()) == [1, 2, 3]
        assert sorted(result["val"].to_list()) == ["a", "b", "c"]

    def test_preserves_schema_types(self, fmt: DeltaLakeFormat, table: Path) -> None:
        schema = fmt.scan(str(table)).collect().schema
        assert schema["id"] == pl.Int64
        assert schema["val"] == pl.String


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestWrite:
    def test_creates_delta_table(self, fmt: DeltaLakeFormat, tmp_path: Path) -> None:
        d = tmp_path / "tbl"
        fmt.write(str(d), pl.DataFrame({"a": [10, 20]}))
        assert fmt.is_table(str(d))

    def test_data_is_readable_after_write(
        self, fmt: DeltaLakeFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "tbl"
        df = pl.DataFrame({"a": [10, 20]})
        fmt.write(str(d), df)
        assert fmt.scan(str(d)).collect().equals(df)

    def test_write_overwrites_existing(self, fmt: DeltaLakeFormat, table: Path) -> None:
        new_df = pl.DataFrame({"id": [99], "val": ["z"]})
        fmt.write(str(table), new_df)
        result = fmt.scan(str(table)).collect()
        assert result.equals(new_df)


# ---------------------------------------------------------------------------
# truncate
# ---------------------------------------------------------------------------


class TestTruncate:
    def test_truncate_removes_all_rows(self, fmt: DeltaLakeFormat, table: Path) -> None:
        fmt.truncate(str(table))
        assert len(fmt.scan(str(table)).collect()) == 0

    def test_truncate_preserves_schema(self, fmt: DeltaLakeFormat, table: Path) -> None:
        # Delta stores schema in the transaction log — types survive truncation.
        original_schema = fmt.scan(str(table)).collect().schema
        fmt.truncate(str(table))
        assert fmt.scan(str(table)).collect().schema == original_schema


# ---------------------------------------------------------------------------
# append
# ---------------------------------------------------------------------------


class TestAppend:
    def test_append_adds_rows(self, fmt: DeltaLakeFormat, table: Path) -> None:
        extra = pl.DataFrame({"id": [4, 5], "val": ["d", "e"]})
        fmt.append(str(table), extra)
        result = fmt.scan(str(table)).collect()
        assert sorted(result["id"].to_list()) == [1, 2, 3, 4, 5]

    def test_multiple_appends_accumulate(
        self, fmt: DeltaLakeFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "tbl"
        fmt.write(str(d), pl.DataFrame({"n": [1]}))
        fmt.append(str(d), pl.DataFrame({"n": [2]}))
        fmt.append(str(d), pl.DataFrame({"n": [3]}))
        assert sorted(fmt.scan(str(d)).collect()["n"].to_list()) == [1, 2, 3]

    def test_append_with_schema_evolution(
        self, fmt: DeltaLakeFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "tbl"
        fmt.write(str(d), pl.DataFrame({"id": [1]}))
        fmt.append(
            str(d),
            pl.DataFrame({"id": [2], "extra": ["new"]}),
            allow_schema_evolution=True,
        )
        result = fmt.scan(str(d)).collect()
        assert "extra" in result.columns


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


class TestMerge:
    def test_merge_updates_matched_row(self, fmt: DeltaLakeFormat, table: Path) -> None:
        updated = pl.DataFrame({"id": [1], "val": ["UPDATED"]})
        fmt.merge(
            str(table),
            updated,
            predicate="DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id",
        )
        result = fmt.scan(str(table)).collect()
        row1_val = result.filter(pl.col("id") == 1)["val"][0]
        assert row1_val == "UPDATED"

    def test_merge_inserts_new_row(self, fmt: DeltaLakeFormat, table: Path) -> None:
        new_row = pl.DataFrame({"id": [9], "val": ["new"]})
        fmt.merge(
            str(table),
            new_row,
            predicate="DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id",
        )
        result = fmt.scan(str(table)).collect()
        assert 9 in result["id"].to_list()

    def test_merge_preserves_unmatched_rows(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        updated = pl.DataFrame({"id": [1], "val": ["UPDATED"]})
        fmt.merge(
            str(table),
            updated,
            predicate="DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id",
        )
        result = fmt.scan(str(table)).collect()
        assert sorted(result["id"].to_list()) == [1, 2, 3]

    def test_merge_except_cols_skips_excluded_column(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        updated = pl.DataFrame({"id": [1], "val": ["SHOULD_NOT_UPDATE"]})
        fmt.merge(
            str(table),
            updated,
            predicate="DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id",
            except_cols=["val"],
        )
        result = fmt.scan(str(table)).collect()
        row1_val = result.filter(pl.col("id") == 1)["val"][0]
        assert row1_val == "a"  # original value unchanged


# ---------------------------------------------------------------------------
# delete_matched
# ---------------------------------------------------------------------------


class TestDeleteMatched:
    def test_delete_matched_removes_rows(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        to_delete = pl.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        fmt.delete_matched(
            str(table),
            to_delete,
            predicate="DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id",
        )
        result = fmt.scan(str(table)).collect()
        assert result["id"].to_list() == [3]

    def test_delete_matched_preserves_unmatched_rows(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        to_delete = pl.DataFrame({"id": [1], "val": ["a"]})
        fmt.delete_matched(
            str(table),
            to_delete,
            predicate="DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id",
        )
        result = fmt.scan(str(table)).collect()
        assert sorted(result["id"].to_list()) == [2, 3]


# ---------------------------------------------------------------------------
# storage_options passthrough (inherited from BaseFormat)
# ---------------------------------------------------------------------------


class TestStorageOptions:
    def test_no_options_is_none(self) -> None:
        assert DeltaLakeFormat()._storage_options is None

    def test_empty_options_is_none(self) -> None:
        assert DeltaLakeFormat({})._storage_options is None

    def test_non_empty_options_stored(self) -> None:
        opts = {"AWS_REGION": "eu-west-1"}
        assert DeltaLakeFormat(opts)._storage_options == opts


# ---------------------------------------------------------------------------
# Comments — stored in the Delta transaction log
# ---------------------------------------------------------------------------


class TestComments:
    def test_set_and_get_table_comment(self, fmt: DeltaLakeFormat, table: Path) -> None:
        fmt.set_table_comment(str(table), "test description")
        assert fmt.get_table_comment(str(table)) == "test description"

    def test_get_table_comment_returns_none_when_not_set(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        assert fmt.get_table_comment(str(table)) is None

    def test_set_and_get_column_comments(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        fmt.set_column_comments(str(table), {"id": "primary key", "val": "value col"})
        comments = fmt.get_column_comments(str(table))
        assert comments["id"] == "primary key"
        assert comments["val"] == "value col"

    def test_get_column_comments_returns_empty_dict_when_not_set(
        self, fmt: DeltaLakeFormat, table: Path
    ) -> None:
        assert fmt.get_column_comments(str(table)) == {}
