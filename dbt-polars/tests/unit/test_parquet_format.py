"""Unit tests for ParquetFormat.

All tests run against the local filesystem using pytest's ``tmp_path``
fixture — no cloud credentials are required.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from dbt.adapters.polars.formats.flatfile.parquetFormat import ParquetFormat
from dbt.adapters.polars.relation import TableFormat
from dbt_common.exceptions import DbtRuntimeError


@pytest.fixture()
def fmt() -> ParquetFormat:
    return ParquetFormat()


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    """An initialised Parquet table directory with three rows."""
    d = tmp_path / "tbl"
    d.mkdir()
    pl.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]}).write_parquet(
        str(d / "data.parquet")
    )
    return d


# ---------------------------------------------------------------------------
# table_format class variable
# ---------------------------------------------------------------------------


class TestTableFormat:
    def test_table_format_is_parquet(self, fmt: ParquetFormat) -> None:
        assert fmt.table_format == TableFormat.parquet

    def test_table_format_is_class_level(self) -> None:
        assert ParquetFormat.table_format == TableFormat.parquet


# ---------------------------------------------------------------------------
# is_table
# ---------------------------------------------------------------------------


class TestIsTable:
    def test_true_when_parquet_file_exists(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        assert fmt.is_table(str(table)) is True

    def test_false_when_directory_empty(
        self, fmt: ParquetFormat, tmp_path: Path
    ) -> None:
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        assert fmt.is_table(str(empty_dir)) is False

    def test_false_when_directory_missing(
        self, fmt: ParquetFormat, tmp_path: Path
    ) -> None:
        assert fmt.is_table(str(tmp_path / "nonexistent")) is False

    def test_false_when_only_non_parquet_files(
        self, fmt: ParquetFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "other"
        d.mkdir()
        (d / "file.csv").write_text("a,b\n1,2")
        assert fmt.is_table(str(d)) is False


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


class TestScan:
    def test_returns_lazyframe(self, fmt: ParquetFormat, table: Path) -> None:
        result = fmt.scan(str(table))
        assert isinstance(result, pl.LazyFrame)

    def test_returns_correct_data(self, fmt: ParquetFormat, table: Path) -> None:
        result = fmt.scan(str(table)).collect()
        assert result["id"].to_list() == [1, 2, 3]
        assert result["val"].to_list() == ["a", "b", "c"]

    def test_reads_multiple_part_files(
        self, fmt: ParquetFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "tbl"
        d.mkdir()
        pl.DataFrame({"x": [1, 2]}).write_parquet(str(d / "part-0.parquet"))
        pl.DataFrame({"x": [3, 4]}).write_parquet(str(d / "part-1.parquet"))

        result = sorted(fmt.scan(str(d)).collect()["x"].to_list())
        assert result == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestWrite:
    def test_creates_data_parquet(self, fmt: ParquetFormat, tmp_path: Path) -> None:
        d = tmp_path / "tbl"
        d.mkdir()
        df = pl.DataFrame({"a": [10, 20]})
        fmt.write(str(d), df)
        assert (d / "data.parquet").exists()
        assert pl.read_parquet(str(d / "data.parquet")).equals(df)

    def test_overwrites_existing_data_parquet(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        new_df = pl.DataFrame({"id": [99], "val": ["z"]})
        fmt.write(str(table), new_df)
        # data.parquet now has the new content
        result = pl.read_parquet(str(table / "data.parquet"))
        assert result.equals(new_df)

    def test_scan_after_write_reflects_new_data(
        self, fmt: ParquetFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "tbl"
        d.mkdir()
        df = pl.DataFrame({"n": [5, 6, 7]})
        fmt.write(str(d), df)
        assert fmt.scan(str(d)).collect().equals(df)


# ---------------------------------------------------------------------------
# truncate
# ---------------------------------------------------------------------------


class TestTruncate:
    def test_truncate_removes_all_rows(self, fmt: ParquetFormat, table: Path) -> None:
        fmt.truncate(str(table))
        result = fmt.scan(str(table)).collect()
        assert len(result) == 0

    def test_truncate_preserves_schema(self, fmt: ParquetFormat, table: Path) -> None:
        original_schema = fmt.scan(str(table)).collect().schema
        fmt.truncate(str(table))
        assert fmt.scan(str(table)).collect().schema == original_schema


# ---------------------------------------------------------------------------
# append
# ---------------------------------------------------------------------------


class TestAppend:
    def test_append_adds_rows(self, fmt: ParquetFormat, table: Path) -> None:
        extra = pl.DataFrame({"id": [4, 5], "val": ["d", "e"]})
        fmt.append(str(table), extra)

        result = fmt.scan(str(table)).collect()
        assert sorted(result["id"].to_list()) == [1, 2, 3, 4, 5]

    def test_append_creates_new_part_file(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        before = set(table.iterdir())
        fmt.append(str(table), pl.DataFrame({"id": [9], "val": ["x"]}))
        after = set(table.iterdir())
        new_files = after - before
        assert len(new_files) == 1
        assert next(iter(new_files)).name.startswith("part-")

    def test_multiple_appends_accumulate(
        self, fmt: ParquetFormat, tmp_path: Path
    ) -> None:
        d = tmp_path / "tbl"
        d.mkdir()
        fmt.write(str(d), pl.DataFrame({"n": [1]}))
        fmt.append(str(d), pl.DataFrame({"n": [2]}))
        fmt.append(str(d), pl.DataFrame({"n": [3]}))

        result = sorted(fmt.scan(str(d)).collect()["n"].to_list())
        assert result == [1, 2, 3]


# ---------------------------------------------------------------------------
# merge / delete_matched — not supported
# ---------------------------------------------------------------------------


class TestMerge:
    def test_merge_raises_dbt_runtime_error(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        with pytest.raises(DbtRuntimeError, match="merge"):
            fmt.merge(
                str(table), pl.DataFrame({"id": [1], "val": ["x"]}), "s.id = t.id"
            )

    def test_merge_message_mentions_delta(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        with pytest.raises(DbtRuntimeError, match="DeltaLakeFormat"):
            fmt.merge(
                str(table), pl.DataFrame({"id": [1], "val": ["x"]}), "s.id = t.id"
            )


class TestDeleteMatched:
    def test_delete_matched_raises_dbt_runtime_error(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        with pytest.raises(DbtRuntimeError, match="delete_matched"):
            fmt.delete_matched(
                str(table), pl.DataFrame({"id": [1], "val": ["a"]}), "s.id = t.id"
            )


# ---------------------------------------------------------------------------
# Comments — no-ops inherited from BaseFormat
# ---------------------------------------------------------------------------


class TestComments:
    def test_set_table_comment_does_not_raise(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        fmt.set_table_comment(str(table), "hello")

    def test_get_table_comment_returns_none(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        fmt.set_table_comment(str(table), "hello")
        assert fmt.get_table_comment(str(table)) is None

    def test_set_column_comments_does_not_raise(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        fmt.set_column_comments(str(table), {"id": "primary key"})

    def test_get_column_comments_returns_empty_dict(
        self, fmt: ParquetFormat, table: Path
    ) -> None:
        fmt.set_column_comments(str(table), {"id": "primary key"})
        assert fmt.get_column_comments(str(table)) == {}


# ---------------------------------------------------------------------------
# storage_options passthrough
# ---------------------------------------------------------------------------


class TestStorageOptions:
    def test_no_options_is_none(self) -> None:
        assert ParquetFormat()._storage_options is None

    def test_empty_options_is_none(self) -> None:
        assert ParquetFormat({})._storage_options is None

    def test_non_empty_options_stored(self) -> None:
        opts = {"AWS_REGION": "eu-west-1"}
        assert ParquetFormat(opts)._storage_options == opts
