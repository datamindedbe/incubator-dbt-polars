import polars as pl
import pytest

from dbt.adapters.polars.catalogs.icebergCatalog import (
    _apply_partial_update,
    _build_key_delete_filter,
)


def test_update_cols_are_replaced_from_incoming():
    existing = pl.DataFrame({"id": [1], "score": [10], "label": ["original"]})
    incoming = pl.DataFrame({"id": [1], "score": [99], "label": ["changed"]})

    result = _apply_partial_update(
        existing, incoming, keys=["id"], update_cols=["score"]
    )

    assert result["score"].to_list() == [99]
    assert result["label"].to_list() == ["original"]


def test_except_cols_are_preserved_from_existing():
    existing = pl.DataFrame({"id": [1, 2], "price": [100, 200], "category": ["a", "b"]})
    incoming = pl.DataFrame({"id": [1, 2], "price": [999, 888], "category": ["x", "y"]})

    result = _apply_partial_update(
        existing, incoming, keys=["id"], update_cols=["price"]
    )

    assert result["price"].to_list() == [999, 888]
    assert result["category"].to_list() == ["a", "b"]


def test_empty_update_cols_returns_existing_unchanged():
    existing = pl.DataFrame({"id": [1], "score": [10]})
    incoming = pl.DataFrame({"id": [1], "score": [99]})

    result = _apply_partial_update(existing, incoming, keys=["id"], update_cols=[])

    assert result.equals(existing)


def test_apply_partial_update_composite_key():
    existing = pl.DataFrame(
        {"region": ["eu", "eu"], "product": ["a", "b"], "revenue": [10, 20]}
    )
    incoming = pl.DataFrame({"region": ["eu"], "product": ["a"], "revenue": [99]})

    result = _apply_partial_update(
        existing.head(1), incoming, keys=["region", "product"], update_cols=["revenue"]
    )

    assert result["revenue"].to_list() == [99]


def test_multiple_update_cols():
    existing = pl.DataFrame({"id": [1], "a": [1], "b": [2], "c": [3]})
    incoming = pl.DataFrame({"id": [1], "a": [10], "b": [20], "c": [30]})

    result = _apply_partial_update(
        existing, incoming, keys=["id"], update_cols=["a", "b"]
    )

    assert result["a"].to_list() == [10]
    assert result["b"].to_list() == [20]
    assert result["c"].to_list() == [3]


def test_apply_partial_update_output_columns_match_existing():
    existing = pl.DataFrame({"id": [1], "x": [1], "y": [2]})
    incoming = pl.DataFrame({"id": [1], "x": [9], "y": [9]})

    result = _apply_partial_update(existing, incoming, keys=["id"], update_cols=["x"])

    assert result.columns == existing.columns


def test_single_key_produces_in_expression():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import In

    df = pl.DataFrame({"id": [1, 2, 3]})
    result = _build_key_delete_filter(["id"], df)

    assert isinstance(result, In)


def test_composite_key_single_row_produces_and():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import And as IcebergAnd

    df = pl.DataFrame({"k1": [1], "k2": ["a"]})
    result = _build_key_delete_filter(["k1", "k2"], df)

    assert isinstance(result, IcebergAnd)


def test_composite_key_multiple_rows_produces_or_of_ands():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import Or

    df = pl.DataFrame({"k1": [1, 2], "k2": ["a", "b"]})
    result = _build_key_delete_filter(["k1", "k2"], df)

    assert isinstance(result, Or)
