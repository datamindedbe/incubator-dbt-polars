import polars as pl
import pytest

from dbt.adapters.polars.catalogs.iceberg_write_utils import _key_prune_filter
from dbt.adapters.polars.catalogs.icebergCatalog import _apply_partial_update


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


def test_single_key_prune_filter_is_one_in():
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import In

    result = _key_prune_filter(["id"], pl.DataFrame({"id": [1, 2, 2, None]}))

    assert isinstance(result, In)
    assert {lit.value for lit in result.literals} == {1, 2}


def test_composite_key_prune_filter_grows_with_distinct_values_not_rows():
    """One In() per key column: a 50 x 288 x 300 batch (4.3M rows) gives 638
    literals, not an Or of 4.3M Ands, which pyiceberg cannot evaluate (22 s at 5k
    rows, a crash at 20k)."""
    pytest.importorskip("pyiceberg")
    from pyiceberg.expressions import And, In

    df = (
        pl.DataFrame({"date": list(range(50))})
        .join(pl.DataFrame({"time": list(range(288))}), how="cross")
        .join(pl.DataFrame({"duid": [f"u{i}" for i in range(300)]}), how="cross")
    )
    result = _key_prune_filter(["date", "time", "duid"], df)

    def leaves(expr):
        if isinstance(expr, And):
            return leaves(expr.left) + leaves(expr.right)
        return [expr]

    ins = leaves(result)
    assert isinstance(result, And)
    assert len(ins) == 3 and all(isinstance(e, In) for e in ins)
    assert sum(len(e.literals) for e in ins) == 50 + 288 + 300
