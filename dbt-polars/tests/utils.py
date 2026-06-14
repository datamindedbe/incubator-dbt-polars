from dbt.tests.util import get_connection, relation_from_name


def polars_check_relations_equal(adapter, relation_names: list[str]) -> None:
    """Polars-native relation comparison for adapters without a SQL engine.

    Drop-in replacement for dbt's check_relations_equal that loads tables directly
    from the catalog and compares them using Polars instead of executing SQL.
    """
    with get_connection(adapter):
        relations = [relation_from_name(adapter, name) for name in relation_names]
        basis, compares = relations[0], relations[1:]

        basis_catalog = adapter.get_catalog(basis.database)
        basis_df = basis_catalog.get_relation(basis).collect()
        col_names = [c for c in basis_df.columns if not c.lower().startswith("dbt_")]
        basis_df = basis_df.select(col_names)

        for compare_rel in compares:
            compare_df = (
                adapter.get_catalog(compare_rel.database)
                .get_relation(compare_rel)
                .collect()
                .select(col_names)
            )

            row_diff = len(basis_df) - len(compare_df)
            assert (
                row_diff == 0
            ), f"Row count differs by {row_diff} between {basis} and {compare_rel}"

            mismatched = len(
                basis_df.join(compare_df, on=col_names, how="anti", nulls_equal=True)
            )
            assert (
                mismatched == 0
            ), f"Got {mismatched} different rows between {basis} and {compare_rel}"
