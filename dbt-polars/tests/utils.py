import polars as pl
from dbt.tests.util import get_connection, relation_from_name


def polars_relation_row_count(adapter, relation_name: str) -> int:
    """Row count of a relation, read directly via the Polars catalog.

    Substitute for
    `len(project.run_sql(f"select * from {schema}.{name}", fetch="all"))`.
    """
    with get_connection(adapter):
        relation = relation_from_name(adapter, relation_name)
        return len(
            adapter.get_storage_catalog(relation.database)
            .get_relation(relation)
            .collect()
        )


def polars_append_rows(adapter, relation_name: str, rows: list[dict]) -> None:
    """Append literal rows to an existing relation directly via the catalog.

    Substitute for executing a raw SQL `INSERT INTO ... VALUES (...)` statement:
    Polars' SQLContext only supports SELECT-style queries, not DML, so there's no
    SQL string this adapter could run for that. Rows are cast to the relation's
    existing schema (e.g. date columns given as ISO strings) before appending.
    """
    with get_connection(adapter):
        relation = relation_from_name(adapter, relation_name)
        catalog = adapter.get_storage_catalog(relation.database)
        existing_schema = catalog.get_relation(relation).collect_schema()
        df = pl.DataFrame(rows).cast(existing_schema)
        catalog.append_relation(relation, df)


def polars_relation_partition_columns(adapter, relation_name: str) -> list[str]:
    """Partition columns of a relation, read directly via the Polars catalog."""
    with get_connection(adapter):
        relation = relation_from_name(adapter, relation_name)
        return adapter.get_storage_catalog(relation.database).get_partition_columns(
            relation
        )


def polars_read_relation(
    adapter,
    relation_name: str,
    columns: list[str],
    order_by: str | list[str] | None = None,
) -> list[tuple]:
    """Polars-native substitute for dbt's `project.run_sql(sql, fetch="all")`.

    Reads selected columns from a relation directly via the catalog instead of
    executing SQL (this adapter has no SQL engine, so `run_sql_for_tests` isn't
    available). Returns rows as a list of tuples, like a DB-API cursor fetchall.
    """
    with get_connection(adapter):
        relation = relation_from_name(adapter, relation_name)
        df = (
            adapter.get_storage_catalog(relation.database)
            .get_relation(relation)
            .collect()
        )

    df = df.select(columns)
    if order_by is not None:
        df = df.sort(order_by)

    return df.rows()


def polars_check_relations_equal(adapter, relation_names: list[str]) -> None:
    """Polars-native relation comparison for adapters without a SQL engine.

    Drop-in replacement for dbt's check_relations_equal that loads tables directly
    from the catalog and compares them using Polars instead of executing SQL.
    """
    with get_connection(adapter):
        relations = [relation_from_name(adapter, name) for name in relation_names]
        basis, compares = relations[0], relations[1:]

        basis_catalog = adapter.get_storage_catalog(basis.database)
        basis_df = basis_catalog.get_relation(basis).collect()
        col_names = [c for c in basis_df.columns if not c.lower().startswith("dbt_")]
        basis_df = basis_df.select(col_names)

        for compare_rel in compares:
            compare_df = (
                adapter.get_storage_catalog(compare_rel.database)
                .get_relation(compare_rel)
                .collect()
                .select(col_names)
            )

            row_diff = len(basis_df) - len(compare_df)
            assert row_diff == 0, (
                f"Row count differs by {row_diff} between {basis} and {compare_rel}"
            )

            mismatched = len(
                basis_df.join(compare_df, on=col_names, how="anti", nulls_equal=True)
            )
            assert mismatched == 0, (
                f"Got {mismatched} different rows between {basis} and {compare_rel}"
            )
