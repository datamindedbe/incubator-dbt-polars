import polars as pl
from dbt.tests.util import get_connection, relation_from_name


def get_records(project, table, select=None, where=None):
    with get_connection(project.adapter):
        relation = relation_from_name(project.adapter, table)
        df = (
            project.adapter.get_storage_catalog(relation.database)
            .get_relation(relation)
            .collect()
        )
    if select and "dbt_valid_to is null as is_current" in select.lower():
        df = df.with_columns(
            pl.col("dbt_valid_to").is_null().alias("is_current")
        ).select(["id", "is_current"])
    if where:
        df = df.filter(pl.sql_expr(where))
    return df.rows()


def update_records(project, table, updates, where=None):
    with get_connection(project.adapter):
        relation = relation_from_name(project.adapter, table)
        catalog = project.adapter.get_storage_catalog(relation.database)
        df = catalog.get_relation(relation).collect()
    mask = pl.sql_expr(where) if where else pl.lit(True)
    df = df.with_columns(
        [
            pl.when(mask).then(pl.sql_expr(expr)).otherwise(pl.col(col)).alias(col)
            for col, expr in updates.items()
        ]
    )
    with get_connection(project.adapter):
        relation = relation_from_name(project.adapter, table)
        catalog = project.adapter.get_storage_catalog(relation.database)
        catalog.write_relation(relation, df, [])


def insert_records(project, to_table, from_table, select, where=None):
    with get_connection(project.adapter):
        from_rel = relation_from_name(project.adapter, from_table)
        to_rel = relation_from_name(project.adapter, to_table)
        from_cat = project.adapter.get_storage_catalog(from_rel.database)
        to_cat = project.adapter.get_storage_catalog(to_rel.database)
        src = from_cat.get_relation(from_rel).collect()
        if where:
            src = src.filter(pl.sql_expr(where))
        dst_cols = set(to_cat.get_relation(to_rel).collect_schema().names())
        to_cat.append_relation(
            to_rel, src.select([c for c in src.columns if c in dst_cols])
        )


def delete_records(project, table, where=None):
    with get_connection(project.adapter):
        relation = relation_from_name(project.adapter, table)
        catalog = project.adapter.get_storage_catalog(relation.database)
        if where is None:
            catalog.truncate_relation(relation)
        else:
            df = catalog.get_relation(relation).collect()
            catalog.write_relation(relation, df.filter(~pl.sql_expr(where)), [])


def clone_table(project, to_table, from_table, select, where=None):
    with get_connection(project.adapter):
        from_rel = relation_from_name(project.adapter, from_table)
        to_rel = relation_from_name(project.adapter, to_table)
        from_cat = project.adapter.get_storage_catalog(from_rel.database)
        to_cat = project.adapter.get_storage_catalog(to_rel.database)
        src = from_cat.get_relation(from_rel).collect()
        if where:
            src = src.filter(pl.sql_expr(where))
        if to_cat.table_exists(to_rel):
            to_cat.drop_relation(to_rel)
        to_cat.write_relation(to_rel, src, [])


def add_column(project, table, column, definition):
    with get_connection(project.adapter):
        relation = relation_from_name(project.adapter, table)
        catalog = project.adapter.get_storage_catalog(relation.database)
        df = catalog.get_relation(relation).collect()
        catalog.write_relation(
            relation,
            df.with_columns(pl.lit(None).cast(pl.String).alias(column)),
            [],
        )
