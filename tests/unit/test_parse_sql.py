import polars as pl
import pytest
import sqlglot
import sqlglot.expressions as exp
from dbt_common.exceptions import DbtRuntimeError
from sqlglot.optimizer.scope import traverse_scope

from dbt.adapters.polars.sql_rewrite import (
    _disambiguated_flat_name,
    _find_qualified_tables,
    _is_qualified_table,
    _names_claimed_by_multiple_identities,
    _names_claimed_by_table,
    _qualifier_source_is_safe,
    _raise_if_a_column_unsafely_qualifies_a_colliding_name,
    _relation_key,
    _replace_qualified_tables_with_flat_names,
    _resolve_qualifier_in_enclosing_scopes,
    parse_and_rewrite,
)


def _first_table(sql: str) -> exp.Table:
    return sqlglot.parse_one(sql).find(exp.Table)


def test_flattens_a_qualified_table_and_aliases_it_to_its_bare_name():
    sql, refs = parse_and_rewrite('SELECT * FROM "db"."sch"."orders"')

    assert sql == 'SELECT * FROM "db__sch__orders" AS "orders"'
    assert list(refs.keys()) == ["db__sch__orders"]
    relation = refs["db__sch__orders"]
    assert (relation.database, relation.schema, relation.identifier) == (
        "db",
        "sch",
        "orders",
    )


def test_leaves_everything_but_qualified_table_names_untouched():
    sql, _ = parse_and_rewrite(
        "select TIMESTAMP '2026-10-05 16:32:36', x + INTERVAL '1 day' * n, [1, 2]\n"
        'from "db"."sch"."orders" o -- comment\n'
        "join db.sch.customers on true"
    )

    assert sql == (
        "select TIMESTAMP '2026-10-05 16:32:36', x + INTERVAL '1 day' * n, [1, 2]\n"
        'from "db__sch__orders" o -- comment\n'
        'join "db__sch__customers" AS customers on true'
    )


def test_functions_are_not_transpiled_to_duckdb_equivalents():
    # duckdb's generator turns log10(x) into LOG(10, x), which Polars reads with
    # the arguments swapped.
    sql, refs = parse_and_rewrite('SELECT log10(x) AS y FROM "db"."sch"."numbers"')

    frames = {name: pl.LazyFrame({"x": [100.0]}) for name in refs}
    result = pl.SQLContext(frames).execute(sql, eager=True)

    assert result["y"].to_list() == [2.0]


def test_keeps_an_explicit_alias_on_the_rewritten_table():
    sql, refs = parse_and_rewrite('SELECT o.id FROM "db"."sch"."orders" AS o')

    assert sql == 'SELECT o.id FROM "db__sch__orders" AS o'
    assert list(refs.keys()) == ["db__sch__orders"]


def test_unqualified_columns_never_trigger_validation():
    sql, refs = parse_and_rewrite('SELECT id, amount FROM "db"."sch"."orders"')

    assert sql == 'SELECT id, amount FROM "db__sch__orders" AS "orders"'
    assert list(refs.keys()) == ["db__sch__orders"]


def test_cte_name_can_qualify_columns_without_an_alias():
    sql, refs = parse_and_rewrite(
        'WITH stg AS (SELECT * FROM "db"."sch"."orders") SELECT stg.id FROM stg'
    )

    assert sql == (
        'WITH stg AS (SELECT * FROM "db__sch__orders" AS "orders") '
        "SELECT stg.id FROM stg"
    )
    assert list(refs.keys()) == ["db__sch__orders"]


@pytest.mark.parametrize(
    "sql",
    [
        'WITH base AS (SELECT * FROM "db"."sch"."base" WHERE id > 1) '
        "SELECT * FROM base",
        'WITH base AS (SELECT base.id FROM "db"."sch"."base" WHERE base.id > 1) '
        "SELECT base.id FROM base",
    ],
)
def test_cte_named_like_the_ref_it_wraps_is_not_shadowed(sql):
    rewritten_sql, refs = parse_and_rewrite(sql)

    frames = {name: pl.LazyFrame({"id": [1, 2, 3]}) for name in refs}
    result = pl.SQLContext(frames).execute(rewritten_sql, eager=True)

    assert result["id"].to_list() == [2, 3]


def test_correlated_subquery_can_qualify_an_outer_alias():
    _, refs = parse_and_rewrite(
        """
        SELECT o.id
        FROM "db"."sch"."orders" AS o
        WHERE o.id IN (
            SELECT t.order_id
            FROM "db"."sch"."other" AS t
            WHERE t.customer_id = o.customer_id
        )
        """
    )

    assert "db__sch__orders" in refs
    assert "db__sch__other" in refs


def test_bare_table_name_can_qualify_columns_when_there_is_no_collision():
    sql, refs = parse_and_rewrite('SELECT orders.id FROM "db"."sch"."orders"')

    assert sql == 'SELECT orders.id FROM "db__sch__orders" AS "orders"'
    assert list(refs.keys()) == ["db__sch__orders"]


def test_raises_when_a_colliding_table_is_qualified_by_its_bare_name():
    with pytest.raises(DbtRuntimeError, match="is ambiguous"):
        parse_and_rewrite(
            """
            SELECT orders.source
            FROM "db"."schema_a"."orders"
            CROSS JOIN "db"."schema_b"."orders"
            """
        )


def test_two_tables_sharing_an_identifier_are_renamed_to_stay_distinct():
    sql, refs = parse_and_rewrite(
        """
        SELECT a.source AS a_source, b.source AS b_source
        FROM "db"."schema_a"."orders" AS a
        CROSS JOIN "db"."schema_b"."orders" AS b
        """
    )

    assert "db__schema_a__orders" in sql
    assert "db__schema_b__orders" in sql
    assert set(refs.keys()) == {"db__schema_a__orders", "db__schema_b__orders"}
    assert refs["db__schema_a__orders"].schema == "schema_a"
    assert refs["db__schema_b__orders"].schema == "schema_b"


def test_alias_matching_an_unrelated_tables_bare_name_keeps_both_distinct():
    sql, refs = parse_and_rewrite(
        """
        SELECT orders.id, o.amount
        FROM "db"."sch"."orders_stg" AS orders
        JOIN "db"."sch"."orders" AS o ON orders.id = o.id
        """
    )

    assert '"db__sch__orders_stg" AS orders' in sql
    assert '"db__sch__orders" AS o' in sql
    assert set(refs.keys()) == {"db__sch__orders_stg", "db__sch__orders"}


# _is_qualified_table


def test_is_qualified_table_returns_true_for_a_three_part_name():
    assert _is_qualified_table(_first_table('SELECT * FROM "db"."sch"."orders"'))


def test_is_qualified_table_returns_true_for_a_two_part_name():
    assert _is_qualified_table(_first_table('SELECT * FROM "sch"."orders"'))


def test_is_qualified_table_returns_false_for_a_bare_name():
    assert not _is_qualified_table(_first_table("SELECT * FROM orders"))


def test_is_qualified_table_returns_false_for_a_non_table_node():
    column = sqlglot.parse_one("SELECT id FROM orders").find(exp.Column)
    assert not _is_qualified_table(column)


# _find_qualified_tables


def test_find_qualified_tables_returns_only_the_qualified_ones():
    ast = sqlglot.parse_one('SELECT * FROM "db"."sch"."orders", local_table')
    assert [table.name for table in _find_qualified_tables(ast)] == ["orders"]


def test_find_qualified_tables_returns_empty_list_when_nothing_qualifies():
    ast = sqlglot.parse_one("SELECT * FROM orders")
    assert _find_qualified_tables(ast) == []


# _relation_key


def test_relation_key_combines_catalog_db_and_name():
    table = _first_table('SELECT * FROM "db"."sch"."orders"')
    assert _relation_key(table) == ("db", "sch", "orders")


def test_relation_key_differs_for_the_same_name_in_different_schemas():
    a = _first_table('SELECT * FROM "db"."schema_a"."orders"')
    b = _first_table('SELECT * FROM "db"."schema_b"."orders"')
    assert _relation_key(a) != _relation_key(b)


# _names_claimed_by_table


def test_names_claimed_by_table_is_just_the_name_when_there_is_no_alias():
    table = _first_table('SELECT * FROM "db"."sch"."orders"')
    assert _names_claimed_by_table(table) == ["orders"]


def test_names_claimed_by_table_includes_the_alias_when_present():
    table = _first_table('SELECT * FROM "db"."sch"."orders" AS o')
    assert _names_claimed_by_table(table) == ["orders", "o"]


# _disambiguated_flat_name


def test_disambiguated_flat_name_joins_catalog_schema_and_name():
    table = _first_table('SELECT * FROM "db"."sch"."orders"')
    assert _disambiguated_flat_name(table) == "db__sch__orders"


# _names_claimed_by_multiple_identities


def test_names_claimed_by_multiple_identities_flags_a_name_shared_by_two_tables():
    tables = [
        _first_table('SELECT * FROM "db"."schema_a"."orders"'),
        _first_table('SELECT * FROM "db"."schema_b"."orders"'),
    ]
    assert _names_claimed_by_multiple_identities(tables) == {"orders"}


def test_names_claimed_by_multiple_identities_ignores_a_name_used_once():
    tables = [_first_table('SELECT * FROM "db"."sch"."orders"')]
    assert _names_claimed_by_multiple_identities(tables) == set()


def test_names_claimed_by_multiple_identities_counts_an_alias_against_a_bare_name():
    tables = [
        _first_table('SELECT * FROM "db"."sch"."orders_stg" AS orders'),
        _first_table('SELECT * FROM "db"."sch"."orders"'),
    ]
    assert _names_claimed_by_multiple_identities(tables) == {"orders"}


# _replace_qualified_tables_with_flat_names


def test_replace_qualified_tables_with_flat_names_rewrites_sql_and_collects_refs():
    original_sql = 'SELECT * FROM "db"."sch"."orders"'
    table = sqlglot.parse_one(original_sql).find(exp.Table)

    sql, refs = _replace_qualified_tables_with_flat_names(original_sql, [table])

    assert sql == 'SELECT * FROM "db__sch__orders" AS "orders"'
    relation = refs["db__sch__orders"]
    assert (relation.database, relation.schema, relation.identifier) == (
        "db",
        "sch",
        "orders",
    )


# _resolve_qualifier_in_enclosing_scopes


def test_resolve_qualifier_in_enclosing_scopes_finds_a_source_in_its_own_scope():
    ast = sqlglot.parse_one('SELECT o.id FROM "db"."sch"."orders" AS o')
    scope = traverse_scope(ast)[-1]

    assert isinstance(_resolve_qualifier_in_enclosing_scopes(scope, "o"), exp.Table)


def test_resolve_qualifier_in_enclosing_scopes_walks_up_for_a_correlated_reference():
    ast = sqlglot.parse_one(
        """
        SELECT o.id
        FROM "db"."sch"."orders" AS o
        WHERE o.id IN (
            SELECT t.order_id
            FROM "db"."sch"."other" AS t
            WHERE t.customer_id = o.customer_id
        )
        """
    )
    inner_scope = traverse_scope(ast)[0]

    source = _resolve_qualifier_in_enclosing_scopes(inner_scope, "o")

    assert isinstance(source, exp.Table)
    assert source.alias == "o"


def test_resolve_qualifier_in_enclosing_scopes_returns_none_when_not_found():
    ast = sqlglot.parse_one('SELECT o.id FROM "db"."sch"."orders" AS o')
    scope = traverse_scope(ast)[-1]

    assert _resolve_qualifier_in_enclosing_scopes(scope, "missing") is None


# _qualifier_source_is_safe


def test_qualifier_source_is_safe_for_a_cte_scope():
    ast = sqlglot.parse_one(
        'WITH stg AS (SELECT * FROM "db"."sch"."orders") SELECT stg.id FROM stg'
    )
    scope = traverse_scope(ast)[-1]

    assert _qualifier_source_is_safe(scope.sources["stg"])


def test_qualifier_source_is_safe_for_an_aliased_table():
    table = _first_table('SELECT * FROM "db"."sch"."orders" AS o')
    assert _qualifier_source_is_safe(table)


def test_qualifier_source_is_safe_is_false_for_an_unaliased_table():
    table = _first_table('SELECT * FROM "db"."sch"."orders"')
    assert not _qualifier_source_is_safe(table)


def test_qualifier_source_is_safe_is_false_for_no_source():
    assert not _qualifier_source_is_safe(None)


# _raise_if_a_column_unsafely_qualifies_a_colliding_name


def test_raise_if_unsafe_qualifier_does_nothing_when_nothing_collides():
    ast = sqlglot.parse_one('SELECT orders.id FROM "db"."sch"."orders"')

    assert (
        _raise_if_a_column_unsafely_qualifies_a_colliding_name(
            ast, colliding_names=set()
        )
        is None
    )


def test_raise_if_unsafe_qualifier_raises_for_a_bare_colliding_name():
    ast = sqlglot.parse_one(
        """
        SELECT orders.source
        FROM "db"."schema_a"."orders"
        CROSS JOIN "db"."schema_b"."orders"
        """
    )

    with pytest.raises(DbtRuntimeError, match="is ambiguous"):
        _raise_if_a_column_unsafely_qualifies_a_colliding_name(
            ast, colliding_names={"orders"}
        )


def test_raise_if_unsafe_qualifier_allows_a_colliding_name_resolved_via_an_alias():
    ast = sqlglot.parse_one('SELECT orders.id FROM "db"."sch"."something" AS orders')

    assert (
        _raise_if_a_column_unsafely_qualifies_a_colliding_name(
            ast, colliding_names={"orders"}
        )
        is None
    )


def test_raise_if_unsafe_qualifier_allows_a_colliding_name_resolved_via_a_cte():
    ast = sqlglot.parse_one(
        "WITH orders AS (SELECT 1 AS id) SELECT orders.id FROM orders"
    )

    assert (
        _raise_if_a_column_unsafely_qualifies_a_colliding_name(
            ast, colliding_names={"orders"}
        )
        is None
    )
