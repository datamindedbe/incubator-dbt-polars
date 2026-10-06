from __future__ import annotations

from typing import TypeGuard

import sqlglot
import sqlglot.expressions as exp
from dbt.adapters.contracts.relation import RelationType
from dbt_common.exceptions import DbtRuntimeError
from sqlglot.optimizer.scope import Scope, traverse_scope

from dbt.adapters.polars.relation import PolarsRelation

RelationKey = tuple[str, str, str]


def parse_and_rewrite(sql: str) -> tuple[str, dict[str, PolarsRelation]]:
    """Rename three-level dbt refs to `catalog__schema__name` so they get a
    unique name in the flat SQLContext namespace that can't shadow a CTE or
    alias in the query. An unaliased ref is aliased to its bare name so column
    qualifiers keep working. Raises DbtRuntimeError if a column qualifies a
    colliding table by its bare name rather than an alias, since that bare
    name can't be resolved to a single table.

    Returns the rewritten SQL and a dict mapping flat table name to
    PolarsRelation.
    """
    # The SQL is only parsed to locate table references; their names are spliced
    # into the original text so everything else reaches Polars exactly as written.
    ast = sqlglot.parse_one(sql, read="duckdb")
    parsed: exp.Expr = ast

    qualified_tables = _find_qualified_tables(parsed)
    colliding_names = _names_claimed_by_multiple_identities(qualified_tables)

    _raise_if_a_column_unsafely_qualifies_a_colliding_name(parsed, colliding_names)

    return _replace_qualified_tables_with_flat_names(sql, qualified_tables)


def _is_qualified_table(node: exp.Expr) -> TypeGuard[exp.Table]:
    return (
        isinstance(node, exp.Table)
        and bool(node.name)
        and bool(node.catalog or node.db)
    )


def _find_qualified_tables(ast: exp.Expr) -> list[exp.Table]:
    return [table for table in ast.find_all(exp.Table) if _is_qualified_table(table)]


def _relation_key(table: exp.Table) -> RelationKey:
    return (table.catalog, table.db, table.name)


def _names_claimed_by_table(table: exp.Table) -> list[str]:
    if table.alias:
        return [table.name, table.alias]
    return [table.name]


def _disambiguated_flat_name(table: exp.Table) -> str:
    catalog = table.catalog.strip('"')
    schema = table.db.strip('"')
    return f"{catalog}__{schema}__{table.name}"


def _names_claimed_by_multiple_identities(
    qualified_tables: list[exp.Table],
) -> set[str]:
    keys_by_claimed_name: dict[str, set[RelationKey]] = {}
    for table in qualified_tables:
        key = _relation_key(table)
        for claimed_name in _names_claimed_by_table(table):
            keys_by_claimed_name.setdefault(claimed_name, set()).add(key)

    return {
        claimed_name
        for claimed_name, keys in keys_by_claimed_name.items()
        if len(keys) > 1
    }


def _replace_qualified_tables_with_flat_names(
    sql: str,
    qualified_tables: list[exp.Table],
) -> tuple[str, dict[str, PolarsRelation]]:
    relation_by_flat_name: dict[str, PolarsRelation] = {}
    replacements: list[tuple[int, int, str]] = []

    for table in qualified_tables:
        flat_name = _disambiguated_flat_name(table)
        relation_by_flat_name[flat_name] = PolarsRelation.create(
            database=table.catalog,
            schema=table.db,
            identifier=table.name,
            type=RelationType.Table,
        )
        name_parts = [
            table.args[part]
            for part in ("catalog", "db", "this")
            if table.args.get(part)
        ]
        start, end = name_parts[0].meta["start"], name_parts[-1].meta["end"]
        replacement = f'"{flat_name}"'
        if not table.args.get("alias"):
            bare_name = table.this
            replacement += (
                f" AS {sql[bare_name.meta['start'] : bare_name.meta['end'] + 1]}"
            )
        replacements.append((start, end, replacement))

    # Splice right to left so earlier offsets stay valid.
    rewritten_sql = sql
    for start, end, replacement in sorted(replacements, reverse=True):
        rewritten_sql = rewritten_sql[:start] + replacement + rewritten_sql[end + 1 :]
    return rewritten_sql, relation_by_flat_name


def _raise_if_a_column_unsafely_qualifies_a_colliding_name(
    ast: exp.Expr, colliding_names: set[str]
) -> None:
    for scope in traverse_scope(ast):
        for column in scope.columns:
            qualifier = column.table
            if qualifier not in colliding_names:
                continue
            source = _resolve_qualifier_in_enclosing_scopes(scope, qualifier)
            if _qualifier_source_is_safe(source):
                continue
            raise DbtRuntimeError(
                f"Column qualifier '{qualifier}' is ambiguous: it names a "
                f"table that collides with another table elsewhere in this "
                f"query. Add an explicit alias to that table reference "
                f"(e.g. ... AS {qualifier}) to disambiguate."
            )


def _resolve_qualifier_in_enclosing_scopes(
    scope: Scope, qualifier: str
) -> Scope | exp.Table | None:
    current_scope: Scope | None = scope
    while current_scope is not None:
        source = current_scope.sources.get(qualifier)
        if source is not None:
            return source
        current_scope = current_scope.parent
    return None


def _qualifier_source_is_safe(source: Scope | exp.Table | None) -> bool:
    if isinstance(source, Scope):
        return True
    if isinstance(source, exp.Table):
        return source.args.get("alias") is not None
    return False
