from __future__ import annotations

from typing import TypeGuard

import sqlglot
import sqlglot.expressions as exp
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from sqlglot.optimizer.scope import Scope, traverse_scope

RelationKey = tuple[str, str, str]


def parse_and_rewrite(sql: str) -> tuple[str, dict[str, PolarsRelation]]:
    """Strip catalog/schema qualifiers from three-level dbt refs, renaming
    colliding tables so every table has a unique name in the flat
    SQLContext namespace. Raises DbtRuntimeError if a column qualifies a
    colliding table by its bare name rather than an alias, since that bare
    name may no longer point at that table after the rename.

    Returns the rewritten SQL and a dict mapping flat table name to
    PolarsRelation.
    """
    ast = sqlglot.parse_one(sql)
    parsed: exp.Expr = ast

    qualified_tables = _find_qualified_tables(parsed)
    colliding_names = _names_claimed_by_multiple_identities(qualified_tables)

    _raise_if_a_column_unsafely_qualifies_a_colliding_name(parsed, colliding_names)

    flat_name_by_key = _assign_flat_names(qualified_tables, colliding_names)
    return _replace_qualified_tables_with_flat_names(parsed, flat_name_by_key)


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


def _assign_flat_names(
    qualified_tables: list[exp.Table],
    colliding_names: set[str],
) -> dict[RelationKey, str]:
    flat_name_by_key: dict[RelationKey, str] = {}
    for table in qualified_tables:
        key = _relation_key(table)
        if table.name in colliding_names:
            flat_name_by_key[key] = _disambiguated_flat_name(table)
        else:
            flat_name_by_key[key] = table.name
    return flat_name_by_key


def _replace_qualified_tables_with_flat_names(
    ast: exp.Expr,
    flat_name_by_key: dict[RelationKey, str],
) -> tuple[str, dict[str, PolarsRelation]]:
    relation_by_flat_name: dict[str, PolarsRelation] = {}

    def replace_table(node: exp.Expr) -> exp.Expr:
        if not _is_qualified_table(node):
            return node

        flat_name = flat_name_by_key[_relation_key(node)]
        relation_by_flat_name[flat_name] = PolarsRelation.create(
            database=node.catalog,
            schema=node.db,
            identifier=node.name,
            type=RelationType.Table,
        )
        return exp.Table(
            this=exp.Identifier(this=flat_name, quoted=False),
            alias=node.args.get("alias"),
        )

    rewritten_ast = ast.transform(replace_table)
    return rewritten_ast.sql(), relation_by_flat_name


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
