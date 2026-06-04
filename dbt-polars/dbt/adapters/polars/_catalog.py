import json
import os
import re

import polars as pl


def scan_delta_table(table_dir: str) -> pl.LazyFrame:
    # The delta log stores relative file paths (e.g. "part-xxx.parquet") which
    # never contain spaces.  Joining them with table_dir gives absolute paths
    # that pl.scan_parquet opens correctly — unlike pl.scan_delta / read_delta
    # which route through delta-rs's object_store and URL-encode the absolute
    # path, breaking on paths that contain spaces.
    active: set[str] = set()
    log_dir = os.path.join(table_dir, "_delta_log")
    for name in sorted(os.listdir(log_dir)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(log_dir, name)) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                action = json.loads(line)
                if "add" in action:
                    active.add(action["add"]["path"])
                elif "remove" in action:
                    active.discard(action["remove"]["path"])

    if not active:
        return pl.LazyFrame()
    return pl.scan_parquet([os.path.join(table_dir, f) for f in active])


def cte_names(sql: str) -> set:
    """Return CTE names that should be excluded from the registered SQL context.

    Polars SQLContext resolves a name to a registered table even when the same
    name is defined as a CTE — the registered table silently wins.  To fix
    this we exclude conflicting table names from context registration.

    However, we must NOT exclude a CTE whose own body references the same
    table name, e.g.:

        order_items AS (SELECT * FROM "order_items")

    Here the CTE simply wraps the base table; excluding it would break the
    body reference.  We only exclude CTEs that reference a *different* table,
    e.g.:

        orders AS (SELECT * FROM "stg_orders")
    """
    exclude = set()
    for m in re.finditer(r'(?:WITH|,)\s+"?(\w+)"?\s+AS\s*\(', sql, re.IGNORECASE):
        name = m.group(1)
        # Extract the CTE body using balanced-paren counting.
        pos = m.end()
        depth = 1
        while pos < len(sql) and depth > 0:
            if sql[pos] == "(":
                depth += 1
            elif sql[pos] == ")":
                depth -= 1
            pos += 1
        body = sql[m.end() : pos - 1]
        # dbt always quotes relation names, so a self-referencing CTE body
        # will contain "name".  If it does, keep the registered table.
        if f'"{name}"' not in body:
            exclude.add(name)
    return exclude


def build_sql_context(catalog_path: str, exclude: set = frozenset()) -> pl.SQLContext:
    ctx = pl.SQLContext()
    if not os.path.isdir(catalog_path):
        return ctx
    for schema_name in os.listdir(catalog_path):
        schema_dir = os.path.join(catalog_path, schema_name)
        if not os.path.isdir(schema_dir):
            continue
        for table_name in os.listdir(schema_dir):
            if table_name in exclude:
                continue
            table_dir = os.path.join(schema_dir, table_name)
            if os.path.isdir(table_dir) and os.path.exists(
                os.path.join(table_dir, "_delta_log")
            ):
                ctx.register(table_name, scan_delta_table(table_dir))
    return ctx


def strip_qualifiers(sql: str) -> str:
    """Strip 3-part and 2-part relation qualifiers so Polars SQLContext can resolve names.

    "db"."schema"."table"  →  "table"
    "schema"."table"       →  "table"
    """
    return re.sub(r'(?:"[^"]+"\.){1,2}"([^"]+)"', r'"\1"', sql)
