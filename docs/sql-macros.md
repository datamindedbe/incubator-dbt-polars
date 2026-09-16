# SQL macro support

Status of dbt-core's generic SQL macros (`dbt.bool_or()`, `dbt.datediff()`, etc.) on dbt-polars, whose SQL runs through Polars' embedded `pl.SQLContext` engine rather than a database.

| Macro | Supported | Note |
|---|---|---|
| `any_value` | Yes | |
| `array_append` | No | |
| `array_concat` | No | |
| `array_construct` | Yes | |
| `bool_or` | Yes | |
| `cast` | Yes | |
| `cast_bool_to_text` | Yes | |
| `concat` | Yes | |
| `current_timestamp` | No | |
| `date` | Yes | |
| `dateadd` | No | |
| `datediff` | No | |
| `date_spine` | No | |
| `date_trunc` | Partial | Only day/hour/minute/month/year truncation is supported. |
| `equals` | Yes | |
| `escape_single_quotes` | Yes | |
| `except` | Yes | |
| `generate_series` | Yes | |
| `get_intervals_between` | No | |
| `get_powers_of_two` | Yes | |
| `hash` | No | |
| `intersect` | Yes | |
| `last_day` | No | |
| `length` | Yes | |
| `listagg` | Partial | Only unordered, unlimited aggregation is supported. |
| `literal` (`string_literal`) | Yes | |
| `position` | Yes | |
| `replace` | Yes | |
| `right` | Yes | |
| `safe_cast` | Yes | |
| `split_part` | Yes | |
