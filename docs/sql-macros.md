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
| `current_timestamp` | Yes | Returns the dbt run start time as a naive UTC timestamp, fixed for the whole run. |
| `date` | Yes | |
| `dateadd` | Yes | Keeps the input type for day and larger dateparts (a date stays a date); sub-day dateparts return a timestamp. |
| `datediff` | Yes | `week` counts Sunday week boundaries. |
| `date_spine` | Yes | Returns dates when the start date is a date. |
| `date_trunc` | Partial | Only minute/hour/day/week/month/quarter/year truncation is supported. Weeks start on Monday. |
| `equals` | Yes | |
| `escape_single_quotes` | Yes | |
| `except` | Yes | |
| `generate_series` | Yes | |
| `get_intervals_between` | Yes | Date strings must be ISO (`YYYY-MM-DD`). |
| `get_powers_of_two` | Yes | |
| `hash` | No | |
| `intersect` | Yes | |
| `last_day` | Yes | |
| `length` | Yes | |
| `listagg` | Partial | Only unordered, unlimited aggregation is supported. |
| `literal` (`string_literal`) | Yes | |
| `position` | Yes | |
| `replace` | Yes | |
| `right` | Yes | |
| `safe_cast` | Yes | |
| `split_part` | Yes | |
