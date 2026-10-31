# Python models

dbt supports [Python models](https://docs.getdbt.com/docs/build/python-models), but mostly as an addition to SQL, with fewer features. dbt-polars makes Python models first class. This guide highlights some of the features of Python models in dbt-polars.

## Creating a Python model

Every Python model is a `.py` file in the models folder. The file needs to contain a function named `model`, which is the entrypoint for dbt-polars to run:

```python
# models/paid_orders.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    orders: pl.LazyFrame = dbt.ref("orders")

    return orders.filter(pl.col("status") == "paid")
```

The model function has two inputs:

* `dbt`: Used to call dbt functions like `dbt.ref` and `dbt.source`.
* `session`: Part of the dbt signature, but unused in dbt-polars.

As with SQL models, the name of the .py file determines the name of the model.

## Configuring a model

`dbt.config` configures a Python model, like the config block of a SQL model. Here, paid_orders is stored as a table:

```python {6}
# models/paid_orders.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    dbt.config(materialized="table")

    orders: pl.LazyFrame = dbt.ref("orders")

    return orders.filter(pl.col("status") == "paid")
```

## Ephemeral models

### Ephemeral models in dbt and dbt-polars

In dbt, [ephemeral models](https://docs.getdbt.com/docs/build/materializations#ephemeral) are commonly used for intermediate steps that don't need to be stored, such as cleaning or filtering data before other models use it. dbt inlines them as common table expressions (CTEs) in the SQL of the models that reference them. Python models don't run as SQL, so in dbt they [can't be ephemeral, and can't reference ephemeral models](https://docs.getdbt.com/docs/build/python-models). dbt-polars supports every combination:

| Pattern | dbt-core | dbt-polars |
|---|---|---|
| SQL model references an ephemeral SQL model | ✅ | ✅ |
| Python model references an ephemeral SQL model | ❌️ | ✅ |
| SQL model references an ephemeral Python model | ❌️ | ✅ |
| Python model references an ephemeral Python model | ❌️ | ✅ |

### Example ephemeral model

In the example below, a SQL model references an ephemeral Python model.

```python {6}
# models/paid_orders.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    dbt.config(materialized="ephemeral")

    orders: pl.LazyFrame = dbt.ref("orders")

    return orders.filter(pl.col("status") == "paid")
```

Then add a SQL model that counts the paid orders:

```sql
-- models/paid_order_count.sql
{{ config(materialized="table") }}

select count(*) as paid_orders from {{ ref('paid_orders') }}
```

Run the project:

```bash
dbt run
```

dbt-polars defers the execution of paid_orders, and optimizes the combined query when it runs paid_order_count. The paid_orders model is never written to storage.

### How ephemeral models work

Ephemeral Python models are possible because Polars supports [two modes of operation: lazy and eager](https://docs.pola.rs/user-guide/concepts/lazy-api/). In eager mode, Polars runs each operation immediately, and keeps the result in memory as a DataFrame. In lazy mode, operations are only added to a query plan, called a LazyFrame. Polars runs the plan when the result is needed, and first optimizes it as a whole, like a SQL engine does with a query.

dbt-polars runs all models in lazy mode. In a Python model, `dbt.ref` returns a LazyFrame, and the model adds its transformations to it without reading any data. A SQL or Python model that references an ephemeral model builds on the LazyFrame of that ephemeral model. Polars optimizes and runs the combined query only when dbt-polars writes the result to a table.

:::tip[Return a LazyFrame]
A model in dbt-polars can return a LazyFrame or a DataFrame. Return a LazyFrame whenever possible. A DataFrame forces Polars to run the query early, which uses more memory and prevents optimizing it together with the models that build on it.
:::

## Loading data from an API

Because Python models are first class in dbt-polars, they open up use cases beyond transforming data. One of them is ingestion. A Python model can fetch data itself, for example from an API, and [incremental models](https://docs.getdbt.com/docs/build/incremental-models) are a great fit for this. Each run only fetches the latest data, so ingestion becomes part of your dbt project instead of a separate tool.

The model below loads hourly temperatures from the [Open-Meteo](https://open-meteo.com/) weather API. It uses the requests library, which is not installed with dbt-polars:

```bash
pip install requests
```

```python
# models/weather_raw.py
import polars as pl
import requests

from datetime import date, timedelta

START_DATE = date(2026, 1, 1)


def model(dbt, session) -> pl.DataFrame:
    dbt.config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key="date",
    )

    run_date = date.today() - timedelta(days=1)
    start_date = run_date if dbt.is_incremental else START_DATE

    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": 52.54833,
            "longitude": 13.407822,
            "start_date": start_date.isoformat(),
            "end_date": run_date.isoformat(),
            "hourly": "temperature_2m",
        },
    )
    response.raise_for_status()
    hourly = response.json()["hourly"]

    return (
        pl.DataFrame({"time": hourly["time"], "temperature": hourly["temperature_2m"]})
        .with_columns(pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M"))
        .with_columns(pl.col("time").dt.date().alias("date"))
    )
```

On the first run, the model fetches all data from START_DATE until yesterday. On later runs, `dbt.is_incremental` is true, and the model only fetches yesterday. With the delete+insert strategy and `unique_key="date"`, a date that is fetched again replaces its existing rows instead of being added twice.

This model returns a DataFrame, because the API response is already in memory.

:::warning
Don't call `.lazy()` on a DataFrame just to return a LazyFrame. dbt-polars expects a LazyFrame to hold little data until it runs, and breaking that assumption makes it run the query less efficiently.
:::

Run the model:

```bash
dbt run --select weather_raw
```

Use `dbt run --full-refresh` to reload all data from START_DATE.

## Read more

* [Python models](https://docs.getdbt.com/docs/build/python-models): configuration and incremental Python models in the dbt documentation.
* [Lazy API](https://docs.pola.rs/user-guide/concepts/lazy-api/): how lazy queries work in Polars.
