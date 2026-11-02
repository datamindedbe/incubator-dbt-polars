---
sidebar_label: "3. Ephemeral models"
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ephemeral models

dbt supports [ephemeral models](https://docs.getdbt.com/docs/build/materializations#ephemeral). An ephemeral model is not written to storage. Like an intermediate function in Python, it is computed as part of every model that references it. This lets you split a large transformation into smaller steps, without storing each step as a table.

In dbt, only SQL models can be ephemeral. dbt-polars also supports ephemeral Python models, and lets SQL and Python models reference each other's ephemeral models, see [Python models](../guides/python-models.md#ephemeral-models).

The ephemeral model `open_meteo_clean` below renames the column `temperature_2m` to `temperature`:

<Tabs groupId="language">
<TabItem value="python" label="Python" default>

```python {6}
# quickstart/models/open_meteo_clean.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    dbt.config(materialized="ephemeral")
    hourly: pl.LazyFrame = dbt.ref("open_meteo_raw")
    return hourly.rename({"temperature_2m": "temperature"})
```

</TabItem>
<TabItem value="sql" label="SQL">

```sql {2}
-- quickstart/models/open_meteo_clean.sql
{{ config(materialized="ephemeral") }}

select
    time,
    temperature_2m as temperature,
    precipitation,
    date
from {{ ref('open_meteo_raw') }}
```

</TabItem>
</Tabs>

The `meteo_daily` model uses it to compute daily averages:

<Tabs groupId="language">
<TabItem value="python" label="Python" default>

```python
# quickstart/models/meteo_daily.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    clean: pl.LazyFrame = dbt.ref("open_meteo_clean")
    return clean.group_by("date").agg(
        pl.col("temperature").mean(), pl.col("precipitation").mean()
    )
```

</TabItem>
<TabItem value="sql" label="SQL">

```sql
-- quickstart/models/meteo_daily.sql
select
    date,
    avg(temperature) as temperature,
    avg(precipitation) as precipitation
from {{ ref('open_meteo_clean') }}
group by date
```

</TabItem>
</Tabs>

After `dbt run`, `data/main/` contains a `meteo_daily` table, but no `open_meteo_clean`.

<div className="alert alert--info margin-bottom--md" role="note">SQL and Python models can be mixed freely. For example, the Python version of the ephemeral `open_meteo_clean` model can be combined with the SQL version of `meteo_daily`.</div>

## Further reading

* See [Python models](../guides/python-models.md#ephemeral-models) for how ephemeral Python models work in dbt-polars.
* See [Performance](../guides/performance.md) for when an ephemeral model is cheaper than a table.
