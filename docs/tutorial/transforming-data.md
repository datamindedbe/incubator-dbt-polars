---
sidebar_label: "2. Transforming data"
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transforming data

In dbt, each .py or .sql file in the models folder defines a model. dbt-core mostly favors SQL models, but in dbt-polars both languages are equally efficient. Choose the one that best fits your expertise and the transformation at hand.

The model below computes daily aggregates of the weather data you loaded:

<Tabs groupId="language">
<TabItem value="python" label="Python" default>

```text {7}
quickstart/
├── models/
│   ├── example/
│   │   ├── my_first_dbt_model.sql
│   │   ├── my_second_dbt_model.sql
│   │   └── schema.yml
│   └── daily_weather.py
└── seeds/
    └── open_meteo_raw.csv
```

```python
# quickstart/models/daily_weather.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    hourly: pl.LazyFrame = dbt.ref("open_meteo_raw")
    return (
        hourly.group_by("date")
        .agg(
            pl.col("temperature_2m").min().alias("min_temperature"),
            pl.col("temperature_2m").max().alias("max_temperature"),
            pl.col("precipitation").sum().alias("total_precipitation"),
        )
        .sort("date")
    )
```

`dbt.ref("open_meteo_raw")` returns the seeded table as a Polars LazyFrame.

</TabItem>
<TabItem value="sql" label="SQL">

```text {7}
quickstart/
├── models/
│   ├── example/
│   │   ├── my_first_dbt_model.sql
│   │   ├── my_second_dbt_model.sql
│   │   └── schema.yml
│   └── daily_weather.sql
└── seeds/
    └── open_meteo_raw.csv
```

```sql
-- quickstart/models/daily_weather.sql
select
    date,
    min(temperature_2m) as min_temperature,
    max(temperature_2m) as max_temperature,
    sum(precipitation) as total_precipitation
from {{ ref('open_meteo_raw') }}
group by date
order by date
```

`{{ ref('open_meteo_raw') }}` refers to the seeded table.

</TabItem>
</Tabs>

Run `dbt run` again to write the new `daily_weather` table.

## Further reading

* See [Python models](../guides/python-models.md) for best practices when writing Python models in dbt-polars.
* See [Model configuration](../model-configuration.md) for the configurations that dbt-polars adds to models.
