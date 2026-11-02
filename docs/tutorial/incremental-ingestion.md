---
sidebar_label: "6. Incremental ingestion"
mdx:
  format: mdx
---

# Incremental ingestion

The seed from [Introduction & setup](loading-data.md) is a static file, so it never contains the latest weather data. A Python model can fetch the data itself, which brings ingestion into dbt.

Replace the seed `seeds/open_meteo_raw.csv` with a model `models/open_meteo_raw.py`. The name stays the same, so the models that reference it need no changes. Your folder structure is now:

```text {10}
quickstart/
├── models/
│   ├── example/
│   │   ├── my_first_dbt_model.sql
│   │   ├── my_second_dbt_model.sql
│   │   └── schema.yml
│   ├── daily_weather.py (or .sql)
│   ├── meteo_daily.py (or .sql)
│   ├── open_meteo_clean.py (or .sql)
│   └── open_meteo_raw.py
├── seeds/
│   └── .
└── tests/
    └── meteo_daily_temperature_range.py (or .sql)
```

The model below fetches hourly weather data from the Open-Meteo API:

1. `dbt.config(...)` makes the model `incremental`. With `delete+insert` and `unique_key="date"`, rows for a date that is fetched again replace the existing rows instead of being added twice. The table is stored as CSV.
2. `run_date` is yesterday.
3. On the first run, the model fetches all data from `START_DATE` until `run_date`. On later runs, `dbt.is_incremental` is true and it only fetches `run_date`.
4. The API response is returned as a Polars DataFrame, with a `date` column derived from the timestamp.

The model uses the `requests` library, which is not installed with dbt-polars:

```bash
pip install requests
```

```python
# quickstart/models/open_meteo_raw.py
import polars as pl
import requests

from datetime import date, timedelta

LATITUDE = 52.54833
LONGITUDE = 13.407822
START_DATE = date(2026, 1, 1)


def model(dbt, session) -> pl.DataFrame:
    dbt.config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key="date",
        file_format="csv",
    )

    run_date = date.today() - timedelta(days=1)

    start_date = run_date if dbt.is_incremental else START_DATE

    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": start_date.isoformat(),
            "end_date": run_date.isoformat(),
            "hourly": "temperature_2m,precipitation",
        },
    )
    response.raise_for_status()
    hourly = response.json()["hourly"]

    return (
        pl.DataFrame(
            {
                "time": hourly["time"],
                "temperature_2m": hourly["temperature_2m"],
                "precipitation": hourly["precipitation"],
            }
        )
        .with_columns(pl.col("time").str.to_datetime("%Y-%m-%dT%H:%M"))
        .with_columns(pl.col("time").dt.strftime("%Y-%m-%d").alias("date"))
    )
```

Build the table using either

```bash
dbt run --full-refresh  # reload the full table
```

or

```bash
dbt run # incremental ingestion
```

dbt automatically runs a full refresh for new models. Subsequent runs are by default incremental.


<div className="alert alert--info" role="note">

In production, `run_date` usually comes from your orchestrator. Pass it as an environment variable:

```bash
RUN_DATE=2026-10-05 dbt run
```

And read it in the model:

```python
import os

run_date = date.fromisoformat(os.environ["RUN_DATE"])
```

</div>

## Further reading

* See [Python models](../guides/python-models.md#loading-data-from-an-api) for more on loading data from an API.
* See [Incremental models](https://docs.getdbt.com/docs/build/incremental-models) in the dbt documentation for the incremental strategies.

## Next steps

Your pipeline writes to the local catalog. To run the same project on another catalog, such as Azure, Iceberg or Databricks, `profiles.yml` changes. See [Connecting to a catalog](../guides/connecting-to-a-catalog.md).
