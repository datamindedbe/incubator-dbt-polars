# Getting started

In this guide, we create a minimal dbt-polars project which processes weather data from open-meteo stored on local disk.

## Initializing a new dbt project

Install dbt-polars using pip:

```bash
pip install dbt-polars
```

and run

```bash
dbt init
```

This will create a new folder with the name of your project.

## Creating a dbt-profile

`dbt init` already asked which catalog type to use and wrote a working profile to `~/.dbt/profiles.yml` — answer `local` when prompted, along with a schema and root path, to match this guide. That's enough to run the project as-is, so you can skip ahead to [Run your project](#run-your-project).

If you'd rather keep the profile alongside the project instead of in your global `~/.dbt/profiles.yml` — handy for a self-contained, portable demo — add a file `profiles.yml` in the project folder with the same information. A `profiles.yml` in the project folder takes precedence over the one in `~/.dbt/`, so dbt picks it up automatically:

```yaml
<your_profile_name>:
    target: dev
    outputs:
        dev:
            type: polars
            schema: dev     # this is the default schema used by dbt

            catalog_type: local  # Writing to local disk
            root: ./data    # root path where data will be written, must not contain spaces (see docs/catalogs/local.md)
```

If you wrote your own `profiles.yml`, also update `dbt_project.yml`'s `profile` key to `<your_profile_name>` (if you used `dbt init`'s profile, this already matches).

Either way, also change `dbt_project.yml`'s

```yaml
models:
  <project name>:
    example:
        +materialized: view
```

to

```yaml
models:
  <project name>:
    +materialized: table
```

This code tells dbt to materialize all models as tables unless specified otherwise. We make this change because `dbt-polars` doesn't support views.

With the profile in place, verify the connection:

```bash
dbt debug
```

## Run your project

With these changes, you can run the project using

```bash
dbt run
```

After the run, a new folder is created named `data/` which contains:

- `<schema>/my_first_dbt_model`
- `<schema>/my_second_dbt_model`

Note: the scaffolded project ships with a data test (`not_null_my_first_dbt_model_id`) that fails by default — you'll see it reported when we run `dbt test` later. It's unrelated to this guide; feel free to ignore it or delete the `models/example/` folder if you'd rather start clean.

## Processing meteo data

For this example, we use [weather data by Open-Meteo.com](https://open-meteo.com/). Download [open_meteo_raw.csv](https://github.com/datamindedbe/incubator-dbt-polars/raw/main/docs/demos/data/open_meteo_raw.csv) and put it in the seeds folder.

Run `dbt seed` to import the seed into the `data/` folder.

```bash
dbt seed
```

### Cleaning the data

The data from open-meteo is already clean, we will only rename the column `temperature_2m` to `temperature`. For this:

- Create a new file `open_meteo_clean.py`
- Add the code below

```python
import polars as pl


def model(dbt, _) -> pl.LazyFrame:
    dbt.config(
        materialized="ephemeral",
    )

    source: pl.LazyFrame = dbt.ref("open_meteo_raw")

    return source.rename(
        {
            "temperature_2m": "temperature",
        }
    )
```

This code reads the data from the `open_meteo_raw` model and renames the temperature column. Setting materialized to `ephemeral` tells dbt to not materialize this data on disk. The code will only be executed once a downstream model writes to disk.

### Aggregating data

Create another file `meteo_daily.py` in the models folder with

```python
import polars as pl


def model(dbt, _) -> pl.LazyFrame:
    source: pl.LazyFrame = dbt.ref("open_meteo_clean")

    return source.group_by(pl.col("date")).agg(
        pl.col("temperature").mean(), pl.col("precipitation").mean()
    )

```

This model uses the ephemeral `open_meteo_clean` model, aggregates temperature and precipitation by day. Use `dbt run` to materialize this table to disk.

## Adding a Python test

Our customers told us that weather data for this location should always be between -20°C and 40°C. Let's test that our data aligns

In the tests folder, add a file `test_meteo_daily_temperatures.py` with

```python
# All dates should have a temperature between -20 and 40
def test(dbt, pl) -> pl.LazyFrame:
    source: pl.LazyFrame = dbt.ref("meteo_daily")

    return source.filter((pl.col("temperature") < -20) | (pl.col("temperature") > 40))
```

By dbt's convention tests succeed when zero rows are returned. Alternatively, python tests can return a boolean with true meaning that the test succeeded.

Run this test

```bash
dbt test
```

You should see one failed test `not_null_my_first_dbt_model_id`. This is a not-null condition on `my_first_dbt_model` and is unrelated to the meteo example.

## Incremental data ingestion

Previously, we read `open_meteo_raw` as a seed. However, this approach means that we don't get access to the latest weather data.

The classical dbt approach is to do this in an ingestion pipeline and then let dbt focus on the transformations. However, with python polars model it becomes quite easy to also add the ingestion in dbt.

We will create an incremental dbt model which initially fetches all past meteo data and on future runs only adds the data of the current date.

This replaces the `open_meteo_raw` seed with a model of the same name, so first delete `seeds/open_meteo_raw.csv` — a seed and a model can't share a name. Because the new model keeps the name `open_meteo_raw`, `open_meteo_clean.py`'s existing `dbt.ref("open_meteo_raw")` needs no changes.

The example below uses the `requests` library, which isn't installed by dbt-polars itself:

```bash
pip install requests
```

Create `models/open_meteo_raw.py` with:

```python
import polars as pl
import requests

from datetime import date, datetime, timedelta

LATITUDE = 52.54833
LONGITUDE = 13.407822
START_DATE = date(2026, 1, 1)


def model(dbt, _) -> pl.DataFrame:
    dbt.config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key="date",
        file_format="csv",  # see docs/index.md#supported-file-formats for other options
    )

    # in practice use an environment variable for this
    # controlled by the process scheduling the data pipeline
    run_date = datetime.today().date() - timedelta(days=1)

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

The materialization config instructs dbt-polars to:

- Use incremental ingestion
- Delete+insert: delete all existing data for the date being ingested before adding the new rows
- Save the data in csv format

## See also

- [Catalog overview and profile reference](../index.md)
- [Local catalog](../catalogs/local.md)
- [Python models and tests](../python-models.md)
- [SQL macro support](../sql-macros.md)
