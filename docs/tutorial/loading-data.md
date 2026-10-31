---
sidebar_label: "1. Introduction & setup"
---

# Introduction & setup

In this demo project, you build a weather data pipeline with dbt-polars. Along the way, you write Python and SQL models, split transformations into steps, add data tests, and finally ingest data incrementally from an API.

## Prerequisites

The demo project starts from the project created in the [Quick start](../quickstart.md). Complete the quick start first, and run the commands of this tutorial from the quickstart folder.

Like the quick start, this tutorial uses the [local catalog](../catalogs/local.md), which writes the tables to the data folder of the project.

## Loading the data

For this example, we use hourly [weather data by Open-Meteo.com](https://open-meteo.com/). Download [open_meteo_raw.csv](https://github.com/datamindedbe/incubator-dbt-polars/raw/main/docs/demos/data/open_meteo_raw.csv) and put it in the `seeds/` folder of the project:

```text {10}
quickstart/
├── dbt_project.yml
├── models/
│   ├── example/
│   │   ├── my_first_dbt_model.sql
│   │   ├── my_second_dbt_model.sql
│   │   └── schema.yml
│   └── my_python_model.py
├── seeds/
│   └── open_meteo_raw.csv
└── tests/
```

Then load it as a table:

```bash
dbt seed
```

This writes the `open_meteo_raw` table to `data/main/`, next to the example models.

## Further reading

* See [Seeds](https://docs.getdbt.com/docs/build/seeds) in the dbt documentation for loading CSV files as tables.
* See [Source data](../guides/source-data.md) for reading raw files directly, without loading them first.
* See [Local storage](../catalogs/local.md) for all options of the local catalog.
