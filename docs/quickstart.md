---
mdx:
  format: mdx
---

# Quick start

In this quick start, you create a minimal dbt-polars project. The project runs locally and saves data to your disk, so you don't need a cloud account.

## Creating a dbt-polars project

Make sure you have a supported Python version installed (3.10, 3.11, 3.12 or 3.13) and your current working directory is the one where you intend to create the project. Start by installing dbt-polars:

```bash
pip install dbt-polars
```

This command installs both dbt-core and dbt-polars. dbt-polars acts as an adapter that enables dbt to process models in Polars. Next, initialize a new project:

```bash
dbt init
```

This scaffolds a new dbt project and shows a few prompts to configure it. For this quick start, answer:

```text
project name:  quickstart
database:      polars
schema:        main     # default
catalog type:  local    # write tables to your local disk
root:          ./data   # default, the folder your tables are written to
```

:::info[Catalogs]

Polars has no default catalog, so dbt-polars connects to an external one to store tables. This quick start uses the local catalog, which writes to your local disk. See [Connecting to a catalog](guides/connecting-to-a-catalog.md) for all options.

:::

dbt init creates the project in a new quickstart folder, with two example SQL models in models/example. See [About dbt projects](https://docs.getdbt.com/docs/build/projects) in the dbt documentation to learn more about the project structure.

```text
quickstart/
├── dbt_project.yml   # project settings, including which profile to use
├── models/           # SQL and Python models
│   └── example/      # example models created by dbt init
├── seeds/            # CSV files loaded with dbt seed
├── tests/            # singular data tests
├── macros/           # reusable SQL (Jinja) snippets
├── snapshots/        # snapshot definitions
└── analyses/         # SQL queries that are compiled but not run
```

One change is needed before the project runs. Open `dbt_project.yml` and change the default materialization from `view` to `ephemeral` in the models block at the end of the file:

<div className="row">
<div className="col col--6">

**Created by dbt init**

```yaml
models:
  quickstart:
    example:
      +materialized: view
```

</div>
<div className="col col--6">

**Replace with**

```yaml
models:
  quickstart:
    +materialized: ephemeral
```

</div>
</div>

We make this change because dbt-polars does not support views. A view is stored and executed by a warehouse, and Polars has no default warehouse. Setting the default materialization to `ephemeral` keeps the models unmaterialized by default. Alternatively, you could set the default materialization to `table` which stores all models on disk.

## Running your first models

From the quickstart folder, run:

```bash
dbt run
```

dbt compiles the example models and dbt-polars executes them with Polars. The [local catalog](catalogs/local.md) writes each table to the root folder you chose during dbt init, in a subfolder per schema. The following files are created:

```text
quickstart/
└── data/                        # root
    └── main/                    # schema
        └── my_first_dbt_model/  # table
            ├── _delta_log/
            │   └── 00000000000000000000.json
            └── part-00001-<uuid>-c000.snappy.parquet
```



## Adding a Python model

The project created by dbt init comes with two SQL models. In this section, we show how to add a Python model to the project.

Start by creating a new file `my_python_model.py` in the models folder:

```python
# quickstart/models/my_python_model.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    dbt.config(materialized="table")  # store the result as a table

    second: pl.LazyFrame = dbt.ref("my_second_dbt_model")
    return second.with_columns((pl.col("id") * 10).alias("id_times_ten"))
```

Python and SQL models can reference each other. In this example, the Python model references `my_second_dbt_model`, which is an ephemeral SQL model. `dbt.config(materialized="table")` instructs dbt to save the model as a table instead of an ephemeral model.

Run the project again:

```bash
dbt run
```

The data folder now also contains my_python_model.

```text
quickstart/
└── data/
    └── main/
        ├── my_first_dbt_model/
        └── my_python_model/
```

## Next steps

You have now built a dbt project with both SQL and Python models. Every model ran on your own computer. Polars processed the data and the local catalog wrote the tables to your disk, so no data warehouse was needed. From here, you can continue with:

- [Demo project](tutorial/loading-data.md): a guided tutorial that sets up a weather data processing pipeline. Along the way, it shows features of both dbt and dbt-polars, which makes it a good start if you are new to dbt.
- [Essentials](guides/connecting-to-a-catalog.md): already familiar with dbt? The Essentials guides highlight what is different in dbt-polars and what you need to know to use it.
