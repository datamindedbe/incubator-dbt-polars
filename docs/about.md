# About dbt-polars

dbt-polars is a dbt adapter that runs your models on [Polars](https://pola.rs) instead of inside a data warehouse. dbt compiles the project as usual; Polars executes the SQL and Python models and writes the results to storage you choose: a local folder, Azure Blob Storage, an Iceberg catalog or Databricks Unity Catalog.

## Why use dbt-polars

### No data warehouse required

Many dbt projects transform data volumes that comfortably fit on a single machine. Polars is a fast, multi-threaded DataFrame engine that processes this data wherever dbt runs: on a laptop, in a CI job, in a container or on an orchestrator worker. You only pay for the compute that runs your pipeline, and there is no warehouse to provision or keep running.

This also holds for Databricks: the [Databricks catalog](catalogs/databricks.md) registers tables in Unity Catalog through REST calls, without using Databricks compute or a SQL warehouse.

### Your data stays in open formats

Tables are written as Delta Lake (the default), Parquet, CSV, NDJSON or Iceberg. Other tools such as Spark, DuckDB, Databricks or Polars itself can read the results directly, so you are not locked into one engine. See [supported file formats](catalog-overview.md#supported-file-formats).

### Python models are first-class

In most dbt adapters, Python models run on a separate platform with its own performance characteristics and cost. In dbt-polars, SQL and Python models run on the same engine: a Python model receives Polars LazyFrames from `dbt.ref()` and `dbt.source()`, and supports the same materializations as SQL models, including incremental. You can pick the language that best fits each transformation and mix both in one project. Python singular tests are supported too. See [Python models and tests](python-models.md).

### Ingestion is part of the pipeline

Because Python models can do anything Python can, they can also fetch data from APIs. Combined with incremental materializations, this lets you build efficient ingestion as ordinary dbt models, so the whole pipeline, from source API to final table, is visible in the lineage graph. The [Getting started](demos/getting_started.md) guide includes an example.

### You keep the dbt workflow

Lineage, documentation, `ref()` and `source()`, data tests, seeds, incremental models and snapshots all work as you are used to in dbt.

### Storage is pluggable

Catalogs define where data is stored. A single profile can use several catalogs, for example to read from one storage account and write to another. If none of the built-in catalogs fit, you can [write your own](catalogs/custom.md) as a separate Python package.

## When dbt-polars is not the right fit

- **Very large datasets.** Polars runs on a single machine. If your data does not fit on one machine, a distributed engine is a better choice.
- **Views and ad-hoc querying.** dbt-polars materializes tables, incremental models and snapshots, but not views. It also does not provide a SQL endpoint for BI tools; those read the resulting tables through their own engine.
- **Warehouse-specific SQL.** SQL models run through Polars' SQL engine, which supports a subset of SQL. Some of dbt's cross-database macros are not available yet. See [SQL macro support](sql-macros.md).
- **Production-critical cloud storage.** The local and Iceberg with SQLite catalogs are usable; the other catalogs are still experimental.
