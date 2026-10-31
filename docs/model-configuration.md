---
title: Model configuration
---

dbt-polars supports the standard [model configurations](https://docs.getdbt.com/reference/model-configs) of dbt, such as materialized, unique_key, incremental_strategy and on_schema_change. This page describes the configurations that dbt-polars adds, and the catalogs that support them.

## Setting a configuration

Set a configuration on a single model with `config`:

```sql
{{ config(materialized="table", file_format="parquet") }}
```

Or set it in dbt_project.yml, for all models of the project or of a folder:

```yaml
models:
  my_project:
    +file_format: parquet
```

## Overview

The configurations below are specific to dbt-polars, or work differently than in other adapters.

* [catalog](#catalog): the catalog the model is written to.
* [file_format](#file_format): the format of the stored files, such as Delta or Parquet.
* [table_format](#table_format): the table format, Delta or Iceberg, on Databricks and Fabric.
* [partition_by](#partition_by): the columns to partition the table by.
* [write_options](#write_options): options passed to the function that writes the table.
* [write_mode](#write_mode): whether the result is streamed to storage or collected in memory first.
* [read_options](#read_options): options for reading the stored files of the model.
* [merge_update_columns and merge_exclude_columns](#merge_update_columns-and-merge_exclude_columns): the columns a merge updates.
* [incremental_predicates](#incremental_predicates): extra conditions for merge and delete+insert.

## catalog

The catalog the model is written to. Without it, the model is written to the default catalog of the profile.

```sql
{{ config(catalog="landing") }}
```

* Supported by all catalogs.

See [Combining catalogs](guides/combining-catalogs.md).

## file_format

The format of the stored files: `delta` (default), `parquet`, `csv` or `ndjson`.

```sql
{{ config(file_format="parquet") }}
```

Only Delta tables support partitioning and incremental predicates. Incremental models in the other formats read the existing table in memory.

* Supported by the local, Azure and S3 catalogs.
* Not supported by the Databricks and Fabric catalogs, which use `table_format` instead, or the Snowflake and Iceberg catalogs, which always write Iceberg tables.

## table_format

The format of the table: `delta` or `iceberg`. It overrides the `default_table_format` of the profile.

```sql
{{ config(table_format="iceberg") }}
```

When an existing table has another format, a table model recreates it, and an incremental model requires `--full-refresh`.

* Supported by the Databricks and Fabric catalogs.
* Not supported by the other catalogs, which each write a single table format.

## partition_by

The columns to partition the table by, as a column name or a list of column names.

```sql
{{ config(partition_by=["order_date"]) }}
```

* Supported by Delta and Iceberg tables in all catalogs.
* Not supported by tables stored as Parquet, CSV or NDJSON.

## write_options

Options passed to the function that writes the table. For Delta tables, these are the parameters of [sink_delta](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_delta.html), and for Parquet, CSV and NDJSON files the parameters of the matching sink function, such as [sink_parquet](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html):

```sql
{{ config(file_format="parquet", write_options={"compression": "zstd"}) }}
```

For Iceberg tables, write_options sets the `batch_size`, see [Writing Iceberg tables in batches](guides/performance.md#writing-iceberg-tables-in-batches).

* Supported by all catalogs.

## write_mode

How the result is written: `lazy` (default) streams the result to storage, while `eager` collects the full result in memory first.

```sql
{{ config(write_mode="eager") }}
```

* Supported by Delta and Iceberg tables in all catalogs.
* Not supported by tables stored as Parquet, CSV or NDJSON, which are always streamed.

## read_options

Options passed to the Polars scan function when other models read the stored files of this model, such as [scan_csv](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_csv.html):

```sql
{{ config(file_format="csv", read_options={"separator": ";"}) }}
```

* Supported by tables stored as Parquet, CSV or NDJSON in the local, Azure and S3 catalogs.
* Not supported by Delta and Iceberg tables.

## merge_update_columns and merge_exclude_columns

The columns to update, or not to update, when the merge incremental strategy finds an existing row with the same unique key. By default, all columns are updated.

```sql
{{ config(
    materialized="incremental",
    unique_key="id",
    merge_exclude_columns=["created_at"],
) }}
```

* Supported by all catalogs.

## incremental_predicates

Extra conditions for the merge and delete+insert incremental strategies, to limit which existing rows they touch. Refer to the existing table as `DBT_INTERNAL_DEST`, and to the new rows as `DBT_INTERNAL_SOURCE`:

```sql
{{ config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="id",
    incremental_predicates=["DBT_INTERNAL_DEST.order_date >= '2026-01-01'"],
) }}
```

* Supported by Delta tables in all catalogs.
* Not supported by Iceberg tables, or tables stored as Parquet, CSV or NDJSON.
