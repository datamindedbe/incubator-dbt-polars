---
sidebar_label: Iceberg
pagination_prev: guides/connecting-to-a-catalog
pagination_next: guides/source-data
---

# Connecting to an Iceberg catalog

In this guide, we connect a dbt-polars project to an Iceberg catalog. We will use a local SQLite database as the catalog, and write the models of the project as Iceberg tables to a folder on your disk. If you don't have a project yet, the [quick start](../../quickstart.md) creates one.

The Iceberg catalog works with any catalog that pyiceberg supports, such as REST, Glue or Hive. A local SQLite catalog needs no cloud account, which makes it a good way to try out Iceberg.

:::warning
You can use the Iceberg catalog to connect to the Iceberg REST endpoint of Databricks, Fabric or Snowflake. We recommend the dedicated [Databricks](databricks.md), [Fabric](fabric.md) or [Snowflake](snowflake.md) catalog instead. These handle authentication for you, and come with platform-specific features.
:::

## Configuring the profile

:::info
For this tutorial, we use a SQLite catalog and store tables on your local disk. See the [parameter reference](../../catalogs/iceberg.md#parameter-reference) for all configuration options.
:::

Install the Iceberg catalog:

```bash
pip install 'dbt-polars[iceberg]'
```

dbt reads its connections from `profiles.yml`, by default in the `~/.dbt` folder. Add a profile for your project, named after the profile setting in dbt_project.yml:

```yaml
my_project:
  target: iceberg
  outputs:
    iceberg:
      type: polars
      schema: analytics
      catalog_type: iceberg
      uri: sqlite:///catalog.db
      warehouse: file://warehouse
```

uri is the SQLite database that holds the catalog, and warehouse is the folder the table data is written to. Both paths are relative to your project folder.

## Running the project

Run the project:

```bash
dbt run
```

dbt-polars registers each model in catalog.db, and writes it as an Iceberg table to the warehouse folder, in a folder per schema:

```text
my_project/
├── catalog.db                   # catalog
└── warehouse/
    └── analytics/               # schema
        └── <model>/             # table
            ├── data/
            │   └── 00000-0-<uuid>.parquet
            └── metadata/
                ├── 00000-<uuid>.metadata.json
                ├── <uuid>-m0.avro
                └── snap-<id>-0-<uuid>.avro
```

## Read more

- [Iceberg catalog reference](../../catalogs/iceberg.md): all profile parameters, and examples for other catalogs, such as REST.
