---
title: Source data
pagination_prev: guides/connecting-to-a-catalog
---

In dbt, raw data enters a project through [sources](https://docs.getdbt.com/docs/build/sources). This page starts from how sources work in dbt, and then shows how dbt-polars extends them to read tables and files with Polars.

## Defining a source

A dbt source names a table that already exists. Each table of the source is identified by a schema and an identifier:

```yaml
# models/sources.yml
sources:
  - name: crm
    schema: crm            # defaults to the name of the source
    tables:
      - name: customers
        identifier: customers   # defaults to the name of the table
```

Models select from it with the source function, and dbt replaces it with the full name of the table:

```sql
select * from {{ source('crm', 'customers') }}
```

dbt-polars reads this source as the table crm/customers from the default catalog, in that catalog's default format, such as Delta or Iceberg. The rest of this page shows how to customize this.

## Reading files

Raw data often arrives as files, such as CSV exports, instead of tables. Set `file_format` to read a source as files:

```yaml
sources:
  - name: crm
    schema: crm
    config:
      file_format: csv
    tables:
      - name: customers
```

Besides `csv`, `file_format` can be `parquet` or `ndjson`. When the file has another name than the table, set the identifier, as in dbt:

```yaml
    tables:
      - name: customers
        identifier: customers_export   # reads crm/customers_export.csv
```

:::info
Reading files is supported by the local, Azure and S3 catalogs, because these catalogs read directly from storage.
:::

## Setting read options

dbt-polars passes `read_options` directly to the Polars scan function of the file format, so you can set any parameter of that function, such as a separator or the type of a column. See the Polars documentation of [scan_csv](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_csv.html), [scan_parquet](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_parquet.html) or [scan_ndjson](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_ndjson.html) for all parameters:

```yaml
    tables:
      - name: customers
        config:
          file_format: csv
          read_options:
            separator: ";"
            schema_overrides: {zip_code: String}
```

CSV files don't store the type of their columns, so Polars infers the types from the data. Use `schema_overrides` to explicitly define the schema. Use Polars type names to set the types, such as `String`, `Int64` or `Date`. See [Data types](https://docs.pola.rs/api/python/stable/reference/datatypes.html) in the Polars documentation for all types.

## Reading from another path

dbt has an `external` property for source tables that live outside the database. dbt-polars uses its `location` for files that don't follow the `<schema>/<identifier>.<file_format>` convention. The location starts at the catalog root, and can contain a glob to read many files as one table:

```yaml
    tables:
      - name: orders
        external:
          location: crm/orders/*.csv          # all monthly files, such as crm/orders/2026-01.csv
        config:
          file_format: csv
      - name: products
        external:
          location: erp_export/PRODUCTS.CSV   # a file outside the schema folder
        config:
          file_format: csv
```

Use `**` to also match files in subfolders, such as `crm/orders/**/*.csv`. Like `file_format`, `external.location` is only supported by the local, Azure and S3 catalogs.

## Reading from another catalog

Source data often lives in another catalog than the one your models write to. You can combine multiple catalogs in profiles.yml, and set `catalog` on a source to indicate which catalog contains it. See [Combining catalogs](combining-catalogs.md) for how to configure them.

```yaml
# profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog: warehouse
      catalogs:
        - name: warehouse
          type: databricks
          ...
        - name: landing
          type: azure
          ...
        - name: lakehouse
          type: fabric
          ...
```

The models are written to the warehouse catalog. The CRM files are in the landing catalog, and the older orders are tables in the lakehouse catalog:

```yaml
# models/sources.yml
sources:
  - name: crm
    catalog: landing
    schema: crm
    config:
      file_format: csv
    tables:
      - name: orders
        ...

  - name: archive
    catalog: lakehouse
    schema: sales
    tables:
      - name: orders
```

A model can combine sources from different catalogs:

```sql
select * from {{ source('crm', 'orders') }}
union all
select * from {{ source('archive', 'orders') }}
```

## Read more

- [Source properties](https://docs.getdbt.com/reference/source-properties): all source properties in the dbt documentation.
- [Model configuration](../model-configuration.md): file_format, read_options and the other configurations of dbt-polars.
