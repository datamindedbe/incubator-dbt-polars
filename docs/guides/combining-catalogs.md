# Combining catalogs

A dbt-polars project can read from and write to several catalogs at the same time. For example, raw files can land in Azure Blob Storage, while the models are written to Databricks Unity Catalog. Models in different catalogs can reference each other with `ref`, as if they were in the same catalog.

This guide shows how to define several catalogs in a profile, and how to choose the catalog of each model, seed and source.

## Defining catalogs in the profile

To use several catalogs, list them under `catalogs` in the profile. Each catalog in the list has a name, a type, a default schema, and the parameters of its type. The profile below combines a Databricks catalog for the models with an Azure catalog for the raw files:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog: warehouse
      catalogs:
        - name: warehouse
          type: databricks
          schema: analytics
          catalog_name: dbt_polars
          host: https://adb-1234567890123456.7.azuredatabricks.net
          auth_type: azure-cli

        - name: landing
          type: azure
          schema: raw
          account_name: <mystorageaccount>
          container: landing
```

The `catalog` key defines the default catalog, where models are written unless they set their own. When omitted, the first catalog in the list is the default. Each catalog defines its own `schema`, so the profile has no top-level `schema`.

An entry takes the same parameters as a single catalog in the profile, with `type` instead of `catalog_type`. See the reference page of each catalog for its parameters.

## Choosing the catalog of a model

Set `catalog` on a model to write it to a catalog other than the default:

```sql
{{ config(catalog="landing") }}

select * from {{ ref('stg_orders') }}
```

To set the catalog of a whole folder of models, or of all seeds, use `+catalog` in dbt_project.yml:

```yaml
models:
  my_project:
    staging:
      +catalog: landing

seeds:
  my_project:
    +catalog: landing
```

## Referencing models across catalogs

Models reference each other with `ref`, in whichever catalog they are. In the example below, stg_orders is written to the landing catalog, while customer_orders is written to the warehouse catalog:

```sql
-- models/stg_orders.sql
{{ config(catalog="landing") }}

select * from {{ source('crm', 'orders') }}
```

```sql
-- models/customer_orders.sql
{{ config(catalog="warehouse") }}

select customer_id, count(*) as orders
from {{ ref('stg_orders') }}
group by customer_id
```

dbt-polars reads stg_orders from the landing catalog, and writes customer_orders to the warehouse catalog. The `ref` doesn't need to know where stg_orders is stored.

## Choosing the catalog of a source

Set `catalog` on a source to read it from a catalog other than the default:

```yaml
sources:
  - name: crm
    catalog: landing
    schema: crm
    tables:
      - name: customers
```

See [Source data](source-data.md) for all the ways to read source data.

## Read more

* [Source data](source-data.md): reading tables and files as sources.
