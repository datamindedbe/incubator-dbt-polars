---
sidebar_label: Databricks
pagination_prev: guides/connecting-to-a-catalog
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting to Databricks

In this guide, we connect a dbt-polars project to Databricks Unity Catalog. Polars runs on your own machine, so no Databricks compute is needed. If you don't have a project yet, the [quick start](../../quickstart.md) creates one.

## Prerequisites

- A Databricks workspace with Unity Catalog.
- On AWS, the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install). On Azure, the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

## Enabling external data access

:::info
This is a one-time setup per metastore. Skip this step if external data access is already enabled.
:::

External data access must be enabled on the metastore so that external engines like dbt-polars can write directly to storage.

As a metastore admin, enable it in Catalog Explorer:

1. In your workspace, click Catalog in the sidebar.
2. Click the gear icon at the top of the Catalog pane and select Metastore.
3. On the Details tab, turn on External data access.

See Enable external data access in the Databricks documentation for [AWS](https://docs.databricks.com/aws/en/external-access/admin) or [Azure](https://learn.microsoft.com/en-us/azure/databricks/external-access/admin) for more details.

## Creating a catalog

Create a catalog for dbt-polars, and grant your user or service principal access to it. Open the SQL editor in your workspace and run:

```sql
CREATE CATALOG dbt_polars;

GRANT USE CATALOG, CREATE SCHEMA, EXTERNAL USE SCHEMA
ON CATALOG dbt_polars TO `<principal>`;
```

Replace `<principal>` with your user's email address or the service principal's application ID. `EXTERNAL USE SCHEMA` allows dbt-polars to read and write table data outside of Databricks compute. It is not included in `ALL PRIVILEGES` or in ownership, so it is needed even when you own the catalog.

## Configuring the profile

:::info
For this tutorial, we authenticate to Databricks with our current user and save tables in Iceberg format. See the [parameter reference](../../catalogs/databricks.md#parameter-reference) for all configuration options.
:::

Install the Databricks catalog with Iceberg support:

```bash
pip install 'dbt-polars[databricks,iceberg]'
```

dbt reads its connections from `profiles.yml`, by default in the `~/.dbt` folder. Add a profile for your project, named after the profile setting in dbt_project.yml:

<Tabs groupId="cloud">
<TabItem value="aws" label="AWS" default>

```yaml
my_project:
  target: databricks
  outputs:
    databricks:
      type: polars
      schema: analytics
      catalog_type: databricks
      catalog_name: dbt_polars
      default_table_format: iceberg
      host: https://my-workspace.cloud.databricks.com
      auth_type: databricks-cli
```

Replace the host with the URL of your Databricks workspace. Then log in to your workspace with the Databricks CLI:

```bash
databricks auth login --host https://my-workspace.cloud.databricks.com
```

</TabItem>
<TabItem value="azure" label="Azure">

```yaml
my_project:
  target: databricks
  outputs:
    databricks:
      type: polars
      schema: analytics
      catalog_type: databricks
      catalog_name: dbt_polars
      default_table_format: iceberg
      host: https://adb-1234567890123456.7.azuredatabricks.net
      auth_type: azure-cli
```

Replace the host with the URL of your Databricks workspace. Then log in to Azure with the Azure CLI:

```bash
az login
```

</TabItem>
</Tabs>

## Running the project

Run the project:

```bash
dbt run
```

dbt-polars creates the analytics schema in the dbt_polars catalog and writes your models to it as managed Iceberg tables. Open the Catalog Explorer in Databricks to find them.

:::tip[Did you notice?]
We did not use any Databricks compute. Polars ran the models on your own machine, and dbt-polars registered the tables through the Unity Catalog REST API.
:::

## Adding descriptions to Unity Catalog

If you open a table in Catalog Explorer, you will see that the descriptions of your models and columns are missing. Like other dbt adapters, dbt-polars only writes descriptions to tables when persist_docs is enabled. To show them in Unity Catalog, it also needs a SQL warehouse.

First, enable persist_docs for your models in dbt_project.yml:

```yaml
models:
  my_project:
    +persist_docs:
      relation: true
      columns: true
```

Then add the HTTP path of a SQL warehouse to the databricks target in profiles.yml. You find it on the Connection details tab of the warehouse:

```yaml
    databricks:
      ...
      persist_docs_http_path: /sql/1.0/warehouses/<warehouse_id>
```

Your user needs the CAN USE permission on the warehouse. Run the project again:

```bash
dbt run
```

The descriptions now show up in Catalog Explorer. The warehouse only runs the statements that update the descriptions, and it is the only Databricks compute dbt-polars uses.

## Read more

- [External tables on Databricks](databricks-external-tables.md): store your tables as Delta tables in your own cloud storage.
- [Databricks catalog reference](../../catalogs/databricks.md): all profile parameters and authentication options.
