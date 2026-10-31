---
sidebar_label: Fabric
pagination_prev: guides/connecting-to-a-catalog
pagination_next: guides/source-data
---

# Connecting to Fabric

In this guide, we connect a dbt-polars project to a Microsoft Fabric Lakehouse. Polars runs on your own machine, so no Spark session or SQL endpoint is needed. If you don't have a project yet, the [quick start](../../quickstart.md) creates one.

## Prerequisites

- A Fabric workspace on a Fabric capacity, such as an F or P SKU or a Fabric trial. The capacity must be running, because OneLake is unavailable while it's paused.
- The Contributor role, or higher, on the workspace.
- The [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

## Creating a Lakehouse

dbt-polars writes its tables to a Lakehouse with schemas enabled. In your Fabric workspace:

1. Select New item, then Lakehouse.
2. Enter a name, for example `my_lakehouse`.
3. Keep Lakehouse schemas enabled, which is the default for new Lakehouses, and select Create.

## Configuring the profile

:::info
For this tutorial, we authenticate to Fabric with our current user and save tables in Delta format. See the [parameter reference](../../catalogs/fabric.md#parameter-reference) for all configuration options.
:::

Install the Fabric catalog:

```bash
pip install 'dbt-polars[fabric]'
```

dbt reads its connections from `profiles.yml`, by default in the `~/.dbt` folder. Add a profile for your project, named after the profile setting in dbt_project.yml:

```yaml
my_project:
  target: fabric
  outputs:
    fabric:
      type: polars
      schema: analytics
      catalog_type: fabric
      workspace: my_workspace
      lakehouse: my_lakehouse
```

Replace the workspace and lakehouse with your own. Then log in to Azure with the Azure CLI:

```bash
az login
```

## Running the project

Run the project:

```bash
dbt run
```

dbt-polars creates the analytics schema in the Lakehouse and writes your models to it as Delta tables. Open the Lakehouse in Fabric to find them under Tables.

:::tip[Did you notice?]
We did not use any Spark or SQL compute. Polars ran the models on your own machine and wrote the tables to OneLake, where Fabric discovers them automatically.
:::

## Read more

- [Fabric catalog reference](../../catalogs/fabric.md): all profile parameters and authentication options.
