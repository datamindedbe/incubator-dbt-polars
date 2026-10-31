---
sidebar_label: Snowflake
pagination_prev: guides/connecting-to-a-catalog
pagination_next: guides/source-data
---

# Connecting to Snowflake

In this guide, we connect a dbt-polars project to Snowflake. dbt-polars writes Snowflake-managed Iceberg tables through Snowflake Horizon Catalog. Polars runs on your own machine, so no Snowflake warehouse is needed. If you don't have a project yet, the [quick start](../../quickstart.md) creates one.

## Prerequisites

- A Snowflake account on AWS or Azure.
- A role that can create databases and roles, such as ACCOUNTADMIN.

## Creating a database

dbt-polars writes its tables to a database that stores Iceberg tables in Snowflake-managed storage, so you don't need to set up a bucket. Open a SQL worksheet in Snowsight and run:

```sql
CREATE DATABASE dbt_polars EXTERNAL_VOLUME = SNOWFLAKE_MANAGED;

CREATE ROLE dbt_polars_role;
GRANT USAGE, CREATE SCHEMA ON DATABASE dbt_polars TO ROLE dbt_polars_role;
GRANT ROLE dbt_polars_role TO USER <user>;
```

Replace `<user>` with your Snowflake user name. dbt-polars acts as dbt_polars_role, and creates its schemas and tables in the dbt_polars database.

## Creating an access token

:::info
For this tutorial, we authenticate to Snowflake with a programmatic access token. See the [parameter reference](../../catalogs/snowflake.md#parameter-reference) for all configuration options, including OAuth and workload identity.
:::

To create an access token for your user in Snowsight:

1. Open your user menu and select Settings.
2. On the Authentication tab, under Programmatic access tokens, select Generate new token.
3. Enter a name, and restrict the token to the dbt_polars_role we created earlier.
4. Select Generate, and copy the token.

Snowflake shows the token only once. By default, Snowflake only accepts access tokens from users with a network policy. See [Programmatic access tokens](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens) if your token is rejected.

## Configuring the profile

Install the Snowflake catalog:

```bash
pip install 'dbt-polars[snowflake]'
```

dbt reads its connections from `profiles.yml`, by default in the `~/.dbt` folder. Add a profile for your project, named after the profile setting in dbt_project.yml:

```yaml
my_project:
  target: snowflake
  outputs:
    snowflake:
      type: polars
      schema: analytics
      catalog_type: snowflake
      account: myorg-myaccount
      database_name: dbt_polars
      role: dbt_polars_role
      credential: "{{ env_var('SNOWFLAKE_PAT') }}"
```

The profile reads the token from an environment variable, so it doesn't end up in profiles.yml. Set it before running dbt:

```bash
export SNOWFLAKE_PAT=<your token>
```

## Running the project

Run the project:

```bash
dbt run
```

dbt-polars creates the analytics schema in the dbt_polars database and writes your models to it as Iceberg tables. Open the database in Snowsight to find them.

:::tip[Did you notice?]
We did not use any Snowflake warehouse. Polars ran the models on your own machine, and dbt-polars registered the tables through Horizon Catalog's Iceberg REST API.
:::

## Adding descriptions

If you open a table in Snowsight, you will see that the descriptions of your models and columns are missing. Like other dbt adapters, dbt-polars only writes descriptions to tables when persist_docs is enabled. Enable it for your models in dbt_project.yml:

```yaml
models:
  my_project:
    +persist_docs:
      relation: true
      columns: true
```

Run the project again:

```bash
dbt run
```

The descriptions now show up in Snowsight.

## Read more

- [Snowflake catalog reference](../../catalogs/snowflake.md): all profile parameters, authentication options and the required permissions.
