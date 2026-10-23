# Databricks catalog

Register Delta or Iceberg tables in Databricks Unity Catalog via the Databricks REST API.

## Supported features

| Feature | Status | Remarks |
|---|---|---|
| Read/write Delta tables | ✅ | Creates external Databricks tables |
| Read/write Iceberg tables | ✅ | Creates managed Databricks tables |
| Push table and column descriptions to Unity Catalog | ✅ | Requires a SQL warehouse, see [`persist_docs_http_path`](#persist_docs_http_path) |
| Read/write Databricks views | ❌️ | |

## Prerequisites

* A Databricks workspace with Unity Catalog, with
  [external data access](https://docs.databricks.com/aws/external-access/admin)
  enabled on its metastore.
* A Unity Catalog catalog with a managed storage location (`CREATE CATALOG ...
  MANAGED LOCATION '...'`). Delta tables are written to its storage root.
* Optionally, a SQL warehouse to push descriptions to Unity Catalog. No other
  Databricks compute is used.

## Installation

```bash
pip install 'dbt-polars[databricks]'          # Delta tables
pip install 'dbt-polars[databricks,iceberg]'  # Delta and Iceberg tables
```

## Profile configuration

### Example

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog_type: databricks
      schema: dev
      catalog_name: my_uc_catalog
      host: https://my-workspace.cloud.databricks.com
      default_table_format: delta
      # persist_docs_http_path: /sql/1.0/warehouses/<warehouse_id>
      client_id: "{{ env_var('DATABRICKS_CLIENT_ID') }}"
      client_secret: "{{ env_var('DATABRICKS_CLIENT_SECRET') }}"
      auth_type: oauth-m2m
```

To combine this catalog with others, put the same parameters in an entry of
`catalogs`, with `type: databricks` and a `name`, see
[Catalog overview](../catalog-overview.md).

### Parameter Reference

The following parameters are supported:

* `catalog_type` - (Required) Must be `databricks`.

* `catalog_name` - (Required) The Unity Catalog catalog that holds the tables.
  It must have a managed storage location (`CREATE CATALOG ... MANAGED
  LOCATION '...'`).

* `schema` - (Required) Default schema for models. dbt-polars creates missing
  schemas.

* `host` - (Optional) Workspace URL, e.g.
  `https://adb-<workspace-id>.<number>.azuredatabricks.net` on Azure or
  `https://<deployment-name>.cloud.databricks.com` on AWS. When omitted, `databricks-sdk`
  attempts to resolve it from the environment (`DATABRICKS_HOST`, a `databricks` CLI
  profile, ...).

* `default_table_format` - (Optional) Format of new tables: `delta` or
  `iceberg`. Defaults to `delta`. Models can override it by setting `table_format`.

:::info
Delta tables are created as `EXTERNAL` (unmanaged) tables in the catalog's
storage root. Iceberg tables are created as `MANAGED` tables, whose storage
Unity Catalog manages.
:::


* <a id="persist_docs_http_path"></a>`persist_docs_http_path` - (Optional) HTTP path of a SQL warehouse, e.g.
    `/sql/1.0/warehouses/<warehouse_id>`. Used to update table and column
    descriptions in Unity Catalog. When not set, descriptions are only stored
    in the table's own metadata and don't show up in Unity Catalog.

* Authentication parameters - (Optional) Passed to
  [`databricks.sdk.Config`](https://github.com/databricks/databricks-sdk-py/blob/main/docs/auth-types-reference.md),
  so every authentication method of `databricks-sdk` is supported. When none
  are set, `databricks-sdk` attempts to find credentials in the environment.
  Delta and Iceberg tables use the same credentials. Commonly used:
  * `token` - Personal access token.
  * `client_id`, `client_secret` - OAuth credentials of a Databricks service
    principal, with `auth_type: oauth-m2m`.
  * `azure_client_id`, `azure_client_secret`, `azure_tenant_id` - Microsoft
    Entra ID service principal, on Azure Databricks.
  * `auth_type` - Forces one authentication method, e.g. `pat`, `oauth-m2m`,
    `azure-client-secret`, `azure-cli` or `databricks-cli`.
  * `profile` - Profile in `~/.databrickscfg` to read the host and credentials
    from.

## Model configuration

The following custom model parameters are available for Databricks:

* `table_format` - (Optional) `delta` or `iceberg`. Overrides the profile's
  `default_table_format`. When the existing table has another format, a
  `table` model recreates it and an incremental model requires
  `--full-refresh`.

  ```sql
  {{ config(materialized='table', table_format='iceberg') }}
  ```

## Required grants

The principal dbt-polars authenticates as needs these grants:

| Privilege | On | Needed for |
|---|---|---|
| `USE CATALOG`, `CREATE SCHEMA` | the catalog | Creating schemas |
| `EXTERNAL USE SCHEMA` | the catalog | Reading and writing table data |
| `EXTERNAL USE LOCATION`, `CREATE EXTERNAL TABLE` | the external location of the catalog's storage root | Creating Delta tables |
| `USE SCHEMA`, `CREATE TABLE` | schemas dbt-polars didn't create | Creating tables in them |
| `SELECT` | tables dbt-polars doesn't own | Reading them, e.g. as a source |

`EXTERNAL USE SCHEMA`, `EXTERNAL USE LOCATION` and `CREATE EXTERNAL TABLE` are
not included in `ALL PRIVILEGES` or ownership, so grant them explicitly. Grant
`EXTERNAL USE SCHEMA` on the catalog: dbt-polars creates its own schemas, and
only the catalog owner can grant the privilege on them.
