# Fabric catalog

Write Delta or Iceberg tables to a Microsoft Fabric Lakehouse via OneLake.

:::warning Fabric capacity must be running
The workspace's Fabric capacity must be running. OneLake is unavailable while
the capacity is paused, so every read and write fails.
:::

## Supported features

| Feature | Status | Remarks |
|---|---|---|
| Read/write Delta tables | ✅ | Fabric discovers them in the Lakehouse automatically |
| Read/write Iceberg tables | ✅ | Through OneLake's Iceberg REST catalog |
| Table descriptions in Fabric | 🟡 | Shown for Delta tables only, from `persist_docs` |
| Column descriptions in Fabric | ❌️ | Fabric doesn't display them |

## Installation

```bash
pip install 'dbt-polars[fabric]'          # Delta tables
pip install 'dbt-polars[fabric,iceberg]'  # Delta and Iceberg tables
```

## Profile configuration

### Example

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog_type: fabric
      schema: dev
      workspace: my_workspace
      lakehouse: my_lakehouse
      default_table_format: delta
```

To combine this catalog with others, put the same parameters in an entry of
`catalogs`, with `type: fabric` and a `name`, see
[Catalog overview](../catalog-overview.md).

### Parameter Reference

The following parameters are supported:

* `catalog_type` - (Required) Must be `fabric`.

* `workspace` - (Required) Name or ID of the Fabric workspace.

* `lakehouse` - (Required) Name or ID of the Lakehouse. It must have schemas
  enabled, which is the default for new Lakehouses. Use IDs when a name
  contains spaces or special characters.

* `schema` - (Required) Default schema for models. dbt-polars creates missing
  schemas. The Lakehouse's default schema `dbo` can't be dropped.

* `default_table_format` - (Optional) Format of new tables: `delta` or
  `iceberg`. Defaults to `delta`. Models can override it by setting
  `table_format`.

* Authentication parameters - (Optional) Passed to
  [`azure.identity.DefaultAzureCredential`](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential),
  which tries environment variables, workload identity, the Azure CLI,
  managed identity and more, in that order. Locally, `az login` is enough.
  Delta and Iceberg tables use the same credentials. Commonly used:
  * `exclude_managed_identity_credential` - Skip managed identity, e.g. on a
    machine where it isn't the identity you want.
  * `managed_identity_client_id` - Client ID of a user-assigned managed
    identity.
  * `exclude_interactive_browser_credential` - Set to `false` to allow a
    browser login.

  For a service principal, set `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` and
  `AZURE_TENANT_ID` in the environment.

## Model configuration

The following custom model parameters are available for Fabric:

* `table_format` - (Optional) `delta` or `iceberg`. Overrides the profile's
  `default_table_format`. When the existing table has another format, a
  `table` model recreates it and an incremental model requires
  `--full-refresh`.

  ```sql
  {{ config(materialized='table', table_format='iceberg') }}
  ```

## Required permissions

- The principal dbt-polars authenticates as needs the **Contributor** role, or
  higher, on the workspace.
- The tenant setting *Users can access data stored in OneLake with apps
  external to Fabric* must be enabled. It is by default.
- For a service principal, the tenant setting *Service principals can use
  Fabric APIs* must be enabled too.
