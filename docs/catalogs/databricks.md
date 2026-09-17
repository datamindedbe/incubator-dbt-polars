# Databricks catalog

The Databricks catalog registers tables in Unity Catalog via the Unity Catalog
REST API, using Unity Catalog credential vending to read/write table data
directly with polars/deltalake. **No Databricks compute or SQL warehouse is
used at any point** - only REST calls to the workspace API.

**Status:** Experimental

> **Limitations (read before using):**
> - **dbt-polars only ever creates/writes `EXTERNAL` Delta tables.** Reading a
>   pre-existing table (e.g. via a dbt `source()`) works regardless of whether
>   it's external or Unity-Catalog-managed, since table-based credential
>   vending supports both - this restriction only applies to tables
>   dbt-polars itself creates.
> - **The target catalog/schema must already have a managed storage location**
>   (e.g. created with `CREATE CATALOG/SCHEMA ... MANAGED LOCATION '...'`).
>   dbt-polars derives new tables' paths from the schema's `storage_root`
>   rather than taking a location in its own profile config.
> - **Unity Catalog's own registered comment/column comments are only ever set
>   when a table is first registered.** The Tables REST API has no update
>   endpoint, so if a model's docs change afterward, dbt-polars logs a warning
>   containing the exact `COMMENT ON TABLE` / `ALTER TABLE ... ALTER COLUMN ...
>   COMMENT` SQL you can run manually (on a SQL warehouse) to bring Unity
>   Catalog's copy back in sync. The underlying Delta table's own metadata -
>   which is what dbt's `docs generate`/`catalog.json` reads back - always
>   stays current regardless.

## Installation

```bash
pip install 'dbt-polars[databricks]'
```

## Profile configuration

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      schema: dev
      catalogs:
        - name: my_catalog
          type: databricks
          catalog_name: my_uc_catalog
          host: https://my-workspace.cloud.databricks.com
          # auth options - see Authentication below
```

### Configuration options

| Key | Required | Description |
|---|---|---|
| `name` | Yes | Catalog name. Used as the `database` identifier in dbt. |
| `type` | Yes | Must be `databricks`. |
| `catalog_name` | Yes | Name of the Unity Catalog catalog to register tables in. |
| `host` | No | Workspace URL. Omit to let `databricks-sdk` resolve it from the environment. |
| *(anything else)* | No | Forwarded to `databricks.sdk.Config` - see Authentication below. |

## Authentication

Every keyword besides `name`, `type`, `catalog_name`, and `schema` is passed
straight through to `databricks.sdk.Config(**kwargs)`, so any auth method it
supports works, matching (and exceeding) what `dbt-databricks` itself supports:

```yaml
# Personal access token
token: "<pat>"

# OAuth machine-to-machine
client_id: "<client-id>"
client_secret: "<client-secret>"
auth_type: oauth-m2m

# Azure service principal
azure_client_id: "<client-id>"
azure_client_secret: "<client-secret>"
azure_tenant_id: "<tenant-id>"
```

`databricks-sdk` also auto-probes PAT/OAuth-M2M/Azure-CLI/`databricks` CLI
profile/GitHub OIDC/etc. from environment variables when `auth_type` is
omitted - see the
[databricks-sdk auth types reference](https://github.com/databricks/databricks-sdk-py/blob/main/docs/auth-types-reference.md).

## Required Unity Catalog grants

- `EXTERNAL USE SCHEMA` on each schema this catalog reads or writes.
- `EXTERNAL USE LOCATION` and `CREATE EXTERNAL TABLE` on the external location
  backing the catalog/schema's managed storage root (needed to bootstrap the
  first write of each new table, before it has a Unity Catalog table id).
- `external_access_enabled` on the metastore.
- The usual `USE CATALOG` / `USE SCHEMA` / `CREATE SCHEMA` privileges.

`EXTERNAL USE SCHEMA`, `EXTERNAL USE LOCATION`, and `CREATE EXTERNAL TABLE`
must each be granted explicitly to the principal dbt-polars authenticates as -
they are not satisfied by `ALL PRIVILEGES` or by catalog/owner status.

## Table layout

A new table is written to `<schema storage_root>/<table_name>` and registered
in Unity Catalog as `<catalog_name>.<schema>.<table_name>`.
