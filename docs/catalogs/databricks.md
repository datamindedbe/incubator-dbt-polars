# Databricks catalog

The Databricks catalog registers tables in Unity Catalog via the Unity Catalog
REST API, using Unity Catalog credential vending to read/write table data
directly with polars/deltalake. **No Databricks compute or SQL warehouse is
used for reads/writes or table registration** - only REST calls to the
workspace API. A SQL warehouse is only used, optionally, to persist docs into
Unity Catalog's native comment fields (see Persisting docs below).

**Status:** Experimental

> **Limitations (read before using):**
> - **dbt-polars only ever creates/writes `EXTERNAL` Delta tables.** Reading a
>   pre-existing table (e.g. via a dbt `source()`) works regardless of whether
>   it's external or Unity-Catalog-managed, since table-based credential
>   vending supports both - this restriction only applies to tables
>   dbt-polars itself creates.
> - **The target catalog must already have a managed storage location**
>   (e.g. created with `CREATE CATALOG ... MANAGED LOCATION '...'`). New
>   tables' paths are derived from the catalog's own storage root plus the
>   schema and table name, rather than taking a location in profile config.
> - **`file_format` is not configurable for this catalog.** Tables are always
>   registered as Delta in Unity Catalog; setting `file_format` to anything
>   other than `delta` on a model raises an error.

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
          # persist_docs_http_path: /sql/1.0/warehouses/<warehouse_id>  # optional, see Persisting docs
```

### Configuration options

| Key | Required | Description |
|---|---|---|
| `name` | Yes | Catalog name. Used as the `database` identifier in dbt. |
| `type` | Yes | Must be `databricks`. |
| `catalog_name` | Yes | Name of the Unity Catalog catalog to register tables in. |
| `host` | No | Workspace URL. Omit to let `databricks-sdk` resolve it from the environment. |
| `persist_docs_http_path` | No | A SQL warehouse's HTTP path (see Persisting docs below). |
| *(anything else)* | No | Forwarded to `databricks.sdk.Config` - see Authentication below. |

## Authentication

Every keyword besides `name`, `type`, `catalog_name`, `host`, `schema`, and
`persist_docs_http_path` is passed straight through to
`databricks.sdk.Config(**kwargs)`, so any auth method it supports works,
matching (and exceeding) what `dbt-databricks` itself supports:

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
  backing the catalog's managed storage root (needed to bootstrap the first
  write of each new table, before it has a Unity Catalog table id).
- `external_access_enabled` on the metastore.
- The usual `USE CATALOG` / `USE SCHEMA` / `CREATE SCHEMA` privileges.

`EXTERNAL USE SCHEMA`, `EXTERNAL USE LOCATION`, and `CREATE EXTERNAL TABLE`
must each be granted explicitly to the principal dbt-polars authenticates as -
they are not satisfied by `ALL PRIVILEGES` or by catalog/owner status.

## Table layout

A new table is written to `<catalog storage_root>/<schema>/<table_name>` and
registered in Unity Catalog as `<catalog_name>.<schema>.<table_name>`.

Dropping a table (or schema) unregisters it from Unity Catalog and vacuums its
underlying storage with no retention, physically removing the data files.
The small `_delta_log` transaction-log files are left behind - Delta has no
operation that removes those.

## Persisting docs

Table/column descriptions from dbt's `persist_docs` are always written to the
Delta table's own metadata - this is what dbt's `docs generate`/`catalog.json`
reads back, and needs no configuration.

Unity Catalog's own native comment fields are a separate thing: its REST API
has no endpoint to set or update them at all, for either tables or columns -
the only way is `COMMENT ON TABLE` / `ALTER TABLE ... ALTER COLUMN ...
COMMENT` SQL, which needs compute. If you set `persist_docs_http_path` to a
SQL warehouse's HTTP path (shown on the warehouse's Connection Details page,
e.g. `/sql/1.0/warehouses/<warehouse_id>`), dbt-polars runs that SQL for you
whenever a comment actually changes. It first checks the current value via
REST and skips the SQL warehouse entirely when nothing changed, so a normal
`dbt run` with unchanged docs never starts it up. Leave it unset to keep this
catalog fully compute-free; Unity Catalog's own comment fields then simply
never get populated.

## Orphaned storage

dbt-polars only cleans up storage through its own `drop_relation`/`drop_schema`
code path (see Table layout above). If a table gets removed some other way -
deleted in the Databricks UI, dropped via SQL from a notebook, deleted by
another tool entirely - its data files are left behind with no built-in
detection, since dbt-polars has no record that it ever existed.

An automated cleanup/audit command (list a schema's storage, diff against
what's registered in Unity Catalog, delete the orphans) was investigated but
turned out not to be achievable through the REST-plus-credential-vending
model this catalog is built on, for two independent reasons:

- Unity Catalog's path-based credential vending refuses catalog-root and
  schema-root paths for *any* operation, including plain `PATH_READ` - it
  rejects them as overlapping with managed storage. Without read access at
  that granularity there is no way to list a schema's actual storage
  contents via REST, so orphans can't be discovered in the first place.
- Even for a single, already-known table path, deleting its files with
  path-vended credentials failed on Azure with a `403 Signed Directory Depth
  Invalid` error during `vacuum`'s bulk delete - the vended SAS token's
  fixed directory-depth signature doesn't cover the depth a batch delete
  touches.

A blind, time-based storage lifecycle policy (e.g. "delete anything untouched
for 90 days") is not a safe substitute either: Delta's data files are never
modified after they're written, so file age can't tell an abandoned table
apart from a stable table that's simply being read, not rewritten.

For now, cleaning up orphaned storage is a manual step - review and delete it
directly through the Databricks UI or your cloud provider's console/CLI.
