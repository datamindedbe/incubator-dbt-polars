# Fabric catalog

The Fabric catalog writes Delta tables into a Microsoft Fabric Lakehouse
through OneLake. Fabric discovers tables written under the Lakehouse's
`Tables/<schema>/<table>` folders automatically, so they show up in the
Lakehouse explorer, the SQL analytics endpoint and Power BI without any
registration step. **No Fabric compute (Spark or SQL) is used.**

**Status:** Experimental

> **Limitations (read before using):**
> - **Only schema-enabled Lakehouses are supported.** dbt schemas map to
>   Lakehouse schemas. Schemas are enabled by default for Lakehouses created in
>   the Fabric portal.
> - **Tables are always Delta.** Setting `file_format` to anything other than
>   `delta` on a model raises an error.
> - **The SQL analytics endpoint syncs asynchronously.** A table written by
>   dbt-polars is immediately readable through OneLake (and by downstream
>   dbt-polars models), but can take seconds to minutes to appear in the SQL
>   analytics endpoint.
> - **Writes are bounded by the access token's lifetime** (typically 60-90
>   minutes): the token is handed to delta-rs as-is and can't be refreshed in
>   the middle of a single write.

## Installation

```bash
pip install 'dbt-polars[fabric]'
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
          type: fabric
          workspace: my_workspace
          lakehouse: my_lakehouse
          # optional DefaultAzureCredential options - see Authentication below
```

### Configuration options

| Key | Required | Description |
|---|---|---|
| `name` | Yes | Catalog name. Used as the `database` identifier in dbt. |
| `type` | Yes | Must be `fabric`. |
| `workspace` | Yes | Fabric workspace name or ID. |
| `lakehouse` | Yes | Lakehouse name or ID. |
| *(anything else)* | No | Forwarded to `azure.identity.DefaultAzureCredential` - see Authentication below. |

## Authentication

dbt-polars authenticates as the Microsoft Entra principal running dbt, using
`DefaultAzureCredential` from `azure-identity`. It tries environment
variables (service principal), workload identity, the Azure CLI, managed
identity and more, in order. Locally, `az login` is enough.

Every key besides `name`, `type`, `workspace`, `lakehouse` and `schema` is
passed to `DefaultAzureCredential(**kwargs)`, for example:

```yaml
exclude_managed_identity_credential: true
managed_identity_client_id: "<client-id>"
```

A single token for the Azure Storage audience
(`https://storage.azure.com/.default`) is used for all OneLake calls.

## Requirements

- The workspace must be on a Fabric capacity (F or P SKU, or a Fabric
  trial). OneLake is unavailable while the capacity is paused.
- The principal needs the **Contributor** workspace role (or higher) to
  create schemas and write tables.
- The tenant setting *Users can access data stored in OneLake with apps
  external to Fabric* must be enabled (it is by default). For service
  principals, *Service principals can use Fabric APIs* must be enabled too.

## Table layout

A table is written to
`abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<schema>/<table_name>`
(or `<lakehouse_id>/Tables/...` when the Lakehouse is configured by ID), and
appears in Fabric as `<lakehouse>.<schema>.<table_name>`.

Dropping a table or schema deletes its folder in OneLake, including the
`_delta_log`. The Lakehouse's default `dbo` schema can't be dropped.

## Persisting docs

Table and column descriptions from `persist_docs` are written to the Delta
table's own metadata, which is what dbt's `docs generate` reads back. Fabric
has no API to write them into its own catalog.
