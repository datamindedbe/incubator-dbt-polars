# Azure catalog

The Azure catalog stores tables as files on Azure Blob Storage or Azure Data Lake Storage Gen2 (ADLS Gen2).

**Status:** Experimental

## Installation

```bash
pip install 'dbt-polars[azure]'
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
          type: azure
          account_name: mystorageaccount
          container: mycontainer
          prefix: optional/path/prefix   # optional; prepended to all paths
          credentials:
            # see Authentication section below
```

### Configuration options

| Key | Required | Description |
|---|---|---|
| `name` | Yes | Catalog name. Used as the `database` identifier in dbt. |
| `type` | Yes | Must be `azure`. |
| `account_name` | Yes | Azure Storage account name. |
| `container` | Yes | Blob container / file system name. |
| `prefix` | No | Path prefix prepended to all schema and table paths. |
| `credentials` | No | Authentication options (see below). Omit to use `DefaultAzureCredential`. |

## Authentication

The `credentials` dict accepts three options. If `credentials` is omitted entirely, `DefaultAzureCredential` from the `azure-identity` package is used, which tries environment variables, workload identity, the Azure CLI, and managed identity in order.

### DefaultAzureCredential (default)

Omit `credentials` or pass keyword arguments forwarded to `DefaultAzureCredential`:

```yaml
credentials:
  exclude_managed_identity_credential: true   # any DefaultAzureCredential kwarg
```

### Bearer token

```yaml
credentials:
  bearer_token: "<token>"
```

### Account key

```yaml
credentials:
  account_key: "<key>"
```

### SAS token

```yaml
credentials:
  sas_token: "<sas-query-string>"
```

### Custom storage_options

For advanced use cases, pass `storage_options` to bypass the built-in credential resolution and supply options directly to the underlying object store library:

```yaml
credentials:
  storage_options:
    account_name: mystorageaccount
    bearer_token: "<token>"
```

## Directory layout

```
<container>/
  <prefix>/                       # if prefix is set
    <schema>/
      <table_name>/               # Delta Lake table
      <table_name>.parquet        # Parquet table
```
