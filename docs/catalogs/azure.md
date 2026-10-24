# Azure Blob Storage

Store tables as files on Azure Blob Storage or Azure Data Lake Storage Gen2 (ADLS Gen2).

## Supported features

| Feature | Status | Remarks |
|---|---|---|
| Read/write Delta tables | ✅ | Default `file_format` |
| Read/write Parquet, CSV and NDJSON files | ✅ | Set `file_format` on the model |

## Installation

```bash
pip install 'dbt-polars[azure]'
```

## Profile configuration

### Example

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog_type: azure
      schema: dev
      account_name: mystorageaccount
      container: mycontainer
      prefix: optional/path/prefix
```

To combine this catalog with others, put the same parameters in an entry of
`catalogs`, with `type: azure` and a `name`, see
[Catalog overview](../catalog-overview.md).

### Parameter Reference

The following parameters are supported, in addition to the
[common profile parameters](../catalog-overview.md#common-profile-parameters):

* `catalog_type` - (Required) Must be `azure`.

* `schema` - (Required) Default schema for models. dbt-polars creates missing
  schemas.

* `account_name` - (Required) Azure Storage account name.

* `container` - (Required) Blob container / file system name.

* `prefix` - (Optional) Path prefix prepended to all schema and table paths.

* `credentials` - (Optional) Authentication options. By default, dbt-polars
  authenticates with
  [`DefaultAzureCredential`](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential),
  which gets the other keys in `credentials` as keyword arguments. To
  authenticate otherwise, set one of:
  * `bearer_token` - OAuth access token.
  * `account_key` - Storage account key.
  * `sas_token` - SAS token.
  * `storage_options` - Options passed directly to the object store, see
    [object_store's Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html).

## Directory layout

```
<container>/
  <prefix>/                       # if prefix is set
    <schema>/
      <table_name>/               # Delta Lake table
      <table_name>.parquet        # Parquet table
```
