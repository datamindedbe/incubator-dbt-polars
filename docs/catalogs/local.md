# Local storage

Store tables as files on the local filesystem.

## Supported features

| Feature | Status | Remarks |
|---|---|---|
| Read/write Delta tables | ✅ | Default `file_format` |
| Read/write Parquet, CSV and NDJSON files | ✅ | Set `file_format` on the model |

## Installation

No extra dependencies needed beyond the base package:

```bash
pip install dbt-polars
```

## Profile configuration

### Example

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog_type: local
      schema: dev
      root: ./data
```

To combine this catalog with others, put the same parameters in an entry of
`catalogs`, with `type: local` and a `name`, see
[Catalog overview](../catalog-overview.md).

### Parameter Reference

The following parameters are supported, in addition to the
[common profile parameters](../catalog-overview.md#common-profile-parameters):

* `catalog_type` - (Required) Must be `local`.

* `schema` - (Required) Default schema for models. dbt-polars creates missing
  schemas.

* `root` - (Required) Directory where schemas and tables are stored. Relative
  paths resolve from the dbt project root. Paths with spaces are not supported
  (Polars limitation).

## Directory layout

The local catalog organises data as:

```
<root>/
  <schema>/
    <table_name>/          # Delta Lake table (directory)
    <table_name>.parquet   # Parquet table
    <table_name>.csv       # CSV table
    <table_name>.ndjson    # NDJSON table
```

Schemas are directories. Creating a schema creates the directory; dropping it removes the directory and all its contents.
