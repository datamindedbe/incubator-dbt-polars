---
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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

<Tabs groupId="profile-shape">
<TabItem value="single" label="Single catalog" default>

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

</TabItem>
<TabItem value="multiple" label="Multiple catalogs">

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog: my_local
      catalogs:
        - name: my_local
          type: local
          schema: dev
          root: ./data
        - name: other_catalog
          ...
```

See [Combining catalogs](../guides/combining-catalogs.md).

</TabItem>
</Tabs>

### Parameter Reference

The following parameters are supported:

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
