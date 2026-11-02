---
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Iceberg (generic)

Store tables in Apache Iceberg format using [pyiceberg](https://py.iceberg.apache.org/).

:::info
For Iceberg tables in Databricks, Snowflake or Microsoft Fabric, use the
dedicated [Databricks](databricks.md), [Snowflake](snowflake.md) or
[Fabric](fabric.md) catalog instead.
:::

## Supported features

| Feature | Status | Remarks |
|---|---|---|
| Read/write Iceberg tables | ✅ | Unsigned integer columns are written as signed, since Iceberg has no unsigned types |
| All catalogs supported by pyiceberg | ✅ | E.g. REST, SQL, Hive and Glue, see [pyiceberg's catalogs](https://py.iceberg.apache.org/configuration/) |

## Installation

```bash
pip install 'dbt-polars[iceberg]'
```

## Profile configuration

### Example

A fully local Iceberg setup with a SQLite catalog and a local warehouse:

<Tabs groupId="profile-shape">
<TabItem value="single" label="Single catalog" default>

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog_type: iceberg
      schema: dev
      uri: sqlite:///catalog.db         # SQLite database for the Iceberg metastore
      warehouse: file://warehouse       # directory where table data is written
```

</TabItem>
<TabItem value="multiple" label="Multiple catalogs">

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog: my_iceberg
      catalogs:
        - name: my_iceberg
          type: iceberg
          schema: dev
          uri: sqlite:///catalog.db     # SQLite database for the Iceberg metastore
          warehouse: file://warehouse   # directory where table data is written
        - name: other_catalog
          ...
```

See [Combining catalogs](../guides/combining-catalogs.md).

</TabItem>
</Tabs>

A REST catalog, such as Databricks Unity Catalog:

```yaml
    dev:
      type: polars
      catalog_type: iceberg
      schema: dev
      pyiceberg_type: rest
      uri: https://<workspace>.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest/
      token: <databricks-pat-or-token>
      warehouse: <unity-catalog-name>    # the Unity Catalog catalog name (e.g. "main")
```

### Parameter Reference

The following parameters are supported:

* `catalog_type` - (Required) Must be `iceberg`.

* `schema` - (Required) Default schema for models. dbt-polars creates missing
  schemas.

* `pyiceberg_type` - (Optional) Sets the pyiceberg `type` property, e.g. `rest`,
  `hive` or `glue`. It is a separate key because `type` is already used by
  dbt-polars.

* All other parameters - (Optional) Passed to `pyiceberg.catalog.load_catalog`,
  see the [pyiceberg docs](https://py.iceberg.apache.org/configuration/) for
  the options of each backend.
