# Connecting to a catalog

Polars has no default storage location, so every dbt-polars project starts by defining one or more storage connections. These connections are called **catalogs**.

The table lists the available catalogs. Check the guide and reference of a catalog for detailed information on how to use it.

| Catalog | Storage location | Install | Learn more |
|---|---|---|---|
| ![](../images/icons/folder.svg) Local | Local disk | dbt-polars | [Reference](../catalogs/local.md) |
| ![](../images/logos/azure-storage-accounts.svg) Azure | Azure Blob Storage or ADLS Gen2 | dbt-polars[azure] | [Guide](catalogs/azure.md), [Reference](../catalogs/azure.md) |
| ![](../images/logos/databricks.svg) Databricks | Databricks Unity Catalog | dbt-polars[databricks] | [Guide](catalogs/databricks.md), [Reference](../catalogs/databricks.md) |
| ![](../images/logos/fabric.svg) Fabric | Microsoft Fabric OneLake | dbt-polars[fabric] | [Guide](catalogs/fabric.md), [Reference](../catalogs/fabric.md) |
| ![](../images/logos/snowflake.svg) Snowflake | Snowflake Horizon Catalog | dbt-polars[snowflake] | [Guide](catalogs/snowflake.md), [Reference](../catalogs/snowflake.md) |
| Iceberg | Any pyiceberg catalog, such as REST or Glue | dbt-polars[iceberg] | [Guide](catalogs/iceberg.md), [Reference](../catalogs/iceberg.md) |
| ![](../images/icons/plus.svg) Custom | Your own storage backend | your own package | [Custom catalogs](custom-catalog.md) |

## Read more

- [Combining catalogs](combining-catalogs.md): use several catalogs in one project.
- [Custom catalogs](custom-catalog.md): connect dbt-polars to a storage backend that has no catalog yet.
