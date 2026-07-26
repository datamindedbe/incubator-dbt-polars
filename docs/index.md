# dbt-polars documentation

dbt-polars is a dbt adapter that runs transformations locally with Polars. It stores data as files rather than connecting to a database, making it well-suited for local development, offline pipelines, and data lake workflows.

## How catalogs work

Every dbt-polars profile defines one or more **catalogs**. A catalog is a named storage backend — a local folder, a blob storage container, or an Iceberg catalog. Each catalog exposes the same interface to dbt: schemas, tables, and the standard materializations (table, incremental, snapshot).

In your profile, the `catalog` key selects the default catalog. Models can target a non-default catalog with `{{ config(database="other_catalog") }}`.

### Profile structure

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      schema: my_schema          # default schema
      catalog: my_catalog        # which catalog is the default (optional — first catalog used if omitted)
      catalogs:
        - name: my_catalog
          type: local
          root: ./data
        - name: archive
          type: local
          root: ./archive
```

### Supported file formats

All storage-backed catalogs (local, azure) write files on disk. The `file_format` config on a model controls which format is used:

| Format | Config value | Notes |
|---|---|---|
| Delta Lake | `delta` | Default. Supports incremental, partitioning, snapshots. |
| Parquet | `parquet` | Single-file, read-only friendly. |
| CSV | `csv` | Human-readable, no schema enforcement. |
| NDJSON | `ndjson` | Line-delimited JSON. |

Set the format in `dbt_project.yml` or per model:

```yaml
models:
  my_project:
    +file_format: delta
```

Iceberg catalogs always use the Iceberg format — the `file_format` config has no effect there.

## Catalog reference

- [Local](catalogs/local.md) — reads and writes files on the local filesystem
- [Azure](catalogs/azure.md) — reads and writes files on Azure Blob Storage / ADLS Gen2
- [Iceberg](catalogs/iceberg.md) — Apache Iceberg tables via pyiceberg

## Other topics

- [Python models and tests](python-models.md)
