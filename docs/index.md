# dbt-polars documentation

dbt-polars is a dbt adapter that runs transformations locally with Polars. It stores data as files rather than connecting to a database, making it well-suited for local development, offline pipelines, and data lake workflows.

## How catalogs work

Every dbt-polars profile defines one or more **catalogs**. A catalog is a named storage backend — a local folder, a blob storage container, or an Iceberg catalog. Each catalog exposes the same interface to dbt: schemas, tables, and the standard materializations (table, incremental, snapshot).

In your profile, the `database` key (aliased as `catalog`) selects the default catalog. Models can target a non-default catalog with `{{ config(database="other_catalog") }}`.

### Profile structure

Most profiles only need one catalog. For that case, set the catalog's own options
directly at the top level — no `catalogs` list required. The backend type goes in
`catalog_type` rather than `type`, since `type: polars` already selects the adapter itself:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      schema: my_schema      # required
      catalog_type: local    # local | azure | s3 | iceberg | databricks
      root: ./data           # local's own option(s)
```

`PolarsCredentials` restructures this into a one-entry `catalogs` list internally, named
after `database`/`catalog` if you set one (`"default"` otherwise).

To configure more than one catalog, set `catalogs` explicitly instead. Each entry needs
its own `schema` — the profile's top-level `schema` must be absent, since there's no
single schema that could apply to every catalog:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog: my_catalog     # which catalog is the default (optional — first catalog used if omitted)
      catalogs:
        - name: my_catalog
          type: local
          root: ./data
          schema: my_schema
        - name: archive
          type: local
          root: ./archive
          schema: archive_schema
```

`target.schema` always reflects the *default* catalog's schema, so it works the same way in
both shapes above. For a non-default catalog's schema (e.g. in a source's `schema:`
override), look it up from `target.catalogs` instead:

```yaml
schema: "{{ (target.catalogs | selectattr('name', 'equalto', 'archive') | first)['schema'] }}"
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
- [Databricks](catalogs/databricks.md) — Unity Catalog tables via REST, no Databricks compute

## Other topics

- [Getting started](demos/getting_started.md)
- [Python models and tests](python-models.md)
- [SQL macro support](sql-macros.md) — which of dbt's generic SQL macros work on dbt-polars, and why the rest don't
