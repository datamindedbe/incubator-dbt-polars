# Catalog overview

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
      catalog_type: local    # local | azure | s3 | iceberg | databricks | fabric | snowflake
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

### Common profile parameters

These parameters apply to every catalog type. Each catalog page lists the
parameters of that catalog on top of these.

* `type` - (Required) Must be `polars`.

* `catalog` - (Optional) Name of the default catalog. Defaults to
  the first entry of `catalogs`, or `default` when `catalogs` isn't set.

* `catalogs` - (Optional) List of catalogs, each with a `name`, a `type`, a
  `schema` and the parameters of its catalog type.

* `threads` - (Optional) Number of models dbt runs at the same time. Defaults
  to 1.

* `compute_threads` - (Optional) Number of those models that run Polars at the
  same time, see [Running models in parallel](#running-models-in-parallel).
  Unlimited by default.

### Running models in parallel

Polars already uses all CPU cores for a single model, so `threads: 1` is usually
fastest. With a remote catalog, such as Snowflake, much of a model's time is spent
waiting for the catalog to commit. To use that time, run more models with `threads`
and limit how many of them run Polars at the same time with `compute_threads`:

```yaml
    dev:
      type: polars
      threads: 4           # models dbt runs at the same time
      compute_threads: 1   # of which run Polars at the same time
      catalog_type: snowflake
      ...
```

A model that waits for an Iceberg commit lets another model compute in the meantime.
Models that hold a collected DataFrame, such as merge, delete+insert and snapshots,
or Python models returning a DataFrame, keep running Polars until they finish, so
memory stays bounded by `compute_threads` models. Without `compute_threads`, all
`threads` models can run Polars at the same time.

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

Iceberg catalogs always use the Iceberg format, so a model that sets `file_format` fails there.

### Reading files as sources

A source in a storage catalog (local, azure, s3) is read from `<schema>/<identifier>`, plus `.<file_format>` for a file format. To read files with another name, such as an upper-case extension or a glob, give the source an `external.location`. It is a path relative to the catalog root and includes the schema.

```yaml
sources:
  # Tables in the format dbt-polars writes
  - name: crm
    database: landing
    schema: crm
    config:
      file_format: csv
    tables:
      - name: customers
      - name: contacts
        identifier: contacts_export

  # Tables read from a path relative to the catalog root
  - name: erp
    database: landing
    schema: erp
    tables:
      - name: orders
        external:
          location: erp/orders/*.CSV
        config:
          file_format: csv
      - name: invoices
        external:
          location: erp/invoices/**/*.parquet
        config:
          file_format: parquet
          read_options: {hive_partitioning: true}
```

- `external.location` must be relative. An absolute path or a URL is an error, because credentials belong to the catalog. To read from another storage location, add a catalog for it and set the source's `database` to that catalog.
- `location` is the only `external` key dbt-polars reads. Any other key, such as `external.file_format`, is an error: set `file_format` and `read_options` under `config:`.
- `read_options` are passed to the Polars `scan_*` function of the file format. In `schema`, `schema_overrides` and `hive_schema`, give types by name: Polars names (`String`, `Int64`) or SQL names (`varchar`, `bigint`).

## Catalog reference

- [Local](catalogs/local.md) — reads and writes files on the local filesystem
- [Azure](catalogs/azure.md) — reads and writes files on Azure Blob Storage / ADLS Gen2
- [Iceberg](catalogs/iceberg.md) — Apache Iceberg tables via pyiceberg
- [Databricks](catalogs/databricks.md) — Unity Catalog tables via REST, compute-free (an optional SQL warehouse can persist docs into Unity Catalog's native comments)
- [Fabric](catalogs/fabric.md) — Delta and Iceberg tables in a Microsoft Fabric Lakehouse via OneLake, compute-free
- [Snowflake](catalogs/snowflake.md) — Snowflake-managed Iceberg tables via Horizon Catalog, compute-free
- [Custom catalogs](catalogs/custom.md) — add your own storage backend as a separate package
