# dbt-polars

A [dbt](https://www.getdbt.com/) adapter that runs models locally using [Polars](https://pola.rs/). Instead of connecting to a database, dbt-polars reads and writes files — on your local filesystem, Azure Blob Storage, AWS S3, or an Iceberg catalog.

## Installation

```bash
pip install dbt-polars
```

Extra dependencies are required for cloud and Iceberg backends:

```bash
pip install 'dbt-polars[azure]'    # Azure Blob Storage
pip install 'dbt-polars[s3]'       # AWS S3
pip install 'dbt-polars[iceberg]'  # Apache Iceberg (SQLite, REST, Databricks, …)
```

## Catalogs

dbt-polars uses **catalogs** to define where data is stored. Each catalog maps to a storage backend. You can configure multiple catalogs in a single profile and reference them from your models.

| Catalog | Backend | Status |
|---|---|---|
| `local` | Local filesystem | Usable |
| `iceberg` with SQLite | Iceberg + SQLite metastore | Usable |
| `iceberg` with REST + Databricks | Iceberg REST API (Unity Catalog) | Experimental |
| `iceberg` with other backends | Any pyiceberg-supported backend | Experimental |
| `azure` | Azure Blob Storage / ADLS Gen2 | Experimental |

## Quick start

See the [Getting started](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/demos/getting_started.md) guide.

A minimal `profiles.yml` for local development is:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      schema: dev

      catalog_type: local
      root: ./data
```

## Documentation

- [Getting started](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/demos/getting_started.md)
- [Catalog overview and profile reference](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/index.md)
- [Local catalog](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/catalogs/local.md)
- [Azure catalog](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/catalogs/azure.md)
- [Iceberg catalog](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/catalogs/iceberg.md)
- [Python models and tests](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/python-models.md)
- [Roadmap and known limitations](https://github.com/datamindedbe/incubator-dbt-polars/blob/main/docs/roadmap.md)
