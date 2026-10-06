# dbt-polars

dbt-polars integrates the flexibility and performance of Polars with the data governance and lineage capabilities of dbt.

Key features of **dbt-polars**:

* **Run anywhere**: Polars runs on any compute platform, so you’re not locked into an expensive data warehouse.
* **Flexible**: Available catalogs let you connect to local storage, Iceberg catalogs, Databricks, and more.
* **Extensible**: Create a custom catalog to fit your unique storage and warehouse needs.
* **First-class Python models**: Run Python models with the same performance and capabilities as SQL models.
* **Mix Python and SQL**: Choose the language that best fits the transformation.
* **dbt-native**: Bring lineage, documentation, and data testing into your workflow.
* **Ingestion**: Build efficient, incremental API ingestion with Python models and keep the entire pipeline visible in the lineage graph.

## Installation

```bash
pip install dbt-polars
```

Extra dependencies are required for cloud and Iceberg backends:

```bash
pip install 'dbt-polars[azure]'       # Azure Blob Storage
pip install 'dbt-polars[databricks]'  # Azure Blob Storage
pip install 'dbt-polars[iceberg]'     # Apache Iceberg (SQLite, REST, Databricks, …)
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
| `databricks` | Unity Catalog REST API + credential vending | Experimental |

## Quick start

See the [Getting started](https://datamindedbe.github.io/incubator-dbt-polars/demos/getting_started) guide.

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

- [Getting started](https://datamindedbe.github.io/incubator-dbt-polars/demos/getting_started)
- [Catalog overview and profile reference](https://datamindedbe.github.io/incubator-dbt-polars/catalog-overview)
- [Local catalog](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/local)
- [Azure catalog](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/azure)
- [Iceberg catalog](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/iceberg)
- [Databricks catalog](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/databricks)
- [Custom catalogs](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/custom)
- [Python models and tests](https://datamindedbe.github.io/incubator-dbt-polars/python-models)
