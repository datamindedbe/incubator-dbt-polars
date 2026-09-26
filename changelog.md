# Changelog

## Unreleased

### Implements

- Custom catalogs as separate packages, see [docs](docs/catalogs/custom.md).
- Reusable catalog tests for catalog implementations (`dbt.adapters.polars.testing`).
- The local, S3, Azure and Databricks catalogs are published as separate packages (`dbt-polars-catalog-<type>`). `dbt-polars` installs the local catalog; the extras (`dbt-polars[azure]`, ...) install the others.
- Databricks catalog: `table_format` option (only `delta` for now).

### Breaking

- Installing the Azure, S3 or Databricks dependencies without the matching extra no longer enables those catalogs; install `dbt-polars[<type>]` instead.

## v0.2.0

### Implements

- Databricks catalog on top of the Unity Catalog Rest API.

## v0.1.1

### Documentation

- Add getting started guide

### Fixes

- dbt init prompts for setting up a profile

## v0.1.0

### Implements

- Support common dbt SQL macros

### Fixes

- Package .sql and .yaml files

## v0.0.2

### Breaking

- schema parameter required on catalog, at the root level or per catalog.
- New profile shape for single- vs multi-catalog setups, see [docs](docs/index.md#profile-structure).

###  fixed

- Error when schema was unspecified on both the model and profile level.
- Iceberg Catalog: Error when using relative, local paths for warehouse or uri.
