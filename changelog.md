# Changelog

## Unreleased

### Implements

- Documentation rendered in docusaurus

### Fixes

- Omit sensitive fields in the profile from `dbt debug`
- [Databricks Catalog]: Request READ instead of READ_WRITE credentials for read-only access
- [Databricks Catalog]: Update the description of all columns in a table with a single query

## v0.3.0

### Implements

- Catalogs are now published as separate packages
- Move tests into a package module (`dbt.adapters.polars.testing`) for reusability in catalogs maintained outside this repo
- [Databricks Catalog]: Add field  `table_format` in the profile. Only allowed value is `delta`.

### Other improvements

- Stop using `pull_request_target` in CI

### Documentation

- [Readme] Add list of key dbt-polars features

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
- New profile shape for single- vs multi-catalog setups, see [docs](docs/catalog-overview.md#profile-structure).

###  fixed

- Error when schema was unspecified on both the model and profile level.
- Iceberg Catalog: Error when using relative, local paths for warehouse or uri.
