# Changelog

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
