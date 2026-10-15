# Changelog

## Unreleased

## v0.4.0

### Implements

- A source can be read from a path relative to its catalog with `external.location`: a file with any extension (`data.CSV`) or a glob (`orders/*.CSV`) ([#29](https://github.com/datamindedbe/incubator-dbt-polars/issues/29))

### Fixes

- Fail with error on SQL statements executed through hooks ([#36](https://github.com/datamindedbe/incubator-dbt-polars/issues/36))
- Resolve polar types in read options (`schema`, `override_schema`) ([#33](https://github.com/datamindedbe/incubator-dbt-polars/issues/33))
- Fail with error on unknown `read_options` keys ([#34](https://github.com/datamindedbe/incubator-dbt-polars/issues/34))
- Lazy append on incremental materialization ([#31](https://github.com/datamindedbe/incubator-dbt-polars/issues/31))
- Full refresh of an incremental model are now atomic
- Fail with error on unknown model config keys

## v0.3.1

### Implements

- `current_timestamp` macro evaluates to naive timestamp when dbt run started.
- `dateadd`, `datediff`, `date_spine` and `get_intervals_between` macro.
- `date_trunc` macro supports `week` and `quarter`.
- `last_day` macro for `week`, `month`, `quarter` and `year`.
- Move documentation to GitHub pages

### Fixes

- Omit sensitive fields in the profile from `dbt debug`
- [Databricks Catalog]: Request READ instead of READ_WRITE credentials for read-only access
- [Databricks Catalog]: Update the description of all columns in a table with a single query
- Queried fail when a CTE has the same name as a model it selects from
- User .str.date to cast strings to date as this is the only supported way to do this conversion in Polars 2.0
- Throw an error when a user uses a hook or contract which is not yet supported

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
