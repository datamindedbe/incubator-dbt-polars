# Snowflake catalog

Write Iceberg tables to Snowflake through Snowflake Horizon Catalog.

## Supported features

| Feature | Status | Remarks |
|---|---|---|
| Read/write Snowflake-managed Iceberg tables | ✅ | Through Horizon's Iceberg REST catalog, without a Snowflake warehouse |
| Table descriptions in Snowflake | ✅ |  |
| Column descriptions in Snowflake | ✅ | Comments set in Snowflake take precedence |
| Read native Snowflake tables | ❌️ | Only Iceberg tables are available through Horizon, see [Prerequisites](#prerequisites) |
| Read/write views | ❌️ |  |

## Prerequisites

* A Snowflake database for Snowflake-managed Iceberg tables: with an external
  volume, or with Snowflake-managed storage.
* To also read tables created in Snowflake with a plain `CREATE TABLE`, set
  `DEFAULT_METADATA_WRITE_FORMAT = 'ICEBERG'` on the database or schema. This
  needs `CATALOG = 'SNOWFLAKE'` on the database. Existing native tables must
  be converted, e.g. with `CREATE ICEBERG TABLE ... AS SELECT`.

## Installation

```bash
pip install 'dbt-polars[snowflake]'
```

## Profile configuration

### Example

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalog_type: snowflake
      schema: dev
      account: myorg-myaccount
      database_name: analytics
      role: transformer
      credential: "{{ env_var('SNOWFLAKE_PAT') }}"
```

To combine this catalog with others, put the same parameters in an entry of
`catalogs`, with `type: snowflake` and a `name`, see
[Catalog overview](../catalog-overview.md).

Snowflake takes a few seconds per commit. Set for example `threads: 4` and
`compute_threads: 1` so other models compute while a model waits for its commit, see
[Running models in parallel](../catalog-overview.md#running-models-in-parallel).

### Parameter Reference

The following parameters are supported:

* `catalog_type` - (Required) Must be `snowflake`.

* `account` - (Required) Account identifier in the form
  `<organization>-<account>`, as returned by
  `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`.

* `database_name` - (Required) Snowflake database holding the Iceberg tables.

* `schema` - (Required) Default schema for models. dbt-polars creates missing
  schemas.

* `role` - (Optional) Role dbt-polars acts as.

* `uppercase_column_names` - (Optional) Store column names in UPPERCASE, as
  Snowflake does for unquoted names, so they can be queried without quotes.
  dbt-polars reads them back in lowercase. Defaults to `true`. Changing it for
  an existing table requires `--full-refresh`.

:::info
Schema and table names are always stored in UPPERCASE, like dbt-snowflake does
by default.
:::

* Authentication parameters - (Optional) Passed to pyiceberg's
  [REST catalog](https://py.iceberg.apache.org/configuration/#rest-catalog),
  which exchanges them for an access token at Snowflake. Commonly used:
  * `credential` - Programmatic access token (PAT) of the user.
  * `token` - OAuth access token, e.g. from your identity provider.
  * `scope` - Overrides the `session:role:<ROLE>` scope derived from `role`.

## Required permissions

The role dbt-polars uses needs these privileges:

| Privilege | On | Needed for |
|---|---|---|
| `USAGE` | the database | Accessing the database at all |
| `CREATE SCHEMA` | the database | Creating schemas |
| `USAGE` | the external volume | Creating and writing tables |
| `USAGE`, `CREATE ICEBERG TABLE` | schemas dbt-polars didn't create | Creating tables in them |
| `SELECT` | tables dbt-polars doesn't own | Reading them, e.g. as a source |
| `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` | tables dbt-polars doesn't own | Writing to them |
