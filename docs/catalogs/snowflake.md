---
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Snowflake Horizon Catalog

Write Iceberg tables to Snowflake through Snowflake Horizon Catalog.

## Supported features

| Feature | Status | Remarks |
|---|---|---|
| Read/write Snowflake-managed Iceberg tables | ✅ | Through Horizon's Iceberg REST catalog, without a Snowflake warehouse |
| Table descriptions in Snowflake | ✅ |  |
| Column descriptions in Snowflake | ✅ | Comments set in Snowflake take precedence |
| Read native Snowflake tables | ❌️ | Only Iceberg tables are available through Horizon, see [Make Iceberg the default table format](#make-iceberg-the-default-table-format) |
| Read/write views | ❌️ |  |

## Prerequisites

* A Snowflake database for Snowflake-managed Iceberg tables: with an external
  volume, or with Snowflake-managed storage.

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

### Parameter Reference

The following parameters are supported, in addition to the
[common profile parameters](../catalog-overview.md#common-profile-parameters):

* `catalog_type` - (Required) Must be `snowflake`.

* `account` - (Required) Account identifier in the form
  `<organization>-<account>`, as returned by
  `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`.

* `database_name` - (Required) Snowflake database holding the Iceberg tables.

* `schema` - (Required) Default schema for models. dbt-polars creates missing
  schemas.

* `role` - (Optional) Role dbt-polars acts as.

* Authentication parameters - (Optional) Passed to pyiceberg's
  [REST catalog](https://py.iceberg.apache.org/configuration/#rest-catalog),
  which exchanges them for an access token at Snowflake. Commonly used:
  * `credential` - Programmatic access token (PAT) of the user.
  * `token` - OAuth access token, e.g. from your identity provider.
  * `scope` - Overrides the `session:role:<ROLE>` scope derived from `role`.

## Recommendations

### Run models in parallel

For small models, most of the run time is spent waiting for Snowflake's REST
API. We recommend `threads: 4` with `compute_threads: 1`, so these calls run in
parallel while one model at a time runs Polars. See
[Running models in parallel](../catalog-overview.md#running-models-in-parallel).

### Make Iceberg the default table format

dbt-polars can only read Iceberg tables. We recommend making Iceberg the
default table format in Snowflake, so a plain `CREATE TABLE` also creates an
Iceberg table:

<Tabs groupId="snowflake-level">
<TabItem value="database" label="Database" default>

```sql
ALTER DATABASE <database_name> SET DEFAULT_METADATA_WRITE_FORMAT = 'ICEBERG';
```

</TabItem>
<TabItem value="schema" label="Schema">

```sql
ALTER SCHEMA <database_name>.<schema> SET DEFAULT_METADATA_WRITE_FORMAT = 'ICEBERG';
```

</TabItem>
<TabItem value="account" label="Account">

Applies to all databases in the account. Requires the `ACCOUNTADMIN` role.

```sql
ALTER ACCOUNT SET DEFAULT_METADATA_WRITE_FORMAT = 'ICEBERG';
```

</TabItem>
</Tabs>

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
