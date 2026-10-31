---
sidebar_label: External Delta tables
pagination_prev: guides/catalogs/databricks
pagination_next: guides/source-data
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# External tables on Databricks

In this guide, we set up dbt-polars to write Delta tables to Databricks Unity Catalog. Polars runs on your own machine, so no Databricks compute is needed. If you don't have a project yet, the [quick start](../../quickstart.md) creates one.

dbt-polars can't yet write managed Delta tables to Unity Catalog. It therefore creates Delta tables as external tables. These live in your own cloud storage, and Unity Catalog only registers them. This needs more setup than managed tables, because Unity Catalog needs access to your storage.

| | Managed tables | External tables |
|---|---|---|
| Supported table format | Iceberg | Delta |
| Storage location | Chosen by Unity Catalog | Your own bucket |
| Dropped in Databricks | Data is deleted | Data remains |
| Setup | Catalog | Catalog, external location and storage credential |

:::info
Unity Catalog recently added catalog commits, which let external engines write managed Delta tables. The feature is still in preview, see the Databricks documentation for [AWS](https://docs.databricks.com/aws/en/tables/features/catalog-commits) or [Azure](https://learn.microsoft.com/en-us/azure/databricks/tables/features/catalog-commits). The deltalake library that Polars uses doesn't support catalog commits yet. Once it does, dbt-polars may also write Delta tables as managed tables, without the setup in this guide.
:::

## Prerequisites

- A Databricks workspace with Unity Catalog.
- An S3 bucket or an ADLS Gen2 storage account.
- Metastore admin rights, to enable external data access and create a storage credential and an external location.
- On AWS, the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install). On Azure, the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

## Enabling external data access

:::info
This is a one-time setup per metastore. Skip this step if external data access is already enabled.
:::

External data access must be enabled on the metastore so that external engines like dbt-polars can write directly to storage.

As a metastore admin, enable it in Catalog Explorer:

1. In your workspace, click Catalog in the sidebar.
2. Click the gear icon at the top of the Catalog pane and select Metastore.
3. On the Details tab, turn on External data access.

See Enable external data access in the Databricks documentation for [AWS](https://docs.databricks.com/aws/en/external-access/admin) or [Azure](https://learn.microsoft.com/en-us/azure/databricks/external-access/admin) for more details.

## Creating an external location

Unity Catalog accesses your cloud storage through an external location, which pairs a storage path with a storage credential.

First, create a storage credential named `my_credential`. The setup depends on your cloud:

<Tabs groupId="cloud">
<TabItem value="aws" label="AWS" default>

1. In the AWS console, create an IAM role with read and write access to your S3 bucket.
2. In Catalog Explorer, go to External data > Credentials > Create credential. Choose AWS IAM Role and enter the role ARN.
3. Add the trust policy that Databricks shows, including the external ID, to the IAM role.

See [Create a storage credential for connecting to AWS S3](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual) for the full setup.

</TabItem>
<TabItem value="azure" label="Azure">

1. In the Azure portal, create an Access Connector for Azure Databricks.
2. Assign the connector's managed identity the Storage Blob Data Contributor role on your storage account.
3. In Catalog Explorer, go to External data > Credentials > Create credential. Choose Azure Managed Identity and enter the connector's resource ID.

See [Use Azure managed identities in Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities) for the full setup.

</TabItem>
</Tabs>

Then create the external location in the SQL editor:

<Tabs groupId="cloud">
<TabItem value="aws" label="AWS" default>

```sql
CREATE EXTERNAL LOCATION my_location
URL 's3://my-bucket/dbt_polars'
WITH (STORAGE CREDENTIAL my_credential);
```

</TabItem>
<TabItem value="azure" label="Azure">

```sql
CREATE EXTERNAL LOCATION my_location
URL 'abfss://my-container@myaccount.dfs.core.windows.net/dbt_polars'
WITH (STORAGE CREDENTIAL my_credential);
```

</TabItem>
</Tabs>

## Creating a catalog

dbt-polars writes external tables to the managed location of the catalog. Create a catalog with its managed location inside the external location, and grant access to both:

<Tabs groupId="cloud">
<TabItem value="aws" label="AWS" default>

```sql
CREATE CATALOG dbt_polars
MANAGED LOCATION 's3://my-bucket/dbt_polars';

GRANT USE CATALOG, CREATE SCHEMA, EXTERNAL USE SCHEMA
ON CATALOG dbt_polars TO `<principal>`;

GRANT EXTERNAL USE LOCATION, CREATE EXTERNAL TABLE
ON EXTERNAL LOCATION my_location TO `<principal>`;
```

</TabItem>
<TabItem value="azure" label="Azure">

```sql
CREATE CATALOG dbt_polars
MANAGED LOCATION 'abfss://my-container@myaccount.dfs.core.windows.net/dbt_polars';

GRANT USE CATALOG, CREATE SCHEMA, EXTERNAL USE SCHEMA
ON CATALOG dbt_polars TO `<principal>`;

GRANT EXTERNAL USE LOCATION, CREATE EXTERNAL TABLE
ON EXTERNAL LOCATION my_location TO `<principal>`;
```

</TabItem>
</Tabs>

Replace `<principal>` with your user's email address or the service principal's application ID. `EXTERNAL USE SCHEMA` and `EXTERNAL USE LOCATION` allow dbt-polars to read and write table data outside of Databricks compute. They are not included in `ALL PRIVILEGES` or in ownership, so they are needed even when you own the catalog.

## Configuring the profile

Install the Databricks catalog:

```bash
pip install 'dbt-polars[databricks]'
```

dbt reads its connections from `profiles.yml`, by default in the `~/.dbt` folder. Add a profile for your project, named after the profile setting in dbt_project.yml:

<Tabs groupId="cloud">
<TabItem value="aws" label="AWS" default>

```yaml
my_project:
  target: databricks
  outputs:
    databricks:
      type: polars
      schema: analytics
      catalog_type: databricks
      catalog_name: dbt_polars
      host: https://my-workspace.cloud.databricks.com
      auth_type: databricks-cli
```

Replace the host with the URL of your Databricks workspace. Then log in to your workspace with the Databricks CLI:

```bash
databricks auth login --host https://my-workspace.cloud.databricks.com
```

</TabItem>
<TabItem value="azure" label="Azure">

```yaml
my_project:
  target: databricks
  outputs:
    databricks:
      type: polars
      schema: analytics
      catalog_type: databricks
      catalog_name: dbt_polars
      host: https://adb-1234567890123456.7.azuredatabricks.net
      auth_type: azure-cli
```

Replace the host with the URL of your Databricks workspace. Then log in to Azure with the Azure CLI:

```bash
az login
```

On Azure, also install `pip install 'dbt-polars-catalog-databricks[azure]'`. dbt-polars uses it to clean up the storage of tables it drops.

</TabItem>
</Tabs>

Delta is the default table format, so the profile doesn't need to set it.

## Running the project

Run the project:

```bash
dbt run
```

dbt-polars creates the analytics schema in the dbt_polars catalog and writes your models to it as external Delta tables, stored in the catalog's managed location. Open the Catalog Explorer in Databricks to find them.

This catalog also supports managed Iceberg tables, so you can combine both kinds in one project by setting `table_format` per model. See [Model configuration](../../catalogs/databricks.md#model-configuration).

## Read more

- [Connecting to Databricks](databricks.md): write managed Iceberg tables, and add descriptions to Unity Catalog.
- [Databricks catalog](../../catalogs/databricks.md): all profile parameters, including service principal authentication.
