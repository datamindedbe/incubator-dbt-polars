---
sidebar_label: Azure
pagination_prev: guides/connecting-to-a-catalog
pagination_next: guides/source-data
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting to Azure

In this guide, we connect a dbt-polars project to Azure Data Lake Storage Gen2. We will create a storage account and a container, and write the models of the project to it as Delta tables. If you don't have a project yet, the [quick start](../../quickstart.md) creates one.

## Prerequisites

- An Azure subscription, with permission to create a storage account and assign roles on it.
- The [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).

## Creating a storage account

Log in to Azure with the Azure CLI:

```bash
az login
```

Then create a resource group, a storage account with hierarchical namespace enabled, and a container. Replace `<mystorageaccount>` with a name of your own:

<Tabs groupId="provisioning">
<TabItem value="cli" label="Azure CLI" default>

```bash
az group create --name dbt-polars --location westeurope

az storage account create --name <mystorageaccount> --resource-group dbt-polars \
  --sku Standard_LRS --hns true

az storage container create --name dbt-polars --account-name <mystorageaccount> \
  --auth-mode login
```

</TabItem>
<TabItem value="terraform" label="Terraform">

```hcl
provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "dbt_polars" {
  name     = "dbt-polars"
  location = "westeurope"
}

resource "azurerm_storage_account" "dbt_polars" {
  name                     = "<mystorageaccount>"
  resource_group_name      = azurerm_resource_group.dbt_polars.name
  location                 = azurerm_resource_group.dbt_polars.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_container" "dbt_polars" {
  name               = "dbt-polars"
  storage_account_id = azurerm_storage_account.dbt_polars.id
}
```

</TabItem>
</Tabs>

## Granting access

dbt-polars reads and writes the data with your Microsoft Entra ID login. Assign yourself the Storage Blob Data Contributor role on the storage account to be able to read/write data:

<Tabs groupId="provisioning">
<TabItem value="cli" label="Azure CLI" default>

```bash
az role assignment create --role "Storage Blob Data Contributor" \
  --assignee "$(az ad signed-in-user show --query id --output tsv)" \
  --scope "$(az storage account show --name <mystorageaccount> --query id --output tsv)"
```

</TabItem>
<TabItem value="terraform" label="Terraform">

```hcl
data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "dbt_polars" {
  scope                = azurerm_storage_account.dbt_polars.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
```

`azurerm_client_config` assigns the role to the user that runs Terraform.

</TabItem>
</Tabs>

The role assignment can take a few minutes to take effect.

## Configuring the profile

:::info
For this tutorial, we authenticate to Azure with our current user and save tables in Delta format. See the [parameter reference](../../catalogs/azure.md#parameter-reference) for all configuration options.
:::

Install the Azure catalog:

```bash
pip install 'dbt-polars[azure]'
```

dbt reads its connections from `profiles.yml`, by default in the `~/.dbt` folder. Add a profile for your project, named after the profile setting in dbt_project.yml:

```yaml
my_project:
  target: azure
  outputs:
    azure:
      type: polars
      schema: analytics
      catalog_type: azure
      account_name: <mystorageaccount>
      container: dbt-polars
```

Replace `<mystorageaccount>` with the name of your storage account. dbt-polars can use your Azure CLI login, so no key or token is stored in profiles.yml.

## Running the project

Run the project:

```bash
dbt run
```

The Azure catalog writes each model as a Delta table to the container, in a folder per schema:

```text
dbt-polars/                  # container
└── analytics/               # schema
    └── <model>/             # table
        ├── _delta_log/
        │   └── 00000000000000000000.json
        └── part-00001-<uuid>-c000.snappy.parquet
```

Open the storage account in the Azure portal to find them in the Storage browser.

## Read more

- [Azure catalog reference](../../catalogs/azure.md): all profile parameters, authentication options and other file formats, such as Parquet.
