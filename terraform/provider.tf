terraform {
  required_version = "~> 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.45"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "1.52.0"
    }
  }

  backend "azurerm" {
    storage_account_name = "dbtpolarsstate"
    container_name       = "state"
    key                  = "dbt-polars.tfstate"
    resource_group_name  = "dbt-polars"
    subscription_id      = "4b72a73f-e970-40e1-b041-499eebd327a7"
  }
}

provider "azuread" {
  tenant_id = "55226c2c-0b83-4621-a5cd-e8e0e57ec920"
}

provider "azurerm" {
  subscription_id = "4b72a73f-e970-40e1-b041-499eebd327a7"

  features {
    key_vault {
      recover_soft_deleted_key_vaults = true
      purge_soft_delete_on_destroy    = false
    }
  }
  storage_use_azuread = true
}

provider "databricks" {
  host = azurerm_databricks_workspace.this.workspace_url
}


provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = "ceaf1e38-1947-4da4-bdfc-c20649d9e1f0"
}
