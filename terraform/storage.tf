locals {
  resource_group_name = "dbt-polars"
  location            = "westeurope"
}

resource "azurerm_storage_account" "this" {
  resource_group_name      = local.resource_group_name
  name                     = "dbtpolars"
  access_tier              = "Hot"
  account_replication_type = "LRS"
  account_tier             = "Standard"
  location                 = local.location
  is_hns_enabled           = true
}

resource "azurerm_role_assignment" "databricks_access_sa" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

