resource "azurerm_databricks_workspace" "this" {
  name                          = "dbt-polars"
  resource_group_name           = local.resource_group_name
  location                      = local.location
  sku                           = "premium"
  managed_resource_group_name   = "rg-dbt-polars-databricks"
  public_network_access_enabled = true
}

resource "azurerm_databricks_access_connector" "this" {
  resource_group_name = local.resource_group_name
  location            = local.location
  name                = "dbt-polars"

  identity {
    type = "SystemAssigned"
  }
}

resource "databricks_storage_credential" "this" {
  name = "dbt-polars"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }
}
