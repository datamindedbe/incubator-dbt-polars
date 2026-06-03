resource "databricks_catalog" "catalog" {
  for_each       = toset(local.domains)
  name           = each.value
  isolation_mode = "ISOLATED"
  owner          = databricks_group.admins.display_name
  storage_root   = trim(databricks_external_location.catalog[each.value].url, "/")
}

resource "azurerm_storage_container" "catalog" {
  for_each             = toset(local.domains)
  name                 = each.value
  storage_account_name = azurerm_storage_account.this.name
}

resource "databricks_external_location" "catalog" {
  for_each        = toset(local.domains)
  name            = each.value
  credential_name = databricks_storage_credential.this.name
  url = format("abfss://%s@%s.dfs.core.windows.net",
    azurerm_storage_container.catalog[each.value].name,
    azurerm_storage_account.this.name
  )
}

resource "databricks_grant" "external_location" {
  for_each = local.domain_users

  principal         = databricks_service_principal.user[each.value.user].application_id
  external_location = databricks_external_location.catalog[each.value.domain].id
  privileges        = ["CREATE_EXTERNAL_TABLE", "EXTERNAL_USE_LOCATION"]
}

resource "databricks_grant" "catalog" {
  for_each = local.domain_users

  principal  = databricks_service_principal.user[each.value.user].application_id
  catalog    = databricks_catalog.catalog[each.value.domain].id
  privileges = ["USE_CATALOG"]
}
