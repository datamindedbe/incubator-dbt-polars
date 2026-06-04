# data "azuread_group" "all" {
#   display_name = "Dataminded all"
# }

resource "databricks_group" "admins" {
  provider     = databricks.account
  display_name = "dbt-polars-admins"
}

resource "databricks_mws_permission_assignment" "admins" {
  provider     = databricks.account
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  principal_id = databricks_group.admins.id
  permissions  = ["ADMIN"]
}

# Service principals (synthetic users — no @ in name)
resource "databricks_service_principal" "user" {
  provider     = databricks.account
  for_each     = local.unique_sp_users
  display_name = each.value
}

resource "databricks_mws_permission_assignment" "user" {
  provider     = databricks.account
  for_each     = databricks_service_principal.user
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  principal_id = each.value.id
  permissions  = ["USER"]
}

# Human users (real email addresses — looked up, not created).
# Permission assignment is intentionally omitted: real users already have
# workspace access via their Entra ID / AAD group membership.
data "databricks_user" "human" {
  for_each  = local.unique_human_users
  user_name = each.value
}

