
resource "databricks_schema" "schema" {
  for_each     = local.schemas
  name         = each.value.schema
  catalog_name = databricks_catalog.catalog[each.value.catalog].name
  owner        = databricks_group.admins.display_name
  storage_root = "${databricks_external_location.catalog[each.value.catalog].url}${each.value.schema}"
}

resource "databricks_grant" "schema_reader" {
  for_each = local.schema_readers

  principal  = databricks_service_principal.user[each.value.user].application_id
  schema     = databricks_schema.schema[each.value.key].id
  privileges = ["USE_SCHEMA"]
}

resource "databricks_grant" "schema_writer" {
  for_each = local.schema_writers

  principal  = databricks_service_principal.user[each.value.user].application_id
  schema     = databricks_schema.schema[each.value.key].id
  privileges = ["ALL_PRIVILEGES"]
}
