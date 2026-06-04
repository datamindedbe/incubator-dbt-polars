
resource "databricks_schema" "schema" {
  for_each      = local.schemas
  name          = each.value.schema
  catalog_name  = databricks_catalog.catalog[each.value.catalog].name
  owner         = databricks_group.admins.display_name
  storage_root  = format("%s/%s", trim(databricks_external_location.catalog[each.value.catalog].url, "/"), each.value.schema)
  force_destroy = true
}

resource "databricks_grant" "schema_reader" {
  for_each = local.schema_readers

  principal  = local.principal_id[each.value.user]
  schema     = databricks_schema.schema[each.value.key].id
  privileges = ["USE_SCHEMA"]
}

resource "databricks_grant" "schema_writer" {
  for_each = local.schema_writers

  principal  = local.principal_id[each.value.user]
  schema     = databricks_schema.schema[each.value.key].id
  privileges = ["ALL_PRIVILEGES"]
}
