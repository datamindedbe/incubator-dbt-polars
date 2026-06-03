locals {
  config = yamldecode(file("access.yaml"))

  domains = keys(local.config)
  schemas = merge([
    for domain in local.domains :
    merge([
      for schema in keys(local.config[domain]) :
      {
        "${domain}:${schema}" : {
          "catalog" : domain,
          "schema" : schema,
          "writers" : local.config[domain][schema]["writers"],
          "readers" : local.config[domain][schema]["readers"]
        }
      }
    ]...)
  ]...)

  domain_users = merge([
    for domain in local.domains :
    merge([
      for schema in keys(local.config[domain]) :
      merge([
        for user in concat(
          local.config[domain][schema]["writers"],
          local.config[domain][schema]["readers"]
        ) :
        {
          "${domain}:${user}" : {
            "domain" : domain,
            "user" : user
          }
        }
      ]...)
    ]...)
  ]...)

  unique_users = toset(concat(
    concat([
      for schema in values(local.schemas) :
      schema.writers
    ]...),
    concat([
      for schema in values(local.schemas) :
      schema.readers
    ]...)
  ))

  schema_readers = merge([
    for key, schema in local.schemas :
    merge([
      for user in schema.readers :
      {
        "${schema.catalog}:${schema.schema}:${user}" : {
          "key" : key,
          "catalog" : schema.catalog,
          "schema" : schema.schema,
          "user" : user
        }
      }
    ]...)
  ]...)

  schema_writers = merge([
    for key, schema in local.schemas :
    merge([
      for user in schema.writers :
      {
        "${schema.catalog}:${schema.schema}:${user}" : {
          "key" : key,
          "catalog" : schema.catalog,
          "schema" : schema.schema,
          "user" : user
        }
      }
    ]...)
  ]...)
}
