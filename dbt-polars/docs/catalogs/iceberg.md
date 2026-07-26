# Iceberg catalog

The Iceberg catalog stores tables in Apache Iceberg format using [pyiceberg](https://py.iceberg.apache.org/). Any backend that pyiceberg supports — SQLite, REST, Hive, Glue, Databricks Unity Catalog — can be used by passing the appropriate pyiceberg properties in the profile.

**Status:** Usable (SQLite), Experimental (REST / Databricks / other backends)

## Installation

```bash
pip install 'dbt-polars[iceberg]'
```

## Profile configuration

All keys beyond `name` and `type` are forwarded to pyiceberg's `load_catalog`. Use `pyiceberg_type` to set the pyiceberg `type` property without conflicting with the dbt-polars `type` key.

### SQLite metastore (local development)

A fully local Iceberg setup with a SQLite catalog and a local warehouse:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      schema: dev
      catalogs:
        - name: my_catalog
          type: iceberg
          uri: sqlite:///./catalog.db       # SQLite database for the Iceberg metastore
          warehouse: file:///./warehouse    # directory where table data is written
```

### Databricks Unity Catalog (REST)

```yaml
catalogs:
  - name: my_uc_catalog
    type: iceberg
    pyiceberg_type: rest               # sets pyiceberg type=rest without overriding dbt-polars type
    uri: https://<workspace>.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest/
    token: <databricks-pat-or-token>
    warehouse: <unity-catalog-name>    # the Unity Catalog catalog name (e.g. "main")
```

### Other REST catalogs

Any pyiceberg REST-compatible catalog can be configured the same way:

```yaml
catalogs:
  - name: my_catalog
    type: iceberg
    pyiceberg_type: rest
    uri: https://my-iceberg-rest-catalog/
    warehouse: my_warehouse
    # additional pyiceberg properties as needed
```

### Configuration options

| Key | Required | Description |
|---|---|---|
| `name` | Yes | Catalog name. Used as the `database` identifier in dbt. |
| `type` | Yes | Must be `iceberg`. |
| `pyiceberg_type` | No | Sets the pyiceberg `type` property (e.g. `rest`, `hive`, `glue`). Use instead of `type` for backends that require a pyiceberg type. |
| All other keys | No | Passed through to `pyiceberg.catalog.load_catalog`. See the [pyiceberg docs](https://py.iceberg.apache.org/configuration/) for backend-specific options. |

## Notes

Iceberg tables have no `file_format` setting — the format is always Iceberg. The `file_format` model config is ignored for Iceberg catalogs.

Unsigned integer types (`UInt8`, `UInt16`, `UInt32`, `UInt64`) are automatically cast to their signed equivalents before writing, because Iceberg has no unsigned integer types.
