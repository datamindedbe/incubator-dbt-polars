# Local catalog

The local catalog stores tables as files on your local filesystem. It is the simplest catalog to set up and requires no extra dependencies.

**Status:** Usable

## Installation

No extra dependencies needed beyond the base package:

```bash
pip install dbt-polars
```

## Profile configuration

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      schema: dev
      catalogs:
        - name: my_catalog
          type: local
          root: ./data          # path to the root directory; relative paths resolve from the dbt project root
```

### Configuration options

| Key | Required | Description |
|---|---|---|
| `name` | Yes | Catalog name. Used as the `database` identifier in dbt. |
| `type` | Yes | Must be `local`. |
| `root` | Yes | Root directory where schemas and tables are stored. Relative paths resolve from the dbt project root. Paths with spaces are not supported (Polars limitation). |

## Directory layout

The local catalog organises data as:

```
<root>/
  <schema>/
    <table_name>/          # Delta Lake table (directory)
    <table_name>.parquet   # Parquet table
    <table_name>.csv       # CSV table
    <table_name>.ndjson    # NDJSON table
```

Schemas are directories. Creating a schema creates the directory; dropping it removes the directory and all its contents.
