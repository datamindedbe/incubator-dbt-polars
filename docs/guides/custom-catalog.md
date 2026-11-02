---
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Custom catalogs

A custom catalog is the intended way to tailor dbt-polars to an existing setup. Organizations often have their own storage layout, authentication or governance tooling that none of the official catalogs match. Instead of forking dbt-polars, you write a catalog as a separate Python package. Once it is installed, a profile selects it with `type`, like any official catalog.

The official local, S3, Azure, Databricks, Fabric and Snowflake catalogs are built exactly this way. You can find their source code [here](https://github.com/datamindedbe/incubator-dbt-polars/tree/main/catalogs) as a reference.

## Creating the package

The [catalog template](https://github.com/datamindedbe/incubator-dbt-polars/tree/main/catalog-template) helps you to create a new catalog for dbt-polars. Clone it with [Copier](https://copier.readthedocs.io):

```bash
uvx copier copy gh:datamindedbe/incubator-dbt-polars dbt-polars-catalog-my-custom
```

Copier asks for:

* catalog_name: the catalog_type in the profile. The package is named `dbt-polars-catalog-<catalog_name>`.
* [base_class](#catalog-base-classes): the class your catalog extends.
* author_name, author_email: package metadata.

The generated project contains:

* `dbt_polars_catalog_my_custom/catalog.py` with the catalog, its config and every method to implement.
* `tests/` with the dbt-polars catalog tests, the dbt tests and a cleanup of test schemas.
* `.github/workflows/` with CI and a PyPI publish workflow.

Run `uvx copier update` in the generated project to pull in later template changes.

## Catalog base classes

The template lets you choose one of two base classes to start from:

| Class | Use it when | You implement |
|---|---|---|
| `StorageCatalog` (recommended) | Your backend stores Delta tables or Parquet/CSV/NDJSON files at a URI | URIs, storage options and schema management; reading, writing, merging, snapshots and comments are inherited |
| `BaseCatalog` | Any other backend | Every catalog operation |

This guide illustrates how to extend the `StorageCatalog`.

:::info
The StorageCatalog itself extends the BaseCatalog. If you only need to make minor changes to BaseCatalog you can still overwrite thise via a StorageCatalog extension.
:::

## What to implement

A `StorageCatalog` stores Delta tables, or Parquet, CSV and NDJSON files, at a URI. Reading, writing, merging, snapshots and comments are inherited. You implement where relations are stored and how schemas are managed:

| Name | Description |
|---|---|
| [Config](#config) | The options of the catalog entry in the profile. |
| [\_\_init\_\_](#__init__) | Called once per catalog in the profile. Resolve paths and set up clients here. |
| [get_uri](#get_uri) | The location of a relation. A Delta table is a directory, other formats are a single file. |
| [get_storage_options](#get_storage_options) | The storage options that Polars and delta-rs use to read and write a URI, such as credentials. |
| [create_schema](#create_schema-and-drop_schema) | Creates a schema. Does nothing when it already exists. |
| [drop_schema](#create_schema-and-drop_schema) | Drops a schema and all its tables. Does nothing when it doesn't exist. |
| [list_schemas](#list_schemas) | The names of all schemas in the catalog. |
| [table_exists](#table_exists-and-drop_relation) | Whether a relation exists. |
| [drop_relation](#table_exists-and-drop_relation) | Drops a relation. Does nothing when it doesn't exist. |
| [list_relations_without_caching](#list_relations_without_caching) | All relations in a schema, or an empty list when the schema doesn't exist. |

For backends that don't store Delta tables or files, extend `BaseCatalog` instead and implement all of its methods.

## Implementing the catalog

Most methods receive a `PolarsRelation` with a `schema` and `identifier`. Read its format with `file_format_of(relation)`, which defaults to `delta`. All methods must agree on one storage layout, such as `<root>/<schema>/<identifier>`. Threads share the catalog, so methods can run concurrently.

The examples are simplified from the [local](https://github.com/datamindedbe/incubator-dbt-polars/tree/main/catalogs/dbt-polars-catalog-local) and [Azure](https://github.com/datamindedbe/incubator-dbt-polars/tree/main/catalogs/dbt-polars-catalog-azure) catalogs.

:::info
A relation is dbt-core's terminology for a model. When implementing these methods, relation most often means table.
:::

### Config

The config defines the options of your catalog in profiles.yml. Each field is an option of the catalog entry. `unique_field` identifies the storage location for dbt's anonymous usage stats. `connection_keys` lists the options dbt debug shows, so leave out secrets.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

```python
@dataclass
class LocalCatalogConfig(CatalogConfig):
    name: str
    type: str
    schema: str
    root: str

    def unique_field(self) -> str:
        return self.root

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "root")
```

```yaml
catalogs:
  - name: my_local
    type: local
    schema: dev
    root: ./data
```

</TabItem>
<TabItem value="azure" label="Azure">

```python
@dataclass
class AzureCatalogConfig(CatalogConfig):
    name: str
    type: str
    schema: str
    account_name: str
    container: str
    prefix: str = ""
    credentials: dict = field(default_factory=dict)

    def unique_field(self) -> str:
        return f"{self.account_name}/{self.container}/{self.prefix}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "account_name", "container", "prefix")
```

```yaml
catalogs:
  - name: my_azure
    type: azure
    schema: dev
    account_name: mystorageaccount
    container: dbt-polars
    prefix: warehouse
```

</TabItem>
</Tabs>

### `__init__`

Runs once per catalog entry. Call `super().__init__(config)`, then do shared setup such as resolving paths against `project_root` or creating storage clients. Raise a `DbtRuntimeError` for an invalid config.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

```python
class LocalCatalog(StorageCatalog):
    config: LocalCatalogConfig

    def __init__(self, config: LocalCatalogConfig, project_root: str):
        super().__init__(config)
        self.absolute_root = resolve_relative_path(config.root, project_root)
```

</TabItem>
<TabItem value="azure" label="Azure">

```python
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


class AzureCatalog(StorageCatalog):
    config: AzureCatalogConfig

    def __init__(self, config: AzureCatalogConfig, project_root: str):
        super().__init__(config)

        # Authenticate once
        self.credential = DefaultAzureCredential()

        # Initialize clients once
        self.file_system = DataLakeServiceClient(
            f"https://{config.account_name}.dfs.core.windows.net",
            credential=self.credential,
        ).get_file_system_client(config.container)
```

</TabItem>
</Tabs>

### `get_uri`

Returns a path or URL that Polars and delta-rs can read and write. Point to the directory for Delta tables, and to the file with its extension for other formats. All inherited data operations use it, also for tables that don't exist yet.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

```python
    def get_uri(self, relation: PolarsRelation) -> str:
        return str(self.relation_path(relation))
```

</TabItem>
<TabItem value="azure" label="Azure">

```python
    def get_uri(self, relation: PolarsRelation) -> str:
        return f"az://{self.config.container}/{self.relation_path(relation)}"
```

</TabItem>
</Tabs>

### `get_storage_options`

Returns the credentials and settings Polars and delta-rs need for a URI, or `None`. It's called before every read and write, so cache tokens and refresh them before they expire. `read_only` is `True` for reads, which allows narrower credentials.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

The local filesystem needs no options.

```python
    def get_storage_options(
        self, uri: str, read_only: bool = False
    ) -> dict[str, str] | None:
        return None
```

</TabItem>
<TabItem value="azure" label="Azure">

The Azure catalog passes a bearer token for the storage account. The full catalog also caches the token and accepts account keys and SAS tokens from the profile.

```python
    def get_storage_options(
        self, uri: str, read_only: bool = False
    ) -> dict[str, str] | None:
        token = self.credential.get_token("https://storage.azure.com/.default")
        return {
            "account_name": self.config.account_name,
            "bearer_token": token.token,
        }
```

</TabItem>
</Tabs>

### `create_schema` and `drop_schema`

Both receive a relation with only a `schema`. `create_schema` must return without error when the schema already exists. `drop_schema` removes the schema with its tables, and must not raise when the schema doesn't exist.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

```python
    def create_schema(self, relation: PolarsRelation) -> None:
        self.schema_path(relation.schema).mkdir(parents=True, exist_ok=True)

    def drop_schema(self, relation: PolarsRelation) -> None:
        shutil.rmtree(self.schema_path(relation.schema), ignore_errors=True)
```

</TabItem>
<TabItem value="azure" label="Azure">

```python
    def create_schema(self, relation: PolarsRelation) -> None:
        self.file_system.create_directory(self.blob_path(relation.schema))

    def drop_schema(self, relation: PolarsRelation) -> None:
        directory = self.file_system.get_directory_client(
            self.blob_path(relation.schema)
        )
        try:
            directory.delete_directory()
        except ResourceNotFoundError:
            pass
```

</TabItem>
</Tabs>

### `list_schemas`

Returns a list of all schemas in the catalog.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

```python
    def list_schemas(self) -> list[str]:
        if not self.absolute_root.exists():
            return []
        return [p.name for p in self.absolute_root.iterdir() if p.is_dir()]
```

</TabItem>
<TabItem value="azure" label="Azure">

```python
    def list_schemas(self) -> list[str]:
        try:
            paths = self.file_system.get_paths(
                path=self.config.prefix, recursive=False
            )
            return [p.name.split("/")[-1] for p in paths if p.is_directory]
        except ResourceNotFoundError:
            return []
```

</TabItem>
</Tabs>

### `table_exists` and `drop_relation`

`table_exists` indicates whether the table exists. `drop_relation` drops a table.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

```python
    def table_exists(self, relation: PolarsRelation) -> bool:
        return self.relation_path(relation).exists()

    def drop_relation(self, relation: PolarsRelation) -> None:
        path = self.relation_path(relation)
        if file_format_of(relation) == "delta":
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()
```

</TabItem>
<TabItem value="azure" label="Azure">

```python
    def table_exists(self, relation: PolarsRelation) -> bool:
        if file_format_of(relation) == "delta":
            uri = self.get_uri(relation)
            return DeltaTable.is_deltatable(
                uri, storage_options=self.get_storage_options(uri, read_only=True)
            )
        return self.file_system.get_file_client(self.relation_path(relation)).exists()

    def drop_relation(self, relation: PolarsRelation) -> None:
        path = self.relation_path(relation)
        try:
            if file_format_of(relation) == "delta":
                self.file_system.get_directory_client(path).delete_directory()
            else:
                self.file_system.get_file_client(path).delete_file()
        except ResourceNotFoundError:
            pass
```

</TabItem>
</Tabs>

### `list_relations_without_caching`

Fills dbt's relation cache at the start of a run and is used by dbt docs generate. Return a `RelationType.Table` relation for each table in the schema, with `file_format` set for non-Delta files. Return an empty list when the schema doesn't exist.

<Tabs groupId="custom-catalog-example">
<TabItem value="local" label="Local" default>

```python
    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        schema_path = self.schema_path(schema_relation.schema)
        if not schema_path.exists():
            return []

        relations = []
        for path in schema_path.iterdir():
            if path.is_dir() and DeltaTable.is_deltatable(str(path)):
                identifier, file_format = path.name, None
            elif path.is_file() and path.suffix.lstrip(".") in FILE_FORMATS:
                identifier, file_format = path.stem, path.suffix.lstrip(".")
            else:
                continue
            relations.append(
                schema_relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=identifier,
                    type=RelationType.Table,
                    catalog=schema_relation.catalog,
                    file_format=file_format,
                )
            )
        return relations
```

</TabItem>
<TabItem value="azure" label="Azure">

```python
    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        try:
            paths = list(
                self.file_system.get_paths(
                    path=self.blob_path(schema_relation.schema), recursive=False
                )
            )
        except ResourceNotFoundError:
            return []

        relations = []
        for path in paths:
            name = path.name.split("/")[-1]
            stem, _, extension = name.rpartition(".")
            uri = f"az://{self.config.container}/{path.name}"
            if path.is_directory and DeltaTable.is_deltatable(
                uri, storage_options=self.get_storage_options(uri, read_only=True)
            ):
                identifier, file_format = name, None
            elif not path.is_directory and extension in FILE_FORMATS:
                identifier, file_format = stem, extension
            else:
                continue
            relations.append(
                schema_relation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=identifier,
                    type=RelationType.Table,
                    catalog=schema_relation.catalog,
                    file_format=file_format,
                )
            )
        return relations
```

</TabItem>
</Tabs>

## Optional features

* External locations: set `supports_external_location = True`, and handle the external paths in `get_uri`.
* Model config keys: list your own keys in `model_config_keys` to avoid dbt-polars rejecting them.

## Using the catalog

Install the package next to dbt-polars and select it in the profile with `type`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalogs:
        - name: my_catalog
          # provided by the package dbt-polars-catalog-my-custom
          type: my_custom
          schema: dev
          bucket: my-bucket
```

## Testing

The generated project runs two layers of reusable tests from dbt-polars. Fill in the catalog options of two test instances in `tests/conftest.py`, such as two buckets or two prefixes. They must not share storage, because some tests check that a table lands in one catalog and not in the other.

### Catalog tests

The tests in `dbt.adapters.polars.testing` call your catalog directly, without dbt. They are fast and point straight at the failing method.

- `StorageCatalogTests` tests schema management, writes, listing, concurrent writes and file formats. Use it for `StorageCatalog` subclasses. The data operations (appends, merges, snapshots, comments) are inherited and already tested in dbt-polars.
- `CatalogTests` tests everything above except file formats, plus appends, merges, snapshots and comments. Use it for `BaseCatalog` subclasses, which implement those operations themselves.

The tests are grouped per feature (`SchemaTests`, `WriteTests`, `IncrementalTests`, `SnapshotTests`, `CommentTests`, `ConcurrencyTests`, `FileFormatTests`), so you can combine only the ones that apply. To skip a single test, override it in your subclass with `@pytest.mark.skip`.

### dbt tests

The tests in `dbt.adapters.polars.testing.catalog` and `dbt.adapters.polars.testing.data_operations` run real dbt projects against your catalog, like dbt's own adapter tests. They need `dbt-tests-adapter`, which the generated project installs as a test dependency.

- `catalog` tests are relevant for every catalog. They cover materializations, basic incremental and snapshot runs, seeds, docs generation, adapter methods and concurrency.
- `data_operations` tests cover incremental strategies, snapshot variants, persisted docs and partitioning. They are only needed when your catalog implements these operations itself, as `BaseCatalog` subclasses do.
