# Custom catalogs

You can add your own storage backend as a separate Python package, without forking dbt-polars. Once the package is installed, select it in the profile with `type`.

## Naming convention

dbt-polars finds a catalog from its `type`:

| Profile `type` | Distribution name | Import name |
|---|---|---|
| `my_custom` | `dbt-polars-catalog-my-custom` | `dbt_polars_catalog_my_custom` |

The type must match `^[a-z][a-z0-9_]*$`. The names of the official catalogs (`local`, `iceberg`, `azure`, `s3` and `databricks`) are reserved. The local, S3, Azure and Databricks catalogs are packages built exactly this way; their source under [`catalogs/`](https://github.com/datamindedbe/incubator-dbt-polars/tree/main/catalogs) is a good starting point.

The package's top-level module must define:

- `config_class`: a `CatalogConfig` subclass. It receives every key of the profile's catalog entry (`name`, `type`, `schema` and your own options) as keyword arguments.
- `create_catalog(config, project_root)`: returns the catalog instance. The catalog class itself works for this.

## Implementing a catalog

Extend `StorageCatalog` for backends that store Delta tables or files at a URI. You implement how relations map to URIs, the storage options passed to Polars and delta-rs, and schema management. Reading, writing, merging, snapshots and comments are inherited.

```python
from dataclasses import dataclass

from dbt.adapters.polars.catalogs import CatalogConfig, PolarsRelation, StorageCatalog


@dataclass
class MyCustomCatalogConfig(CatalogConfig):
    name: str
    type: str
    schema: str
    bucket: str

    def unique_field(self) -> str:
        return self.bucket

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "bucket")


class MyCustomCatalog(StorageCatalog):
    config: MyCustomCatalogConfig

    def __init__(self, config: MyCustomCatalogConfig, project_root: str):
        super().__init__(config)

    def get_uri(self, relation: PolarsRelation) -> str: ...
    def get_storage_options(self, uri: str) -> dict[str, str] | None: ...
    def create_schema(self, relation: PolarsRelation) -> None: ...
    def drop_schema(self, relation: PolarsRelation) -> None: ...
    def list_schemas(self) -> list[str]: ...
    def table_exists(self, relation: PolarsRelation) -> bool: ...
    def drop_relation(self, relation: PolarsRelation) -> None: ...
    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]: ...


config_class = MyCustomCatalogConfig
create_catalog = MyCustomCatalog
```

For backends that don't store Delta tables or files, extend `BaseCatalog` and implement all of its methods.

The supported API is what `dbt.adapters.polars.catalogs` exports: `StorageCatalog`, `BaseCatalog`, `CatalogConfig`, `PolarsRelation`, `get_write_options`, `FILE_FORMATS`, `file_format_of` and `resolve_relative_path`. A relation's `file_format` is `None` when the model doesn't set one; in a `StorageCatalog`, `file_format_of(relation)` returns the effective format (`delta` by default). Pin a compatible range in your package, e.g. `dbt-polars>=0.3,<0.4`.

## Profile configuration

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      catalogs:
        - name: my_catalog
          type: my_custom
          schema: dev
          bucket: my-bucket
```

## Testing

dbt-polars ships two layers of reusable tests.

### Catalog tests

`dbt.adapters.polars.testing` calls your catalog directly, without dbt. They are fast and point straight at the failing method:

- `StorageCatalogTests`: schema management, writes, listing, concurrent writes and file formats. Use it for `StorageCatalog` subclasses; the data operations (appends, merges, snapshots, comments) are inherited and already tested in dbt-polars.
- `CatalogTests`: everything above except file formats, plus appends, merges, snapshots and comments. Use it for `BaseCatalog` subclasses, which implement those operations themselves.

Subclass one of them and provide a `catalog` fixture:

```python
import pytest

from dbt.adapters.polars.testing import StorageCatalogTests
from dbt_polars_catalog_my_custom import MyCustomCatalog, MyCustomCatalogConfig


class TestMyCustomCatalog(StorageCatalogTests):
    @pytest.fixture
    def catalog(self):
        config = MyCustomCatalogConfig(
            name="test", type="my_custom", schema="unused", bucket="test-bucket"
        )
        return MyCustomCatalog(config, "")
```

The tests are grouped per feature (`SchemaTests`, `WriteTests`, `IncrementalTests`, `SnapshotTests`, `CommentTests`, `ConcurrencyTests`, `FileFormatTests`), so you can combine only the ones that apply. To skip a single test, override it in your subclass with `@pytest.mark.skip`.

### dbt tests

`dbt.adapters.polars.testing.catalog` and `dbt.adapters.polars.testing.data_operations` run real dbt projects against your catalog, like dbt's own adapter tests. They need `dbt-tests-adapter`, which is not a dependency of dbt-polars:

```bash
pip install dbt-tests-adapter
```

Register the plugin and provide a `dbt_profile_target` fixture in your `conftest.py`. The schema is filled in per test class. The first catalog is the default; a second catalog named `secondary` is only needed for `BaseSeedWithExplicitCatalog*`:

```python
import pytest

pytest_plugins = ["dbt.adapters.polars.testing.plugin"]


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "polars",
        "catalogs": [
            {"type": "my_custom", "name": "primary", "bucket": "test-bucket"},
            {"type": "my_custom", "name": "secondary", "bucket": "test-bucket-2"},
        ],
    }
```

Then subclass the tests you want to run:

```python
from dbt.adapters.polars.testing.catalog import (
    BaseDocsGenerate,
    BaseSimpleMaterializations,
)


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestDocsGenerate(BaseDocsGenerate):
    pass
```

The tests are split in two groups:

- `catalog`: relevant for every catalog. Materializations, basic incremental and snapshot runs, seeds, docs generation, adapter methods and concurrency.
- `data_operations`: incremental strategies, snapshot variants, persisted docs and partitioning. Only needed when your catalog implements these operations itself, i.e. `BaseCatalog` subclasses or `StorageCatalog` subclasses that override them.
