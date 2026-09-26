import pytest

from dbt.adapters.polars.catalogs import create_catalog, resolve_catalog_plugin
from dbt.adapters.polars.testing import CatalogTests

pytest.importorskip("pyiceberg")


class TestIcebergCatalog(CatalogTests):
    @pytest.fixture
    def catalog(self, tmp_path):
        config = resolve_catalog_plugin("iceberg").config_class(
            name="catalog_test",
            type="iceberg",
            schema="unused",
            pyiceberg_type="sql",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=f"file://{tmp_path}/warehouse",
        )
        return create_catalog(config, "")
