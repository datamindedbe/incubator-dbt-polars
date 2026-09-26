import pytest
from dbt.adapters.polars.catalogs import create_catalog, resolve_catalog_plugin
from dbt.adapters.polars.testing import CatalogTests, FileFormatTests


class TestLocalCatalog(CatalogTests, FileFormatTests):
    @pytest.fixture
    def catalog(self, tmp_path):
        config = resolve_catalog_plugin("local").config_class(
            name="catalog_test", type="local", schema="unused", root=str(tmp_path)
        )
        return create_catalog(config, "")
