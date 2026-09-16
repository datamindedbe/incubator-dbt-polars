import pytest
from dbt.exceptions import CompilationError

from tests.functional.adapter.sql_macros.base_utils import BaseUtils
from tests.functional.adapter.sql_macros.fixture_listagg import (
    models__test_listagg_sql,
    models__test_listagg_yml,
    seeds__data_listagg_csv,
    seeds__data_listagg_output_csv,
)


class BaseListagg(BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": seeds__data_listagg_csv,
            "data_listagg_output.csv": seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                models__test_listagg_sql, "listagg"
            ),
        }


@pytest.mark.xfail(
    strict=True,
    raises=CompilationError,
    reason=(
        "dbt-polars only supports listagg() without an explicit order or limit — "
        "Polars' SQL engine has no WITHIN GROUP clause or array-slicing function. "
        "This fixture exercises the ordered/limited variants. Revisit if Polars "
        "adds support."
    ),
)
class TestListagg(BaseListagg):
    pass
