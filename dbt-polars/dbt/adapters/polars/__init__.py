from dbt.adapters.polars.connections import PolarsConnectionManager
from dbt.adapters.polars.impl import PolarsAdapter
from dbt.adapters.polars.connections import PolarsCredentials
from dbt.include import polars

from dbt.adapters.base import AdapterPlugin

Plugin = AdapterPlugin(
    adapter=PolarsAdapter,
    credentials=PolarsCredentials,
    include_path=polars.PACKAGE_PATH,
)
