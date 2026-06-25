from dbt.adapters.base import AdapterPlugin
from dbt.adapters.polars.connections import PolarsConnectionManager, PolarsCredentials
from dbt.adapters.polars.impl import PolarsAdapter
from dbt.include import polars

Plugin = AdapterPlugin(
    adapter=PolarsAdapter,
    credentials=PolarsCredentials,
    include_path=polars.PACKAGE_PATH,
)
