from dbt.adapters.base.plugin import AdapterPlugin
from dbt.adapters.polars.connections import PolarsCredentials
from dbt.adapters.polars.impl import PolarsAdapter
from dbt.include.polars import PACKAGE_PATH

Plugin = AdapterPlugin(
    adapter=PolarsAdapter,  # type: ignore[arg-type]
    credentials=PolarsCredentials,
    include_path=PACKAGE_PATH,
)
