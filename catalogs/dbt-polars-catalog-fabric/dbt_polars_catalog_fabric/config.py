from dbt.adapters.polars.catalogs import SUPPORTED_TABLE_FORMATS, CatalogConfig
from dbt_common.exceptions import DbtRuntimeError


class FabricCatalogConfig(CatalogConfig):
    """Config for a schema-enabled Microsoft Fabric Lakehouse.

    `workspace` and `lakehouse` accept either display names or GUIDs.
    `default_table_format` is the format of new tables, `delta` or `iceberg`;
    models override it with their own `table_format` config. Every other
    keyword is passed straight through to `azure.identity.
    DefaultAzureCredential`, so the principal running dbt authenticates with
    whatever that chain resolves (environment, workload identity, Azure CLI,
    managed identity, ...).
    """

    def __init__(
        self,
        *,
        name: str,
        type: str,
        schema: str,
        workspace: str,
        lakehouse: str,
        default_table_format: str = "delta",
        **kwargs: object,
    ) -> None:
        if default_table_format not in SUPPORTED_TABLE_FORMATS:
            raise DbtRuntimeError(
                f"Fabric catalog '{name}': default_table_format "
                f"'{default_table_format}' is not supported, use one of: "
                f"{', '.join(SUPPORTED_TABLE_FORMATS)}."
            )
        self.name = name
        self.type = type
        self.schema = schema
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.default_table_format = default_table_format
        self.credential_kwargs: dict[str, object] = kwargs

    def unique_field(self) -> str:
        return f"{self.workspace}/{self.lakehouse}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "workspace", "lakehouse", "default_table_format")
