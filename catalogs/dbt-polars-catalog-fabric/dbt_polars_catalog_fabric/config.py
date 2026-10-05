from dbt.adapters.polars.catalogs import CatalogConfig


class FabricCatalogConfig(CatalogConfig):
    """Config for a schema-enabled Microsoft Fabric Lakehouse.

    `workspace` and `lakehouse` accept either display names or GUIDs. Every
    other keyword is passed straight through to `azure.identity.
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
        **kwargs: object,
    ) -> None:
        self.name = name
        self.type = type
        self.schema = schema
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.credential_kwargs: dict[str, object] = kwargs

    def unique_field(self) -> str:
        return f"{self.workspace}/{self.lakehouse}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "workspace", "lakehouse")
