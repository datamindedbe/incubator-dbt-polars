from dbt.adapters.polars.catalogs import (
    CatalogConfig,
    IcebergCatalog,
    IcebergCatalogConfig,
)

HORIZON_URI = "https://{account}.snowflakecomputing.com/polaris/api/catalog"


class SnowflakeCatalogConfig(CatalogConfig):
    """Config for Snowflake-managed Iceberg tables through Horizon Catalog.

    `account` is the `<org>-<account>` identifier, `database_name` the Snowflake
    database holding the tables and `role` the role to act as. Every
    other keyword is passed straight through to pyiceberg's REST catalog, so any
    authentication it supports works, e.g. `credential` for a programmatic
    access token or `token` for an OAuth access token.
    """

    def __init__(
        self,
        *,
        name: str,
        type: str,
        schema: str,
        account: str,
        database_name: str,
        role: str | None = None,
        **kwargs: object,
    ) -> None:
        self.name = name
        self.type = type
        self.schema = schema
        self.account = account
        self.database_name = database_name
        self.role = role
        self.pyiceberg_kwargs: dict[str, object] = kwargs

    def iceberg_config(self) -> IcebergCatalogConfig:
        role_scope = {"scope": f"session:role:{self.role.upper()}"} if self.role else {}
        return IcebergCatalogConfig(
            name=self.name,
            type="iceberg",
            schema=self.schema,
            pyiceberg_type="rest",
            uri=HORIZON_URI.format(account=self.account),
            warehouse=self.database_name.upper(),
            **{
                "header.X-Iceberg-Access-Delegation": "vended-credentials",
                **role_scope,
                **self.pyiceberg_kwargs,
            },
        )

    def unique_field(self) -> str:
        return f"{self.account}/{self.database_name}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "account", "database_name", "role")


class SnowflakeCatalog(IcebergCatalog):
    """Snowflake-managed Iceberg tables through Snowflake Horizon Catalog.

    Uses Horizon's Iceberg REST endpoint with credentials vended by Snowflake,
    so no Snowflake warehouse is needed. Schema and table names are stored in
    UPPERCASE, as Snowflake does for unquoted names, and lower-cased again for
    dbt.
    """

    config: SnowflakeCatalogConfig  # type: ignore[assignment]
    min_commit_interval = 2

    def __init__(self, config: SnowflakeCatalogConfig, project_root: str) -> None:
        super().__init__(config.iceberg_config(), project_root)
        self.config = config

    def catalog_name(self, name: str) -> str:
        return name.upper()

    def dbt_name(self, name: str) -> str:
        return name.lower()
