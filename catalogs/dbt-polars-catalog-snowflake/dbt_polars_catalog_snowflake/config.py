from dbt.adapters.polars.catalogs import CatalogConfig, IcebergCatalogConfig

HORIZON_URI = "https://{account}.snowflakecomputing.com/polaris/api/catalog"


class SnowflakeCatalogConfig(CatalogConfig):
    """Config for Snowflake-managed Iceberg tables through Horizon Catalog.

    `account` is the `<org>-<account>` identifier, `database` the Snowflake
    database holding the tables and `role` the role to act as.
    `uppercase_column_names` stores column names in UPPERCASE, as Snowflake does
    for unquoted names, and lower-cases them again when dbt-polars reads. Every
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
        database: str,
        role: str | None = None,
        uppercase_column_names: bool = True,
        **kwargs: object,
    ) -> None:
        self.name = name
        self.type = type
        self.schema = schema
        self.account = account
        self.database = database
        self.role = role
        self.uppercase_column_names = uppercase_column_names
        self.pyiceberg_kwargs: dict[str, object] = kwargs

    def iceberg_config(self) -> IcebergCatalogConfig:
        role_scope = {"scope": f"session:role:{self.role.upper()}"} if self.role else {}
        return IcebergCatalogConfig(
            name=self.name,
            type="iceberg",
            schema=self.schema,
            pyiceberg_type="rest",
            uri=HORIZON_URI.format(account=self.account),
            warehouse=self.database.upper(),
            **{
                "header.X-Iceberg-Access-Delegation": "vended-credentials",
                **role_scope,
                **self.pyiceberg_kwargs,
            },
        )

    def unique_field(self) -> str:
        return f"{self.account}/{self.database}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "account", "database", "role")
