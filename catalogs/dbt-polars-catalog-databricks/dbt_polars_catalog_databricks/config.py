from dbt.adapters.polars.catalogs import CatalogConfig
from dbt_common.exceptions import DbtRuntimeError

SUPPORTED_TABLE_FORMATS = ("delta",)


class DatabricksCatalogConfig(CatalogConfig):
    """Config for the Databricks/Unity Catalog backend.

    `catalog_name` is the only required dbt-polars-specific field (which Unity
    Catalog catalog to register tables in - schema storage locations are read
    from Unity Catalog itself, not configured here). Every other keyword is
    passed straight through to `databricks.sdk.Config`, so any auth method it
    supports (PAT, OAuth M2M, Azure client-secret, external-browser, azure-cli,
    databricks-cli profile, ...) works by supplying the matching kwargs.

    `persist_docs_http_path`: docs (table/column comments from persist_docs)
    are always written to the Delta table's own metadata. If a SQL warehouse's
    HTTP path (e.g. `/sql/1.0/warehouses/<warehouse_id>`, as shown on the
    warehouse's Connection Details page) is also set here, they're additionally
    written into Unity Catalog's native comment fields via `COMMENT ON TABLE`/
    `ALTER TABLE ... ALTER COLUMN ... COMMENT` - the only way to set those,
    since Unity Catalog's REST API has no endpoint for it at all. Left unset,
    docs never reach Unity Catalog itself.
    """

    def __init__(
        self,
        *,
        name: str,
        type: str,
        schema: str,
        catalog_name: str,
        host: str | None = None,
        persist_docs_http_path: str | None = None,
        table_format: str = "delta",
        **kwargs: object,
    ) -> None:
        if table_format not in SUPPORTED_TABLE_FORMATS:
            raise DbtRuntimeError(
                f"Databricks catalog '{name}': table_format '{table_format}' is not "
                f"supported, only {', '.join(SUPPORTED_TABLE_FORMATS)} for now."
            )
        self.name = name
        self.type = type
        self.schema = schema
        self.catalog_name = catalog_name
        self.persist_docs_http_path = persist_docs_http_path
        self.table_format = table_format
        self.sdk_kwargs: dict[str, object] = {
            **({"host": host} if host else {}),
            **kwargs,
        }

    def unique_field(self) -> str:
        return f"{self.catalog_name}/{self.schema}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "catalog_name", "host", "persist_docs_http_path")
