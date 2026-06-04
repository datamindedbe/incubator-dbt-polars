"""Databricks Unity Catalog client and helpers."""

from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional, Tuple  # Tuple used by _cred_cache

import polars as pl
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_oauth_token(host: str, client_id: str, client_secret: str) -> str:
    """Exchange a Databricks service principal client_id/secret for an access token.

    Uses the workspace-local OIDC endpoint — no Azure AD dependency.
    """
    resp = requests.post(
        f"{host.rstrip('/')}/oidc/v1/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "all-apis",
        },
        timeout=30,
        verify=False,  # corporate CA bundle not in system trust store
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


_POLARS_TO_UC: Dict[str, str] = {
    "Int8": "BYTE",
    "Int16": "SHORT",
    "Int32": "INT",
    "Int64": "LONG",
    "UInt8": "BYTE",
    "UInt16": "SHORT",
    "UInt32": "INT",
    "UInt64": "LONG",
    "Float32": "FLOAT",
    "Float64": "DOUBLE",
    "Boolean": "BOOLEAN",
    "Utf8": "STRING",
    "String": "STRING",
    "Date": "DATE",
    "Datetime": "TIMESTAMP",
    "Duration": "INTERVAL",
    "Decimal": "DECIMAL(38,18)",
}


def _polars_schema_to_uc_columns(schema: pl.Schema) -> List[Dict[str, Any]]:
    return [
        {
            "name": col,
            "type_text": _POLARS_TO_UC.get(str(dtype).split("[")[0], "STRING"),
            "type_name": _POLARS_TO_UC.get(str(dtype).split("[")[0], "STRING"),
            "position": i,
            "nullable": True,
        }
        for i, (col, dtype) in enumerate(schema.items())
    ]


def _account_name_from_location(storage_location: str) -> str:
    m = re.search(r"@([^.]+)\.", storage_location)
    return m.group(1) if m else ""


class DatabricksClient:
    def __init__(self, host: str, token: str) -> None:
        self.host = host.rstrip("/")
        self._s = requests.Session()
        self._s.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )
        self._s.verify = False  # corporate CA bundle not in system trust store
        # (table_id, operation) -> (storage_options, expiry_epoch_seconds)
        self._cred_cache: Dict[Tuple[str, str], Tuple[Dict[str, str], float]] = {}
        # (catalog, schema, table) -> table metadata dict from list_tables / get_table
        self._table_cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _raise(self, r: "requests.Response") -> None:
        try:
            r.raise_for_status()
        except requests.HTTPError as exc:
            raise requests.HTTPError(f"{exc} — {r.text}", response=r) from exc

    def _get(self, path: str, **params: Any) -> Any:
        r = self._s.get(f"{self.host}{path}", params=params, timeout=30)
        self._raise(r)
        return r.json()

    def _post(self, path: str, body: Dict[str, Any]) -> Any:
        r = self._s.post(f"{self.host}{path}", json=body, timeout=30)
        self._raise(r)
        return r.json()

    def _patch(self, path: str, body: Dict[str, Any]) -> Any:
        r = self._s.patch(f"{self.host}{path}", json=body, timeout=30)
        self._raise(r)
        return r.json()

    def _delete(self, path: str, **params: Any) -> None:
        r = self._s.delete(f"{self.host}{path}", params=params, timeout=30)
        self._raise(r)

    # ------------------------------------------------------------------
    # Schemas
    # ------------------------------------------------------------------

    def list_schemas(self, catalog: str) -> List[str]:
        data = self._get("/api/2.1/unity-catalog/schemas", catalog_name=catalog)
        return [s["name"] for s in data.get("schemas", [])]

    def get_schema(self, catalog: str, schema: str) -> Dict[str, Any]:
        return self._get(f"/api/2.1/unity-catalog/schemas/{catalog}.{schema}")

    def create_schema(self, catalog: str, schema: str) -> None:
        self._post(
            "/api/2.1/unity-catalog/schemas",
            {
                "catalog_name": catalog,
                "name": schema,
            },
        )

    def drop_schema(self, catalog: str, schema: str) -> None:
        self._delete(
            f"/api/2.1/unity-catalog/schemas/{catalog}.{schema}",
            force="true",
        )

    # ------------------------------------------------------------------
    # Tables
    # ------------------------------------------------------------------

    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        data = self._get(
            "/api/2.1/unity-catalog/tables",
            catalog_name=catalog,
            schema_name=schema,
        )
        tables = data.get("tables", [])
        for t in tables:
            name = t.get("name")
            if name:
                self._table_cache[(catalog, schema, name)] = t
        return tables

    def get_table(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        cached = self._table_cache.get((catalog, schema, table))
        if cached is not None:
            return cached
        result = self._get(f"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}")
        self._table_cache[(catalog, schema, table)] = result
        return result

    def table_exists(self, catalog: str, schema: str, table: str) -> bool:
        try:
            self.get_table(catalog, schema, table)
            return True
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return False
            raise

    def register_table(
        self,
        catalog: str,
        schema: str,
        name: str,
        storage_location: str,
    ) -> Dict[str, Any]:
        # Omit columns — UC infers the schema from the Delta log after the
        # first write.  Call refresh_table() after writing data to sync it.
        return self._post(
            "/api/2.1/unity-catalog/tables",
            {
                "catalog_name": catalog,
                "schema_name": schema,
                "name": name,
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": storage_location,
            },
        )

    def drop_table(self, catalog: str, schema: str, table: str) -> None:
        self._delete(f"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}")

    def rename_table(
        self, catalog: str, schema: str, old_name: str, new_name: str
    ) -> None:
        self._patch(
            f"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{old_name}",
            {"name": new_name},
        )

    # ------------------------------------------------------------------
    # Credential vending
    # ------------------------------------------------------------------

    def vend_storage_credentials(
        self, table_id: str, operation: str = "READ"
    ) -> Dict[str, str]:
        """Return polars/deltalake storage_options for the given table.

        Valid operation values: "READ", "READ_WRITE".

        Credentials are cached by (table_id, operation) and reused until
        5 minutes before their expiration_time.  Write credentials are
        cached too — a fresh write to the same table within the same run
        reuses the same short-lived token.
        """
        key = (table_id, operation)
        cached = self._cred_cache.get(key)
        if cached is not None:
            storage_options, expiry = cached
            if time.time() < expiry - 300:  # 5-minute safety buffer
                return storage_options

        raw = self._post(
            "/api/2.1/unity-catalog/temporary-table-credentials",
            {"table_id": table_id, "operation": operation},
        )

        if "azure_user_delegation_sas" in raw:
            sas = raw["azure_user_delegation_sas"]
            storage_options = {"azure_storage_sas_token": sas["sas_token"]}
        elif "azure_service_principal" in raw:
            sp = raw["azure_service_principal"]
            storage_options = {
                "azure_client_id": sp["application_id"],
                "azure_client_secret": sp["client_secret"],
                "azure_tenant_id": sp["directory_id"],
            }
        else:
            raise ValueError(
                f"Unrecognised credential type in vending response: {list(raw)}"
            )

        expiry = raw.get("expiration_time", 0) / 1000  # ms → seconds
        if expiry > 0:
            self._cred_cache[key] = (storage_options, expiry)

        return storage_options

    # ------------------------------------------------------------------
    # Convenience: write + register a Delta table in one call
    # ------------------------------------------------------------------

    def write_and_register(
        self,
        df: pl.DataFrame,
        catalog: str,
        schema: str,
        name: str,
    ) -> None:
        """Write a Polars DataFrame as a Delta table on ADLS and register it in UC.

        For existing tables: the storage location comes from the UC table metadata.
        For new tables: the storage location is derived from the schema's storage_root
        in UC, then the table is registered before writing so we can vend credentials.
        """
        if self.table_exists(catalog, schema, name):
            info = self.get_table(catalog, schema, name)
            storage_location = info["storage_location"]
            table_id = info["table_id"]
            write_creds = self.vend_storage_credentials(table_id, "READ_WRITE")
            df.write_delta(
                storage_location, mode="overwrite", storage_options=write_creds,
                delta_write_options={"schema_mode": "overwrite"},
            )
        else:
            schema_info = self.get_schema(catalog, schema)
            storage_root = schema_info.get("storage_root", "")
            if not storage_root:
                raise ValueError(
                    f"Schema {catalog}.{schema} has no storage_root in Unity Catalog. "
                    "Set a managed storage location on the schema."
                )
            # dbt internal tables (names containing __dbt_) are stored under a
            # _dbt/ subdirectory.  This ensures their paths never share a string
            # prefix with a final table name (which would trigger UC's
            # LOCATION_OVERLAP check when the same temp name is reused on the
            # next run).
            if "__dbt_" in name:
                storage_location = f"{storage_root.rstrip('/')}/_dbt/{name}"
            else:
                storage_location = f"{storage_root.rstrip('/')}/{name}"
            # Register first to obtain a table_id, then vend credentials for the write.
            info = self.register_table(catalog, schema, name, storage_location)
            table_id = info["table_id"]
            write_creds = self.vend_storage_credentials(table_id, "READ_WRITE")
            df.write_delta(
                storage_location, mode="overwrite", storage_options=write_creds,
                delta_write_options={"schema_mode": "overwrite"},
            )


# ------------------------------------------------------------------
# SQL context builder for Unity Catalog
# ------------------------------------------------------------------


def build_uc_sql_context(
    client: DatabricksClient,
    catalog: str,
    schema: str,
    exclude: set = frozenset(),
    needed: set = frozenset(),
) -> pl.SQLContext:
    """Register UC tables as LazyFrames for SQL execution.

    Only tables whose names appear in `needed` are registered (and credential
    vending is skipped for everything else).  Calling vend_storage_credentials
    for every table in the schema on every model run hits Databricks rate
    limits as the schema grows.
    """
    import sys as _sys
    import time as _time

    t_list_start = _time.perf_counter()
    tables = client.list_tables(catalog, schema)
    t_list_end = _time.perf_counter()

    ctx = pl.SQLContext()
    t_vend_total = 0.0
    t_scan_total = 0.0
    registered = []

    for table in tables:
        name = table.get("name", "")
        if name in exclude or (needed and name not in needed):
            continue
        table_id = table.get("table_id", "")
        location = table.get("storage_location", "")
        if not (name and table_id and location):
            continue
        try:
            tv0 = _time.perf_counter()
            storage_options = client.vend_storage_credentials(table_id, "READ")
            tv1 = _time.perf_counter()
            ctx.register(name, pl.scan_delta(location, storage_options=storage_options))
            tv2 = _time.perf_counter()
            t_vend_total += tv1 - tv0
            t_scan_total += tv2 - tv1
            registered.append(name)
        except Exception:
            pass

    print(
        f"[timing] build_uc_sql_context: "
        f"list_tables={t_list_end - t_list_start:.2f}s  "
        f"vend_creds={t_vend_total:.2f}s  "
        f"scan_delta={t_scan_total:.2f}s  "
        f"tables={registered}",
        file=_sys.stderr,
    )
    return ctx
