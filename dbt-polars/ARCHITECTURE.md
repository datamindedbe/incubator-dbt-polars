# dbt-polars Architecture Decisions

## Overview

dbt-polars is a dbt adapter that executes models using the Polars DataFrame library and persists all relations as Delta Lake tables. It supports two targets:

- **Local** — catalog is a directory tree on the local filesystem. No server required.
- **Databricks Unity Catalog** — catalog is managed by Unity Catalog; data lives on Azure Data Lake Storage Gen2. No SQL compute required.

---

## ADR 1: Local filesystem as the catalog

**Decision:** The catalog root is a configurable directory path. Schemas map to subdirectories; tables map to further subdirectories within those. A relation `database.schema.table` is stored at `{catalog_path}/{schema}/{table}/`.

**Why:** Eliminates any server dependency. The adapter is self-contained and works offline. Iteration is fast because there is no network or process boundary.

**Trade-offs:** Not suitable for multi-user or remote scenarios without swapping the storage layer.

---

## ADR 2: Delta Lake as the storage format

**Decision:** Every relation (seed or model output) is written as a Delta table (`df.write_delta(path, mode="overwrite")`). The `_delta_log/` directory provides ACID versioning and schema evolution.

**Why:** Delta Lake is the only open table format with a pure-Python writer (`deltalake` / `delta-rs`) that requires no JVM. It gives transactional overwrites, schema enforcement, and time-travel for free. Alternatives (Parquet files, Iceberg) either lack Python-native writers or require more infrastructure.

---

## ADR 3: Polars SQLContext for model execution

**Decision:** SQL models are executed by registering all catalog tables into a `polars.SQLContext` as `LazyFrame`s, then running the model's compiled SELECT via `ctx.execute(sql)`. The result is collected and written as a new Delta table.

**Why:** Polars has a built-in SQL interface that accepts standard SQL (the same dialect dbt compiles to). This avoids adding a separate query engine (e.g. DuckDB) as a dependency. Polars is already a required dependency for DataFrame processing.

**Trade-offs:**
- Polars SQL does not support 3-part qualified names (`"db"."schema"."table"`), so dbt's compiled SQL is pre-processed with a regex to strip qualifiers before execution (see ADR 5).
- All catalog tables are registered in the context on every model run; there is no incremental registration. This is acceptable for the current local/small-data use case.

---

## ADR 4: LazyFrame throughout the read path

**Decision:** `scan_delta_table` returns a `pl.LazyFrame` (via `pl.scan_parquet`). The SQL context registers LazyFrames. `ctx.execute()` also returns a LazyFrame. `.collect()` is called only once, immediately before `write_delta`.

**Why:** Polars' lazy evaluation allows the query optimizer to push down predicates and projections across the entire pipeline — from catalog scan through SQL execution to the final write. Materializing early would force a full scan of every registered table even when only a subset of columns or rows is needed.

---

## ADR 5: Strip relation qualifiers before Polars SQL execution

**Decision:** Before passing compiled SQL to `SQLContext.execute`, a regex strips all `"database".` and `"schema".` prefixes, leaving bare quoted table names: `"db"."schema"."table"` → `"table"`.

**Why:** Polars SQLContext registers tables by a single name string with no concept of schemas or databases.

**Trade-offs:** Tables with the same name in different schemas collide (last registration wins). A future fix would register tables with a compound key and rewrite SQL references accordingly.

---

## ADR 6: Parse the delta log directly to obtain active file paths

**Decision:** `scan_delta_table` reads the `_delta_log/*.json` files directly, replaying `add` and `remove` actions in version order to build the set of currently-active relative file paths. These relative paths (e.g. `part-00001-xxx.parquet`) are joined with `table_dir` to produce absolute paths passed to `pl.scan_parquet`.

**Why:** `pl.scan_delta` and `pl.read_delta` both route through delta-rs's `object_store` crate, which URL-encodes all absolute paths. On Linux, a path containing a space (e.g. `.../platform chapter/...`) becomes `.../platform%20chapter/...`, which the OS rejects with `ENOENT`. Symlinks and `os.chdir` workarounds fail because delta-rs resolves symlinks to the real absolute path before encoding.

The relative file paths stored in the delta log never contain spaces. Joining them with `table_dir` produces an absolute path with a real space, which `pl.scan_parquet` opens correctly because it does not go through delta-rs's storage layer.

---

## ADR 7: Macro overrides for seeds and models

**Decision:** The standard dbt seed and model materialization macros are overridden with polars-specific variants:

| Macro | Override | Reason |
|---|---|---|
| `load_csv_rows` | `polars__load_csv_rows` | The default calls `adapter.add_query()` (SQL INSERT), which was removed in dbt-adapters 1.x. Seeds call `adapter.load_dataframe()` (Python) instead. |
| `create_csv_table` | `polars__create_csv_table` | No-op. Delta tables are schema-on-write; no DDL is needed. |
| `reset_csv_table` | `polars__reset_csv_table` | No-op. `write_delta(mode="overwrite")` handles truncation. |
| `create_view_as` | `polars__create_view_as` | Views don't exist in a filesystem catalog. Both views and tables are materialized as Delta tables via `adapter.execute_select_as_delta()`. |
| `create_table_as` | `polars__create_table_as` | Same as above. |

---

## ADR 8: Connection manager `execute()` is a conditional no-op

**Decision:** `PolarsConnectionManager.execute()` returns an empty `agate.Table` when `fetch=False` (DDL paths, materialization hooks). When `fetch=True` (e.g. `dbt show`), it builds a `SQLContext` from the catalog and returns the query result as an `agate.Table`.

**Why:** dbt's materialization flow sends DDL-like SQL strings through `execute()` that the adapter has no use for — these are handled upstream by macro overrides. `dbt show` genuinely needs results back, so that path is implemented. Keeping the two cases separate avoids building a SQL context on every statement call.

---

## ADR 9: uv for dependency management

**Decision:** The project uses `uv` with a `pyproject.toml` (replacing `setup.py` + `dev-requirements.txt` + `tox.ini`). Python is pinned to 3.12 via `.python-version`.

**Why:** `uv` is significantly faster than pip for dependency resolution and installation. `pyproject.toml` with `[dependency-groups]` consolidates package metadata and dev dependencies in one file. Python 3.14 (the system default at the time of setup) is incompatible with `mashumaro` / dbt-core, so an explicit pin is required.

---

## ADR 10: Databricks Unity Catalog as a second target

**Decision:** A second adapter target type (`target_type: unity_catalog`) connects to Databricks Unity Catalog. The credentials, host, and catalog name are configured in the dbt profile. The local and Unity Catalog targets share the same Polars SQL execution engine (ADR 3) and Delta format (ADR 2); only the catalog management and storage I/O layers differ.

**Why:** Teams want a local target for development (fast, free, offline) and a Unity Catalog target for production (governed, shared, auditable). Keeping the execution engine identical means model SQL runs the same way in both environments.

**Profile shape:**
```yaml
uc_production:
  target: prod
  outputs:
    prod:
      type: polars
      target_type: unity_catalog
      host: "https://adb-<workspace-id>.azuredatabricks.net"
      catalog: my_catalog
      database: my_catalog      # mirrors catalog; Unity Catalog uses catalog.schema.table
      schema: my_schema
      client_id: "{{ env_var('DBT_SP_CLIENT_ID') }}"
      token: "{{ env_var('DBT_SP_TOKEN') }}"   # PAT for the service principal
```

---

## ADR 11: Authentication via Databricks OAuth M2M (workspace OIDC endpoint)

**Decision:** The adapter authenticates using Databricks OAuth Machine-to-Machine (M2M) with a Databricks service principal. On connection open, it exchanges the service principal's `client_id` and `client_secret` (OAuth secret) for a short-lived access token by calling the workspace's own OIDC endpoint:

```
POST https://{host}/oidc/v1/token
grant_type=client_credentials&client_id=…&client_secret=…&scope=all-apis
```

The returned `access_token` is used as the `Bearer` token in all subsequent Databricks API calls.

**Why:** Databricks service principals authenticate via the workspace-local OIDC endpoint, not Azure AD. This keeps the adapter independent of any Azure tenant — only the workspace host, client ID, and OAuth secret are needed. Access tokens are short-lived (typically 1 hour) and automatically expire, which is more secure than long-lived PATs.

**Profile fields:**
```yaml
client_id: "<service-principal-application-id>"
client_secret: "<oauth-secret>"
```

**Trade-offs:** The access token must be refreshed before expiry. The current implementation fetches a fresh token on every `open()` call (i.e. every dbt invocation), which is correct for typical `dbt run` durations. Very long-running jobs spanning more than one hour would need token refresh mid-run.

---

## ADR 12: All catalog operations via the Unity Catalog REST API — no SQL compute

**Decision:** Schema and table lifecycle operations (create, list, drop, rename) are performed exclusively through the Unity Catalog REST API (`/api/2.1/unity-catalog/…`). No SQL warehouse, Spark cluster, or other compute resource is started.

**Why:** SQL compute is expensive and slow to start. The Unity Catalog REST API provides full catalog CRUD without any compute, making the adapter cost-free for catalog operations. This also removes the need to manage cluster/warehouse credentials.

**Key API calls:**

| Operation | Endpoint |
|---|---|
| List schemas | `GET /api/2.1/unity-catalog/schemas?catalog_name={catalog}` |
| Create schema | `POST /api/2.1/unity-catalog/schemas` |
| Drop schema | `DELETE /api/2.1/unity-catalog/schemas/{catalog}.{schema}` |
| Get table metadata | `GET /api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}` |
| List tables | `GET /api/2.1/unity-catalog/tables?catalog_name={catalog}&schema_name={schema}` |
| Register table | `POST /api/2.1/unity-catalog/tables` |
| Drop table | `DELETE /api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}` |

When registering a new table (after a seed or model run), the adapter POSTs the table's Delta format metadata and its ADLS storage location to the API. Unity Catalog records the table in its metastore without touching the data.

---

## ADR 13: Temporary storage credentials via Unity Catalog credential vending

**Decision:** Before reading or writing any Delta table on Azure storage, the adapter requests short-lived Azure credentials from the Unity Catalog credential vending API (`POST /api/2.1/unity-catalog/temporary-table-credentials`). These credentials are scoped to the specific table and operation (`READ` or `READWRITE`) and are passed as `storage_options` to `pl.scan_parquet` / `polars.write_delta`.

**Why:** The adapter's service principal has no direct ADLS permissions. Unity Catalog acts as a credential broker: it holds the storage credentials centrally, enforces column- and row-level access policies, and issues short-lived delegated credentials per request. This is the correct architecture for Unity Catalog external access — bypassing credential vending by granting the service principal direct ADLS permissions would circumvent Unity Catalog's governance.

**Credential vending flow:**

```
1. GET table metadata → storage_location (abfss://...), table_id
2. POST /api/2.1/unity-catalog/temporary-table-credentials
       { "table_id": "...", "operation": "READ" | "READWRITE" }
   → { "azure_user_delegation_sas": { "sas_token": "...", "url": "..." } }
         OR
   → { "azure_service_principal": { "directory_id": "...",
                                     "application_id": "...",
                                     "client_secret": "..." } }
3. Pass credentials as storage_options to Polars / deltalake
```

**Credential lifetime:** Vended credentials are typically valid for one hour. For a single model run this is sufficient. Long-running jobs should re-vend before the credentials expire.

---

## ADR 14: Delta tables on Azure Data Lake Storage Gen2

**Decision:** Data for Unity Catalog tables is stored as Delta tables on ADLS Gen2. The `storage_location` for each table (`abfss://container@account.dfs.core.windows.net/path`) is obtained from the Unity Catalog table metadata API. Reads use `pl.scan_parquet` with the vended credentials and the file list from the delta log (ADR 6). Writes use `polars.write_delta` (or `deltalake.write_deltalake`) with the vended credentials passed as `storage_options`.

**Why:** ADLS Gen2 is the native storage for Databricks on Azure. Using Delta format keeps the on-disk representation consistent between the local and Unity Catalog targets, meaning the same Polars read/write code path is used in both cases — only the path and `storage_options` differ.

**Storage options shape for ADLS (SAS token):**
```python
storage_options = {
    "azure_storage_account_name": "myaccount",
    "azure_storage_sas_token": "<vended_sas_token>",
}
```

**Storage options shape for ADLS (service principal):**
```python
storage_options = {
    "azure_storage_account_name": "myaccount",
    "azure_client_id": "...",
    "azure_client_secret": "...",
    "azure_tenant_id": "...",
}
```

**Trade-offs:** The delta log parsing approach (ADR 6) still applies for reading: extract the active relative file paths from `_delta_log/*.json`, prepend the ADLS `storage_location`, and pass to `pl.scan_parquet` with `storage_options`. This avoids going through delta-rs's object_store path, which has separate issues with Azure URI handling.
