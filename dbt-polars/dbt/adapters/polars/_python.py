import os
import re
from typing import Any, Callable, Dict

import polars as pl

from dbt.adapters.base.impl import PythonJobHelper


def _local_load_fn(catalog_path: str) -> Callable[[str], pl.LazyFrame]:
    """Return a load function that reads a qualified relation from the local catalog."""
    from dbt.adapters.polars._catalog import scan_delta_table

    def load(qualified_name: str) -> pl.LazyFrame:
        parts = re.findall(r'"([^"]+)"', qualified_name)
        if not parts:
            raise ValueError(f"Cannot parse relation name: {qualified_name!r}")
        table_name = parts[-1]
        for schema in os.listdir(catalog_path):
            table_dir = os.path.join(catalog_path, schema, table_name)
            if os.path.isdir(table_dir) and os.path.exists(
                os.path.join(table_dir, "_delta_log")
            ):
                return scan_delta_table(table_dir)
        raise FileNotFoundError(
            f"Table '{table_name}' not found in catalog at {catalog_path}"
        )

    return load


def _databricks_load_fn(
    client: Any, catalog: str, schema: str
) -> Callable[[str], pl.LazyFrame]:
    """Return a load function that reads a qualified relation from Unity Catalog."""

    def load(qualified_name: str) -> pl.LazyFrame:
        parts = re.findall(r'"([^"]+)"', qualified_name)
        if not parts:
            raise ValueError(f"Cannot parse relation name: {qualified_name!r}")
        table_name = parts[-1]
        ref_schema = parts[-2] if len(parts) >= 2 else schema
        ref_catalog = parts[-3] if len(parts) >= 3 else catalog
        info = client.get_table(ref_catalog, ref_schema, table_name)
        creds = client.vend_storage_credentials(info["table_id"], "READ")
        return pl.scan_delta(info["storage_location"], storage_options=creds)

    return load


def _run_model(
    compiled_code: str, load_fn: Callable[[str], pl.LazyFrame]
) -> pl.DataFrame:
    """Execute the compiled Python model and return the result as a DataFrame."""
    namespace: Dict[str, Any] = {}
    exec(compiled_code, namespace)  # noqa: S102

    model_fn = namespace.get("model")
    if not callable(model_fn):
        raise ValueError(
            "Python model must define a callable 'model(dbt, session)'"
        )

    dbt_obj_cls = namespace.get("dbtObj")
    if dbt_obj_cls is None:
        raise ValueError(
            "Compiled Python model is missing the dbtObj class. "
            "Ensure you are using dbt >= 1.3."
        )

    result = model_fn(dbt_obj_cls(load_fn), None)

    if isinstance(result, pl.LazyFrame):
        return result.collect()
    if isinstance(result, pl.DataFrame):
        return result
    raise TypeError(
        f"model() must return a polars.DataFrame or polars.LazyFrame, "
        f"got {type(result).__name__}"
    )


class PolarsPythonJobHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict[str, Any], credential: Any) -> None:
        self._schema = parsed_model["schema"]
        self._identifier = parsed_model["alias"]
        self._credential = credential

    def submit(self, compiled_code: str) -> None:
        if self._credential.host:
            self._submit_databricks(compiled_code)
        else:
            self._submit_local(compiled_code)

    def _submit_local(self, compiled_code: str) -> None:
        catalog_path = self._credential.path
        df = _run_model(compiled_code, _local_load_fn(catalog_path))
        table_path = os.path.join(catalog_path, self._schema, self._identifier)
        os.makedirs(os.path.join(catalog_path, self._schema), exist_ok=True)
        df.write_delta(table_path, mode="overwrite",
                       delta_write_options={"schema_mode": "overwrite"})

    def _submit_databricks(self, compiled_code: str) -> None:
        from dbt.adapters.polars._databricks import DatabricksClient, get_oauth_token

        cred = self._credential
        token = get_oauth_token(cred.host, cred.client_id, cred.client_secret)
        client = DatabricksClient(cred.host, token)
        catalog = cred.catalog or cred.database

        df = _run_model(
            compiled_code,
            _databricks_load_fn(client, catalog, self._schema),
        )
        client.write_and_register(df, catalog, self._schema, self._identifier)
