import os


def azure_blob_catalog(name: str, prefix: str, schema: str) -> dict:
    return {
        "type": "azure",
        "name": name,
        "account_name": os.environ.get("AZURE_STORAGE_ACCOUNT", ""),
        "container": os.environ.get("AZURE_STORAGE_CONTAINER", ""),
        "prefix": prefix,
        "credentials": {
            "exclude_managed_identity_credential": True,
        },
        "schema": schema,
    }


def azure_target(schema: str) -> dict:
    prefix = os.environ.get("AZURE_STORAGE_PREFIX", "dbt-test")
    return {
        "type": "polars",
        "catalogs": [
            azure_blob_catalog("local", f"{prefix}/local", schema),
            azure_blob_catalog("local2", f"{prefix}/local2", schema),
        ],
    }


def local_catalog(name: str, root: str, schema: str) -> dict:
    return {"type": "local", "name": name, "root": root, "schema": schema}


def default_target(schema: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [
            local_catalog(name="local", root="test_root/", schema=schema),
            local_catalog(name="local2", root="test_root2/", schema=schema),
        ],
    }


def iceberg_catalog(name: str, uri: str, warehouse: str, schema: str) -> dict:
    return {
        "type": "iceberg",
        "name": name,
        "uri": uri,
        "warehouse": warehouse,
        "schema": schema,
    }


def iceberg_target(base_path: str, schema: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [
            iceberg_catalog(
                "local",
                f"sqlite:///{base_path}/local.db",
                f"file://{base_path}/wh_local",
                schema,
            ),
            iceberg_catalog(
                "local2",
                f"sqlite:///{base_path}/local2.db",
                f"file://{base_path}/wh_local2",
                schema,
            ),
        ],
    }


def s3_catalog(name: str, prefix: str, schema: str) -> dict:
    return {
        "type": "s3",
        "name": name,
        "bucket": os.environ.get("AWS_S3_BUCKET", ""),
        "prefix": prefix,
        "schema": schema,
    }


def s3_target(schema: str) -> dict:
    prefix = os.environ.get("AWS_S3_PREFIX", "dbt-test")
    return {
        "type": "polars",
        "catalogs": [
            s3_catalog("local", f"{prefix}/local", schema),
            s3_catalog("local2", f"{prefix}/local2", schema),
        ],
    }


def get_databricks_token() -> str:
    import requests
    from azure.identity import DefaultAzureCredential

    DATABRICKS_WORKSPACE_URL = os.environ.get("DATABRICKS_WORKSPACE_URL")

    credential = DefaultAzureCredential(exclude_managed_identity_credential=True)
    azure_token = credential.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default")
    response = requests.post(
        f"{DATABRICKS_WORKSPACE_URL}/api/2.0/token/create",
        headers={"Authorization": f"Bearer {azure_token.token}"},
        json={"lifetime_seconds": 3600, "comment": "pyiceberg session"},
    )
    response.raise_for_status()
    return response.json()["token_value"]


def iceberg_databricks_catalog(token: str, schema: str) -> dict:
    DATABRICKS_UC_CATALOG = os.environ.get("DATABRICKS_UC_CATALOG")
    DATABRICKS_WORKSPACE_URL = os.environ.get("DATABRICKS_WORKSPACE_URL")

    return {
        "type": "iceberg",
        "pyiceberg_type": "rest",
        "name": DATABRICKS_UC_CATALOG,
        "uri": f"{DATABRICKS_WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest/",
        "token": token,
        "warehouse": DATABRICKS_UC_CATALOG,
        "schema": schema,
    }


def iceberg_databricks_target(token: str, schema: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [iceberg_databricks_catalog(token, schema)],
    }


def databricks_catalog(token: str, name: str, schema: str) -> dict:
    return {
        "type": "databricks",
        "name": name,
        "catalog_name": os.environ.get("DATABRICKS_UC_CATALOG", ""),
        "host": os.environ.get("DATABRICKS_WORKSPACE_URL"),
        "token": token,
        "persist_docs_http_path": os.environ.get("DATABRICKS_PERSIST_DOCS_HTTP_PATH"),
        "schema": schema,
    }


def databricks_target(token: str, schema: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [databricks_catalog(token, "local", schema)],
    }


def get_databricks_pyiceberg_catalog(token: str):
    from pyiceberg.catalog import load_catalog

    DATABRICKS_UC_CATALOG = os.environ.get("DATABRICKS_UC_CATALOG")
    DATABRICKS_WORKSPACE_URL = os.environ.get("DATABRICKS_WORKSPACE_URL")

    return load_catalog(
        DATABRICKS_UC_CATALOG,
        **{
            "type": "rest",
            "uri": f"{DATABRICKS_WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest/",
            "token": token,
            "warehouse": DATABRICKS_UC_CATALOG,
        },
    )
