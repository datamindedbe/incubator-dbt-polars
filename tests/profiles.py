import json
import os
import urllib.request


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
            azure_blob_catalog("primary", f"{prefix}/primary", schema),
            azure_blob_catalog("secondary", f"{prefix}/secondary", schema),
        ],
    }


def local_catalog(name: str, root: str, schema: str) -> dict:
    return {"type": "local", "name": name, "root": root, "schema": schema}


def default_target(schema: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [
            local_catalog(name="primary", root="test_root/", schema=schema),
            local_catalog(name="secondary", root="test_root2/", schema=schema),
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
                "primary",
                f"sqlite:///{base_path}/primary.db",
                f"file://{base_path}/wh_primary",
                schema,
            ),
            iceberg_catalog(
                "secondary",
                f"sqlite:///{base_path}/secondary.db",
                f"file://{base_path}/wh_secondary",
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
            s3_catalog("primary", f"{prefix}/primary", schema),
            s3_catalog("secondary", f"{prefix}/secondary", schema),
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
        "catalogs": [databricks_catalog(token, "primary", schema)],
    }


def fabric_catalog(name: str, schema: str) -> dict:
    return {
        "type": "fabric",
        "name": name,
        "workspace": os.environ.get("FABRIC_WORKSPACE", ""),
        "lakehouse": os.environ.get("FABRIC_LAKEHOUSE", ""),
        "exclude_managed_identity_credential": True,
        "schema": schema,
    }


def fabric_target(schema: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [fabric_catalog("primary", schema)],
    }


def github_oidc_token(audience: str) -> str:
    # GitHub ID tokens expire after about 10 minutes, so each profile requests a new one
    request_token = os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
    request = urllib.request.Request(
        f"{os.environ['ACTIONS_ID_TOKEN_REQUEST_URL']}&audience={audience}",
        headers={"Authorization": f"Bearer {request_token}"},
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)["value"]


def snowflake_credentials() -> dict:
    if "ACTIONS_ID_TOKEN_REQUEST_URL" in os.environ:
        return {
            "credential": github_oidc_token("snowflakecomputing.com"),
            "header.X-Snowflake-Workload-Identity-Provider": "OIDC",
        }
    return {"credential": os.environ.get("SNOWFLAKE_PAT", "")}


def snowflake_catalog(name: str, schema: str) -> dict:
    return {
        "type": "snowflake",
        "name": name,
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        "database_name": os.environ.get("SNOWFLAKE_DATABASE", ""),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        **snowflake_credentials(),
        "schema": schema,
    }


def snowflake_target(schema: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [snowflake_catalog("primary", schema)],
    }
