def local_catalog(name: str, root: str) -> dict:
    return {
        "type": "local",
        "name": name,
        "root": root,
    }


def default_target() -> dict:
    return {
        "type": "polars",
        "catalogs": [
            local_catalog(name="local", root="test_root/"),
            local_catalog(name="local2", root="test_root2/"),
        ],
    }


def iceberg_catalog(name: str, uri: str, warehouse: str) -> dict:
    return {
        "type": "iceberg",
        "name": name,
        "uri": uri,
        "warehouse": warehouse,
    }


def iceberg_target(base_path: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [
            iceberg_catalog(
                "local",
                f"sqlite:///{base_path}/local.db",
                f"file://{base_path}/wh_local",
            ),
            iceberg_catalog(
                "local2",
                f"sqlite:///{base_path}/local2.db",
                f"file://{base_path}/wh_local2",
            ),
        ],
    }


_DATABRICKS_WORKSPACE_URL = "https://adb-7405609020332160.0.azuredatabricks.net"
_DATABRICKS_UC_CATALOG = "managed"
_DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


def get_databricks_token() -> str:
    import requests
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential(exclude_managed_identity_credential=True)
    azure_token = credential.get_token(f"{_DATABRICKS_RESOURCE_ID}/.default")
    response = requests.post(
        f"{_DATABRICKS_WORKSPACE_URL}/api/2.0/token/create",
        headers={"Authorization": f"Bearer {azure_token.token}"},
        json={"lifetime_seconds": 3600, "comment": "pyiceberg session"},
    )
    response.raise_for_status()
    return response.json()["token_value"]


def iceberg_databricks_catalog(token: str) -> dict:
    return {
        "type": "iceberg",
        "pyiceberg_type": "rest",
        "name": _DATABRICKS_UC_CATALOG,
        "uri": f"{_DATABRICKS_WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest/",
        "token": token,
        "warehouse": _DATABRICKS_UC_CATALOG,
    }


def iceberg_databricks_target(token: str) -> dict:
    return {
        "type": "polars",
        "catalogs": [iceberg_databricks_catalog(token)],
    }


def get_databricks_pyiceberg_catalog(token: str):
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        _DATABRICKS_UC_CATALOG,
        **{
            "type": "rest",
            "uri": f"{_DATABRICKS_WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest/",
            "token": token,
            "warehouse": _DATABRICKS_UC_CATALOG,
        },
    )
