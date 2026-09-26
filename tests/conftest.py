import pytest

from dbt.adapters.polars.testing.mixin import (
    is_removable_test_schema,
    new_schema_prefix,
    polars_profile_data,
)
from tests.config_presets import CONFIG_PRESETS
from tests.profiles import (
    azure_target,
    databricks_target,
    default_target,
    get_databricks_pyiceberg_catalog,
    get_databricks_token,
    iceberg_databricks_target,
    iceberg_target,
    s3_target,
)

pytest_plugins = ["dbt.adapters.polars.testing.plugin"]


def pytest_addoption(parser):
    parser.addoption(
        "--profile",
        dest="profile",
        choices=[
            "local",
            "iceberg",
            "iceberg-databricks",
            "azure",
            "s3",
            "databricks",
        ],
        default="local",
        help=(
            "dbt profile to use for tests (determines the catalog backend). "
            "Mark tests with @pytest.mark.require_profiles(...) to "
            "restrict to specific profiles, or "
            "@pytest.mark.skip_profiles(...) to exclude from specific profiles."
        ),
    )
    parser.addoption(
        "--config",
        dest="config_preset",
        choices=list(CONFIG_PRESETS),
        default="default",
        help=(
            "Named config preset to inject into all models and seeds. "
            f"Available: {list(CONFIG_PRESETS)}. "
            "Mark tests with @pytest.mark.require_configs(...) or "
            "@pytest.mark.skip_configs(...) to filter by preset."
        ),
    )


def pytest_runtest_setup(item):
    profile = item.config.option.profile
    config_preset = item.config.option.config_preset

    require_profiles = item.get_closest_marker("require_profiles")
    if require_profiles and profile not in require_profiles.args:
        pytest.skip(
            f"requires profile in {list(require_profiles.args)!r}, active: {profile!r}"
        )
    skip_profiles = item.get_closest_marker("skip_profiles")
    if skip_profiles and profile in skip_profiles.args:
        pytest.skip(f"skipped for profile {profile!r}")

    require_configs = item.get_closest_marker("require_configs")
    if require_configs and config_preset not in require_configs.args:
        pytest.skip(
            f"requires config in {list(require_configs.args)!r}, "
            + f"active: {config_preset!r}"
        )
    skip_configs = item.get_closest_marker("skip_configs")
    if skip_configs and config_preset in skip_configs.args:
        pytest.skip(f"skipped for config preset {config_preset!r}")


@pytest.fixture(scope="session")
def databricks_token(request):
    if request.config.option.profile not in ("iceberg-databricks", "databricks"):
        return None
    return get_databricks_token()


@pytest.fixture(scope="class")
def dbt_profile_target(request, tmp_path_factory, databricks_token, unique_schema):
    profile = request.config.option.profile
    if profile == "iceberg":
        base = str(tmp_path_factory.mktemp("iceberg"))
        return iceberg_target(base, unique_schema)
    if profile == "iceberg-databricks":
        return iceberg_databricks_target(databricks_token, unique_schema)
    if profile == "azure":
        return azure_target(unique_schema)
    if profile == "s3":
        return s3_target(unique_schema)
    if profile == "databricks":
        return databricks_target(databricks_token, unique_schema)
    return default_target(unique_schema)


@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
    return polars_profile_data(
        unique_schema, dbt_profile_target, profiles_config_update
    )


@pytest.fixture(scope="class")
def prefix() -> str:
    return new_schema_prefix()


@pytest.fixture(scope="class")
def polars_project_config(request) -> dict:
    config: dict = {"models": {"+materialized": "table"}}
    for key, val in CONFIG_PRESETS[request.config.option.config_preset].items():
        if key in config and isinstance(config[key], dict) and isinstance(val, dict):
            config[key] = {**config[key], **val}
        else:
            config[key] = val
    return config


def schema_segment(object_path: str, storage_prefix: str) -> str:
    """Schema name from an object path laid out as <prefix>/<catalog>/<schema>/..."""
    parts = object_path[len(storage_prefix) + 1 :].split("/")
    return parts[1] if len(parts) > 1 else ""


@pytest.fixture(scope="session", autouse=True)
def cleanup_azure_test_schemas(request):
    yield
    if request.config.option.profile != "azure":
        return
    import os

    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT", "")
    container = os.environ.get("AZURE_STORAGE_CONTAINER", "")
    prefix = os.environ.get("AZURE_STORAGE_PREFIX", "dbt-test")
    if not account_name or not container:
        return
    from concurrent.futures import ThreadPoolExecutor

    client = BlobServiceClient(
        f"https://{account_name}.blob.core.windows.net",
        credential=DefaultAzureCredential(exclude_managed_identity_credential=True),
    ).get_container_client(container)

    depth_groups: dict[int, list[str]] = {}
    for blob in client.list_blobs(name_starts_with=f"{prefix}/"):
        if is_removable_test_schema(schema_segment(blob.name, prefix)):
            depth = blob.name.rstrip("/").count("/")
            depth_groups.setdefault(depth, []).append(blob.name)

    def delete_blob(name: str) -> None:
        try:
            client.delete_blob(name)
        except Exception:
            pass

    for depth in sorted(depth_groups.keys(), reverse=True):
        with ThreadPoolExecutor(max_workers=16) as executor:
            list(executor.map(delete_blob, depth_groups[depth]))


@pytest.fixture(scope="session", autouse=True)
def cleanup_s3_test_schemas(request):
    yield
    if request.config.option.profile != "s3":
        return
    import os

    import boto3

    bucket = os.environ.get("AWS_S3_BUCKET", "")
    prefix = os.environ.get("AWS_S3_PREFIX", "dbt-test")
    if not bucket:
        return
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        objects = [
            {"Key": obj["Key"]}
            for obj in page.get("Contents", [])
            if is_removable_test_schema(schema_segment(obj["Key"], prefix))
        ]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})


@pytest.fixture(scope="session", autouse=True)
def cleanup_databricks_test_schemas(request, databricks_token):
    yield
    if request.config.option.profile != "iceberg-databricks":
        return
    catalog = get_databricks_pyiceberg_catalog(databricks_token)
    for namespace in catalog.list_namespaces():
        if not is_removable_test_schema(namespace[0]):
            continue
        try:
            for table_id in catalog.list_tables(namespace):
                catalog.purge_table(table_id)
            catalog.drop_namespace(namespace)
        except Exception:
            pass


@pytest.fixture(scope="session", autouse=True)
def cleanup_databricks_catalog_test_schemas(request, databricks_token):
    """Session-wide sweep of leftover `databricks` catalog test schemas.

    Per-class cleanup (PolarsTestMixin.cleanup_all_catalog_schemas) only knows
    about each catalog's own configured schema, so it misses schemas created
    via a model's own `schema=` config (e.g. sqlmodels/identifier_collisions
    tests using `schema_a`), and test classes that don't mix in
    PolarsTestMixin at all get no per-class cleanup - notably dbt-core's own
    BaseDebug-derived tests, whose `project` fixture teardown call to
    `drop_test_schema()` is wrapped in a try/except that silently swallows the
    failure it hits for the `debug` command (a known dbt-core testing-
    framework limitation, not specific to this adapter). This sweep catches
    both cases, mirroring cleanup_azure_test_schemas/cleanup_s3_test_schemas.
    """
    yield
    if request.config.option.profile != "databricks":
        return
    import os

    from dbt.adapters.polars.catalogs import create_catalog, resolve_catalog_plugin
    from dbt.adapters.polars.relation import PolarsRelation

    catalog_name = os.environ.get("DATABRICKS_UC_CATALOG", "")
    if not catalog_name:
        return
    config = resolve_catalog_plugin("databricks").config_class(
        name="cleanup",
        type="databricks",
        schema="cleanup",
        catalog_name=catalog_name,
        host=os.environ.get("DATABRICKS_WORKSPACE_URL"),
        token=databricks_token,
    )
    catalog = create_catalog(config, "")
    for schema_name in catalog.list_schemas():
        if not is_removable_test_schema(schema_name):
            continue
        relation = PolarsRelation.create(database=catalog_name, schema=schema_name)
        try:
            catalog.drop_schema(relation)
        except Exception:
            pass
