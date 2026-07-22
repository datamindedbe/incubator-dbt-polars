import random
from datetime import datetime, timezone

# Patch check_relations_equal in dbt.tests.util before any test modules are
# imported, so all dbt base test classes automatically use the Polars-native
# comparison (this adapter has no SQL engine to run the EXCEPT-based SQL).
import dbt.tests.util
import pytest

from tests.config_presets import CONFIG_PRESETS
from tests.profiles import (
    default_target,
    get_databricks_pyiceberg_catalog,
    get_databricks_token,
    iceberg_databricks_target,
    iceberg_target,
)
from tests.utils import _resolve_relation, polars_check_relations_equal

dbt.tests.util.check_relations_equal = polars_check_relations_equal
dbt.tests.util.relation_from_name = _resolve_relation

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption(
        "--profile",
        dest="profile",
        choices=["local", "iceberg", "iceberg-databricks"],
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
            f"requires config in {list(require_configs.args)!r}, active: {config_preset!r}"
        )
    skip_configs = item.get_closest_marker("skip_configs")
    if skip_configs and config_preset in skip_configs.args:
        pytest.skip(f"skipped for config preset {config_preset!r}")


@pytest.fixture(scope="session")
def databricks_token(request):
    if request.config.option.profile != "iceberg-databricks":
        return None
    return get_databricks_token()


@pytest.fixture(scope="class")
def dbt_profile_target(request, tmp_path_factory, databricks_token):
    profile = request.config.option.profile
    if profile == "iceberg":
        base = str(tmp_path_factory.mktemp("iceberg"))
        return iceberg_target(base)
    if profile == "iceberg-databricks":
        return iceberg_databricks_target(databricks_token)
    return default_target()


@pytest.fixture(scope="class")
def prefix(request):
    """Unique schema prefix that varies per test class and active profile."""
    profile = request.config.option.profile
    _randint = random.randint(0, 9999)
    _runtime_timedelta = datetime.now(timezone.utc).replace(tzinfo=None) - datetime(
        1970, 1, 1, 0, 0, 0
    )
    _runtime = int(_runtime_timedelta.total_seconds() * 1e6)
    return f"test{_runtime}{_randint:04}_{profile.replace('-', '_')}"


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    test_file = request.module.__name__.split(".")[-1]
    return f"{prefix}_{test_file}"


@pytest.fixture(scope="class")
def project_root(tmpdir_factory):
    return tmpdir_factory.mktemp("project")


@pytest.fixture(scope="session", autouse=True)
def cleanup_databricks_test_schemas(request, databricks_token):
    yield
    if request.config.option.profile != "iceberg-databricks":
        return
    catalog = get_databricks_pyiceberg_catalog(databricks_token)
    for namespace in catalog.list_namespaces():
        ns_name = namespace[0]
        if not ns_name.startswith("test"):
            continue
        try:
            for table_id in catalog.list_tables(namespace):
                catalog.purge_table(table_id)
            catalog.drop_namespace(namespace)
        except Exception:
            pass


class PolarsTestMixin:
    """Overrides SQL-based fixtures from dbt base test classes that don't apply
    to the Polars adapter (which has no SQL engine)."""

    @pytest.fixture(scope="class")
    def project_config_update(self, request):
        config: dict = {"models": {"+materialized": "table"}}
        preset_name = request.config.option.config_preset
        if preset_name:
            for key, val in CONFIG_PRESETS[preset_name].items():
                if (
                    key in config
                    and isinstance(config[key], dict)
                    and isinstance(val, dict)
                ):
                    config[key] = {**config[key], **val}
                else:
                    config[key] = val
        return config

    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
        )
        project.adapter.drop_schema(relation)

    # Uncomment to keep all schemas -- useful for debugging
    # @pytest.fixture(scope="class", autouse=True)
    # def keep_test_schema(self, project):
    #     project.drop_test_schema = lambda: None
    #     yield
