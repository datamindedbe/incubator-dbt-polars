import random
from datetime import datetime, timezone

# Patch check_relations_equal in dbt.tests.util before any test modules are
# imported, so all dbt base test classes automatically use the Polars-native
# comparison (this adapter has no SQL engine to run the EXCEPT-based SQL).
import dbt.tests.util
import pytest

from tests.profiles import default_target
from tests.utils import polars_check_relations_equal

dbt.tests.util.check_relations_equal = polars_check_relations_equal

pytest_plugins = ["dbt.tests.fixtures.project"]

ALL_TABLE_FORMATS = ["delta", "iceberg"]


def pytest_addoption(parser):
    parser.addoption(
        "--table-format",
        action="append",
        dest="table_formats",
        choices=ALL_TABLE_FORMATS,
        default=None,
        help=(
            "Table format to parametrize table_format_specific tests against; "
            "pass multiple times to iterate over several formats. Defaults to "
            "every known table format. Use -m table_format_specific to run "
            "only format-specific tests, or -m 'not table_format_specific' "
            "for format-agnostic tests only."
        ),
    )


def pytest_configure(config):
    if config.option.table_formats is None:
        config.option.table_formats = list(ALL_TABLE_FORMATS)


def pytest_generate_tests(metafunc):
    if "table_format" not in metafunc.fixturenames:
        return
    if metafunc.definition.get_closest_marker("table_format_specific") is None:
        return
    metafunc.parametrize(
        "table_format",
        metafunc.config.option.table_formats,
        indirect=True,
        scope="class",
    )


@pytest.fixture(scope="class")
def table_format(request):
    return getattr(request, "param", request.config.option.table_formats[0])


@pytest.fixture(scope="class")
def prefix(table_format):
    """Extend dbt-tests-adapter's `prefix` to also include the table format."""
    _randint = random.randint(0, 9999)
    _runtime_timedelta = datetime.now(timezone.utc).replace(tzinfo=None) - datetime(
        1970, 1, 1, 0, 0, 0
    )
    _runtime = (
        int(_runtime_timedelta.total_seconds() * 1e6) + _runtime_timedelta.microseconds
    )
    return f"test{_runtime}{_randint:04}_{table_format}"


@pytest.fixture(scope="class")
def project_root(tmpdir_factory, table_format):
    """Override dbt-tests-adapter's `project_root` so it also varies with
    `table_format`; otherwise it's cached once per class and reused across
    table formats, and the second format's project-file setup collides with
    files the first format already wrote."""
    return tmpdir_factory.mktemp("project")


def with_table_format(
    config: dict, table_format: str, resource: str = "models"
) -> dict:
    """Merge a `+table_format` config into the given resource section (default
    "models") of a project_config_update dict."""
    config = dict(config)
    config[resource] = {**config.get(resource, {}), "+table_format": table_format}
    return config


def table_format_fixture(resource: str = "models"):
    """Decorate a `project_config_update`-style method (self -> dict) to turn
    it into a fixture that also merges in the active --table-format."""

    def decorator(fn):
        def wrapper(self, table_format):
            return with_table_format(fn(self), table_format, resource=resource)

        return pytest.fixture(scope="class", name=fn.__name__)(wrapper)

    return decorator


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return default_target()


class PolarsTestMixin:
    """Overrides SQL-based fixtures from dbt base test classes that don't apply
    to the Polars adapter (which has no SQL engine)."""

    @table_format_fixture()
    def project_config_update(self):
        return {"models": {"+materialized": "table"}}

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
