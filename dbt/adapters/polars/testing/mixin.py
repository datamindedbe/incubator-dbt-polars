import logging
import os
import random
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dbt.adapters.polars.catalogs import create_catalog

logger = logging.getLogger(__name__)

FALLBACK_RUN_ID = uuid.uuid4().hex
STALE_SCHEMA_AGE = timedelta(hours=24)
SCHEMA_TIMESTAMP = re.compile(r"^test(?:_[0-9a-f]{8}_[a-z0-9]+_)?(\d{16})\d{4}_")


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def worker_schema_prefix() -> str:
    """`test_<run>_<worker>_`: unique per test run and pytest-xdist worker."""
    run_id = os.environ.get("PYTEST_XDIST_TESTRUNUID", FALLBACK_RUN_ID)[:8]
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    return f"test_{run_id}_{worker_id}_"


def new_schema_prefix() -> str:
    runtime = utc_now() - datetime(1970, 1, 1)
    timestamp = int(runtime.total_seconds() * 1e6)
    return f"{worker_schema_prefix()}{timestamp}{random.randint(0, 9999):04}"


def is_removable_test_schema(schema: str) -> bool:
    """True for schemas created by this worker, and for test schemas older than
    STALE_SCHEMA_AGE (left behind by cancelled or crashed runs)."""
    if schema.startswith(worker_schema_prefix()):
        return True
    match = SCHEMA_TIMESTAMP.match(schema)
    if match is None:
        return False
    created = datetime(1970, 1, 1) + timedelta(microseconds=int(match.group(1)))
    return utc_now() - created > STALE_SCHEMA_AGE


def polars_profile_data(
    unique_schema: str, dbt_profile_target: dict, profiles_config_update: dict | None
) -> dict:
    """Inject unique_schema into every catalog entry, since dbt-polars requires
    each entry to carry its own schema when `catalogs` is set explicitly."""
    profile: dict[str, Any] = {
        "test": {
            "outputs": {"default": dbt_profile_target},
            "target": "default",
        },
    }
    if profiles_config_update:
        profile.update(profiles_config_update)

    for output in profile["test"]["outputs"].values():
        if "catalogs" in output:
            output["catalogs"] = [
                {**catalog, "schema": catalog.get("schema") or unique_schema}
                for catalog in output["catalogs"]
            ]

    return profile


class PolarsProfileMixin:
    """Profile, schema and cleanup fixtures for the Polars adapter.

    Defined on the class so they take precedence over the plugin's fixtures.
    """

    @pytest.fixture(scope="class")
    def prefix(self) -> str:
        return new_schema_prefix()

    @pytest.fixture(scope="class")
    def unique_schema(self, request, prefix) -> str:
        test_file = request.module.__name__.split(".")[-1]
        return f"{prefix}_{test_file}"

    @pytest.fixture(scope="class")
    def project_root(self, tmpdir_factory):
        return tmpdir_factory.mktemp("project")

    @pytest.fixture(scope="class")
    def dbt_profile_data(
        self, unique_schema, dbt_profile_target, profiles_config_update
    ):
        return polars_profile_data(
            unique_schema, dbt_profile_target, profiles_config_update
        )

    @pytest.fixture(scope="class", autouse=True)
    def cleanup_all_catalog_schemas(self, project):
        def drop_all():
            credentials = project.adapter.config.credentials
            for catalog_name, config in credentials.catalog_configs.items():
                catalog = create_catalog(config, project.adapter.config.project_root)
                relation = project.adapter.Relation.create(
                    database=catalog_name,
                    schema=config.schema,
                )
                try:
                    catalog.drop_schema(relation)
                except Exception as e:
                    logger.warning(
                        "Failed to drop schema %s/%s during cleanup: %s",
                        catalog_name,
                        config.schema,
                        e,
                    )
            project.created_schemas = []

        project.drop_test_schema = drop_all
        yield


class PolarsTestMixin(PolarsProfileMixin):
    """Adds the Polars project defaults (tables instead of views)."""

    @pytest.fixture(scope="class")
    def project_config_update(self, polars_project_config):
        return polars_project_config

    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
        )
        project.adapter.drop_schema(relation)
