import os
import re
from unittest import mock

import pytest
from dbt.adapters.polars.impl import PolarsAdapter
from dbt.tests import util
from dbt.tests.adapter.ephemeral import test_ephemeral
from dbt.tests.adapter.ephemeral.test_ephemeral import BaseEphemeral, BaseEphemeralMulti
from tests.conftest import PolarsTestMixin


class TestEphemeralMulti(BaseEphemeralMulti, PolarsTestMixin):
    def test_ephemeral_multi(self, project):
        util.run_dbt(["seed"])
        results = util.run_dbt(["run"])
        assert len(results) == 3

        util.check_relations_equal(project.adapter, ["seed", "dependent"])
        util.check_relations_equal(project.adapter, ["seed", "double_dependent"])
        util.check_relations_equal(project.adapter, ["seed", "super_dependent"])


class TestEphemeralNested(BaseEphemeral, PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_level_two.sql": test_ephemeral.models_n__ephemeral_level_two_sql,
            "root_view.sql": test_ephemeral.models_n__root_view_sql,
            "ephemeral.sql": test_ephemeral.models_n__ephemeral_sql,
            "source_table.sql": test_ephemeral.models_n__source_table_sql,
        }

    def test_ephemeral_nested(self, project):
        results = util.run_dbt(["run"])
        assert len(results) == 2
        assert os.path.exists("./target/run/test/models/root_view.sql")
        with open("./target/run/test/models/root_view.sql") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            "with __dbt__cte__ephemeral_level_two as ("
            f'select * from "{project.database}"."test_test_ephemeral"."source_table"'
            "),  __dbt__cte__ephemeral as ("
            "select * from __dbt__cte__ephemeral_level_two"
            ") select * from __dbt__cte__ephemeral"
        )

        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralModelIsInlinedNotExecuted(PolarsTestMixin):
    """An ephemeral model is never materialized on its own; dbt inlines its
    compiled body as a CTE into whatever model references it. _run_sql
    should therefore only ever be called for the dependent model, never for
    the ephemeral model itself.
    """

    ephemeral_model = """
        {{ config(materialized='ephemeral') }}
        select 1 as id
    """

    dependent_model = """
        select * from {{ ref('ephemeral_model') }}
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_model.sql": self.ephemeral_model,
            "dependent_model.sql": self.dependent_model,
        }

    def test_ephemeral_model_never_reaches_run_sql(self, project):
        executed_sql = []
        original_run_sql = PolarsAdapter._run_sql

        def spy(self, sql):
            executed_sql.append(sql)
            return original_run_sql(self, sql)

        with mock.patch.object(PolarsAdapter, "_run_sql", spy):
            results = util.run_dbt(["run"])

        assert len(results) == 1, (
            "Ephemeral models don't produce their own run result; only "
            f"'dependent_model' should, got {len(results)} results."
        )
        assert len(executed_sql) == 1, (
            f"Expected exactly one call to _run_sql, got {len(executed_sql)}. "
            "An ephemeral model must never be executed directly -- it should "
            "only appear inlined as a CTE in the SQL of models that ref() it."
        )
        assert "__dbt__cte__ephemeral_model" in executed_sql[0], (
            "Expected the ephemeral model's body to be inlined as a CTE in "
            f"the executed SQL, got: {executed_sql[0]!r}"
        )


class TestEphemeralErrorHandling(BaseEphemeral, PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": test_ephemeral.ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": test_ephemeral.ephemeral_errors__base__base_sql,
                "base_copy.sql": test_ephemeral.ephemeral_errors__base__base_copy_sql,
            },
        }

    def test_ephemeral_error_handling(self, project):
        results = util.run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert "Compilation Error" in results[0].message
