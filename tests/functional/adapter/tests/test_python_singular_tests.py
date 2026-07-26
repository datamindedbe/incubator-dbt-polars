import pytest
from dbt.tests.adapter.basic import files
from dbt.tests.util import check_result_nodes_by_name, run_dbt

python_test_passing = """
def test(dbt, pl):
    df = dbt.ref("base")
    return df.filter(pl.col("id") > 1000)
"""

python_test_failing = """
def test(dbt, pl):
    return dbt.ref("base")
"""

base_model_three_rows = "{{ config(materialized='table') }}\n" + (
    "select 1 as id, 10 as amount\n"
    "union all select 2 as id, 20 as amount\n"
    "union all select 3 as id, 30 as amount"
)


class TestPythonSingularTests:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_singular_tests"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": "{{ config(materialized='table') }}\nselect 1 as id"}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_passing.py": python_test_passing,
            "python_failing.py": python_test_failing,
            "sql_passing.sql": files.test_passing_sql,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_python_singular_tests(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 3
        check_result_nodes_by_name(
            results, ["python_passing", "python_failing", "sql_passing"]
        )

        for result in results:
            if result.node.name in ("python_passing", "sql_passing"):
                assert result.status == "pass"
            elif result.node.name == "python_failing":
                assert result.status == "fail"


class TestPythonSingularTestStoreFailures:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_test_store_failures"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": base_model_three_rows}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_store_failures.py": """
def test(dbt, pl):
    dbt.config(store_failures=True)
    return dbt.ref("base")
"""
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)
            failures_relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema + "_dbt_test__audit",
            )
            project.adapter.drop_schema(failures_relation)

    def test_store_failures_writes_table(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"

        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema + "_dbt_test__audit",
                identifier="python_store_failures",
            )
            catalog = project.adapter.get_storage_catalog(project.database)
            assert catalog.table_exists(relation)
            df = catalog.get_relation(relation).collect()
            assert df.height == 3


class TestPythonSingularTestStoreFailuresAsViewRejected:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_test_store_failures_view_rejected"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": base_model_three_rows}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_store_failures_view.py": """
def test(dbt, pl):
    dbt.config(store_failures=True, store_failures_as="view")
    return dbt.ref("base")
"""
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_store_failures_as_view_raises_compiler_error(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "views are not supported by this adapter" in (results[0].message or "")


class TestPythonSingularTestLimit:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_test_limit"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": base_model_three_rows}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_limit.py": """
def test(dbt, pl):
    dbt.config(limit=1)
    return dbt.ref("base")
"""
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_limit_caps_failure_count(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"
        assert results[0].failures == 1


class TestPythonSingularTestFailCalc:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_test_fail_calc"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": base_model_three_rows}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_fail_calc.py": """
def test(dbt, pl):
    dbt.config(fail_calc="sum(amount)")
    return dbt.ref("base")
"""
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_fail_calc_overrides_row_count(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "fail"
        # 10 + 20 + 30, not the row count (3)
        assert results[0].failures == 60


class TestPythonSingularTestWarnErrorIf:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_test_warn_error_if"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": base_model_three_rows}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_thresholds.py": """
def test(dbt, pl):
    dbt.config(warn_if=">100", error_if=">1000")
    return dbt.ref("base")
"""
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_thresholds_are_respected(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"])
        # 3 failing rows returned, but neither ">100" nor ">1000" is met -- with the
        # old hardcoded `failures > 0` semantics this would always fail regardless.
        assert len(results) == 1
        assert results[0].status == "pass"


class TestPythonSingularTestSqlHeaderRejected:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_test_sql_header_rejected"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": "{{ config(materialized='table') }}\nselect 1 as id"}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_sql_header.py": """
def test(dbt, pl):
    dbt.config(sql_header="-- not applicable to python")
    return dbt.ref("base")
"""
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_sql_header_raises_compiler_error(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "sql_header is not supported for Python singular tests" in (
            results[0].message or ""
        )


class TestPythonSingularTestBooleanReturn:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "python_test_boolean_return"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": base_model_three_rows}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_bool_passing.py": """
def test(dbt, pl):
    return True
""",
            "python_bool_failing.py": """
def test(dbt, pl):
    return False
""",
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_boolean_return_pass_fail(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 2
        check_result_nodes_by_name(
            results, ["python_bool_passing", "python_bool_failing"]
        )
        for result in results:
            if result.node.name == "python_bool_passing":
                assert result.status == "pass"
                assert result.failures == 0
            elif result.node.name == "python_bool_failing":
                assert result.status == "fail"
                assert result.failures == 1


class TestPythonSingularTestBooleanReturnIgnoresRowConfigs:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Simulates project-level config (e.g. `tests: +warn_if: ...` in
        # dbt_project.yml) that's tuned for row-count tests but shouldn't affect a
        # boolean-returning test's outcome at all.
        return {
            "name": "python_test_boolean_ignores_row_configs",
            "tests": {
                "+warn_if": "> 100",
                "+error_if": "> 1000",
                "+limit": 1,
                "+fail_calc": "sum(amount)",
                "+store_failures": True,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"base.sql": base_model_three_rows}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "python_bool_failing.py": """
def test(dbt, pl):
    return False
"""
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)
            failures_relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema + "_dbt_test__audit",
            )
            project.adapter.drop_schema(failures_relation)

    def test_project_level_row_configs_do_not_apply(self, project):
        run_dbt(["run"])
        results = run_dbt(["test"], expect_pass=False)
        assert len(results) == 1
        # Would incorrectly "pass" if warn_if/error_if thresholds (tuned for a
        # row count) were applied to the boolean-derived failures=1 value.
        assert results[0].status == "fail"
        assert results[0].failures == 1
