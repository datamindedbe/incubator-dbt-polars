import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import get_connection, relation_from_name, run_dbt
from tests.conftest import PolarsTestMixin

MODEL_SELECT_ONE = "select 1 as a"


class TestSelectOneModel(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"select_one.sql": MODEL_SELECT_ONE}

    def test_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestDuplicateNameInContext(PolarsTestMixin):
    model_in_schema_a = """
        {{ config(schema='schema_a', alias='shared_name') }}
        select 1 as id
    """

    model_in_schema_b = """
        {{ config(schema='schema_b', alias='shared_name') }}
        select 2 as id
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_in_schema_a.sql": self.model_in_schema_a,
            "model_in_schema_b.sql": self.model_in_schema_b,
        }

    def test_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert all(r.status == "success" for r in results)


class TestCteNameCollision(PolarsTestMixin):
    upstream = """
        {{ config(materialized='table') }}
        SELECT 1 AS id, 'real' AS source
    """

    # Uses a CTE named 'upstream' — same identifier as the real table above.
    # The CTE should take precedence; the real table must not shadow it.
    cte_collision = """
        {{ config(materialized='table') }}
        WITH upstream AS (
            SELECT 1 AS id, 'cte' AS source
        )
        SELECT * FROM upstream
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": self.upstream,
            "cte_collision.sql": self.cte_collision,
        }

    def test_cte_takes_precedence_over_same_named_table(self, project):
        run_dbt(["run", "--select", "upstream"])
        run_dbt(["run", "--select", "cte_collision"])

        with get_connection(project.adapter):
            rel = relation_from_name(project.adapter, "cte_collision")
            df = project.adapter.get_catalog(rel.database).get_relation(rel).collect()

        assert df["source"].to_list() == ["cte"], (
            f"Expected CTE data ('cte') but got {df['source'].to_list()!r}. "
            "The real 'upstream' table shadowed the CTE."
        )


class TestCrossSchemaIdentifierCollision(PolarsTestMixin):
    orders_a = """
        {{ config(schema='schema_a', alias='orders') }}
        SELECT 'a' AS source
    """

    orders_b = """
        {{ config(schema='schema_b', alias='orders') }}
        SELECT 'b' AS source
    """

    combined = """
        SELECT
            a.source AS a_source,
            b.source AS b_source
        FROM {{ ref('orders_a') }} AS a
        CROSS JOIN {{ ref('orders_b') }} AS b
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders_a.sql": self.orders_a,
            "orders_b.sql": self.orders_b,
            "combined.sql": self.combined,
        }

    def test_both_schemas_are_distinct_in_context(self, project):
        run_dbt(["run"])

        with get_connection(project.adapter):
            rel = relation_from_name(project.adapter, "combined")
            df = project.adapter.get_catalog(rel.database).get_relation(rel).collect()

        assert df["a_source"].to_list() == ["a"], (
            f"Expected a_source='a' but got {df['a_source'].to_list()!r}. "
            "'orders' from schema_b overwrote schema_a in the SQL context."
        )
        assert df["b_source"].to_list() == ["b"], (
            f"Expected b_source='b' but got {df['b_source'].to_list()!r}. "
            "'orders' from schema_a overwrote schema_b in the SQL context."
        )


class TestCrossSchemaCollisionWithColumnQualifier(PolarsTestMixin):
    orders_a = """
        {{ config(schema='schema_a', alias='orders') }}
        SELECT 'a' AS source
    """
    orders_b = """
        {{ config(schema='schema_b', alias='orders') }}
        SELECT 'b' AS source
    """
    # Uses bare 'orders' as a column qualifier without an alias on either table.
    # The adapter cannot safely determine which 'orders' the qualifier refers to.
    bad_join = """
        SELECT orders.source
        FROM {{ ref('orders_a') }}
        CROSS JOIN {{ ref('orders_b') }}
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders_a.sql": self.orders_a,
            "orders_b.sql": self.orders_b,
            "bad_join.sql": self.bad_join,
        }

    def test_raises_on_ambiguous_column_qualifier(self, project):
        run_dbt(["run", "--select", "orders_a orders_b"])
        results = run_dbt(["run", "--select", "bad_join"], expect_pass=False)
        assert results[0].status == RunStatus.Error
        assert "exists in multiple schemas" in results[0].message


class TestCTESameNameAsModel(PolarsTestMixin):
    first = """
        select 1 as id
    """

    second = """
        select 2 as id
    """

    combined = """
    WITH combined AS (
        select * from {{ ref('first') }}
    )
    select * from {{ ref('second') }}
    where id in (select id from combined)
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first.sql": self.first,
            "second.sql": self.second,
            "combined.sql": self.combined,
        }

    def test_run(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        assert all(r.status == "success" for r in results)
