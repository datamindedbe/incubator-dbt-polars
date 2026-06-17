import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.util import get_connection, relation_from_name, run_dbt
from tests.conftest import PolarsTestMixin


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


class TestAliasShadowsUnrelatedModelWildcard(PolarsTestMixin):
    """An alias can coincide with the bare identifier of a different,
    unrelated model referenced in the same query. Since the two physical
    identifiers ('orders_stg' and 'orders') don't collide, _parse_and_rewrite
    never renames either one, so the SQLContext ends up with a registered
    frame named 'orders' *and* a query-local alias 'orders' pointing at a
    different frame.
    """

    orders_stg = """
        select 1 as id, 'stg' as note
    """

    orders = """
        select 1 as id, 100 as amount
    """

    combined = """
        select orders.*
        from {{ ref('orders_stg') }} as orders
        join {{ ref('orders') }} as o on orders.id = o.id
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders_stg.sql": self.orders_stg,
            "orders.sql": self.orders,
            "combined.sql": self.combined,
        }

    def test_wildcard_resolves_against_aliased_table(self, project):
        run_dbt(["run"])

        with get_connection(project.adapter):
            rel = relation_from_name(project.adapter, "combined")
            df = project.adapter.get_catalog(rel.database).get_relation(rel).collect()

        assert set(df.columns) == {"id", "note"}, (
            f"Expected columns from 'orders_stg' (id, note) via the 'orders' "
            f"alias, got {df.columns!r}. 'orders.*' likely resolved against "
            "the unrelated model literally named 'orders' instead of the "
            "local alias."
        )
        assert df["note"].to_list() == ["stg"], (
            f"Expected note='stg' (from orders_stg) but got "
            f"{df['note'].to_list()!r}."
        )


class TestAliasShadowsUnrelatedModelColumn(PolarsTestMixin):
    """Same setup as above, but selects a column that only exists on the
    aliased-to table. Polars resolves this against the *registered frame*
    literally named 'orders' rather than the local alias, raising a
    misleading 'column not found' error that blames the wrong table.
    """

    orders_stg = """
        select 1 as id, 'stg' as note
    """

    orders = """
        select 1 as id, 100 as amount
    """

    combined = """
        select orders.id, orders.note, o.amount
        from {{ ref('orders_stg') }} as orders
        join {{ ref('orders') }} as o on orders.id = o.id
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders_stg.sql": self.orders_stg,
            "orders.sql": self.orders,
            "combined.sql": self.combined,
        }

    def test_column_resolves_against_aliased_table(self, project):
        run_dbt(["run"])

        with get_connection(project.adapter):
            rel = relation_from_name(project.adapter, "combined")
            df = project.adapter.get_catalog(rel.database).get_relation(rel).collect()

        assert df["note"].to_list() == ["stg"]
        assert df["amount"].to_list() == [100]


class TestUnrelatedAliasDoesNotBlockCollisionResolution(PolarsTestMixin):
    """The collision check that guards against ambiguous bare-name column
    qualifiers (see TestCrossSchemaCollisionWithColumnQualifier) collects
    column qualifiers globally across the whole query with no scope
    awareness. An alias named 'orders' used inside one CTE (for an unrelated
    model) must not be mistaken for a qualifier on the genuinely colliding,
    but otherwise unambiguous, 'orders' models defined below.
    """

    orders_stg = """
        select 1 as id, 'stg' as note
    """

    orders_a = """
        {{ config(schema='schema_a', alias='orders') }}
        select 'a' as source
    """

    orders_b = """
        {{ config(schema='schema_b', alias='orders') }}
        select 'b' as source
    """

    combined = """
        with stg as (
            select orders.id as id
            from {{ ref('orders_stg') }} as orders
        ),
        a as (
            select source from {{ ref('orders_a') }}
        ),
        b as (
            select source from {{ ref('orders_b') }}
        )
        select
            stg.id,
            a.source as a_source,
            b.source as b_source
        from stg
        cross join a
        cross join b
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders_stg.sql": self.orders_stg,
            "orders_a.sql": self.orders_a,
            "orders_b.sql": self.orders_b,
            "combined.sql": self.combined,
        }

    def C(self, project):
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results), (
            f"Expected all models to run successfully, got statuses "
            f"{[r.status for r in results]!r}. An unrelated alias named "
            "'orders' inside the 'stg' CTE likely caused the genuinely "
            "resolvable 'orders_a'/'orders_b' collision to be falsely "
            "flagged as ambiguous."
        )

        with get_connection(project.adapter):
            rel = relation_from_name(project.adapter, "combined")
            df = project.adapter.get_catalog(rel.database).get_relation(rel).collect()

        assert df["a_source"].to_list() == ["a"]
        assert df["b_source"].to_list() == ["b"]
