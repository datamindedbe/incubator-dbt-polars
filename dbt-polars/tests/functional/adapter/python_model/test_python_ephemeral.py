import pytest
from dbt.tests.util import run_dbt
from tests.conftest import PolarsTestMixin
from tests.utils import polars_read_relation

ephemeral_sql = """
{{ config(materialized='ephemeral') }}
select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
union all
select 3 as id, 'charlie' as name
"""

python_model = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='table')
    df = dbt.ref('ephemeral_source')
    return df.filter(pl.col('id') > 1)
"""


class TestPythonRefEphemeral(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_source.sql": ephemeral_sql,
            "python_from_ephemeral.py": python_model,
        }

    def test_python_ref_ephemeral(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter,
            "python_from_ephemeral",
            columns=["id", "name"],
            order_by="id",
        )
        assert rows == [(2, "bob"), (3, "charlie")]


ephemeral_python = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='ephemeral')
    return pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
"""

downstream_python = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='table')
    df = dbt.ref('ephemeral_python')
    return df.filter(pl.col('id') > 1)
"""

downstream_sql = """
{{ config(materialized='table') }}
select * from {{ ref('ephemeral_python') }}
where id > 1
"""


class TestEphemeralPythonInPythonModel(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_python.py": ephemeral_python,
            "downstream_python.py": downstream_python,
        }

    def test_ephemeral_python_in_python(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter,
            "downstream_python",
            columns=["id", "name"],
            order_by="id",
        )
        assert rows == [(2, "bob"), (3, "charlie")]


sql_ephemeral_for_mixed = """
{{ config(materialized='ephemeral') }}
select 1 as id, 'sql' as source
"""

python_ephemeral_for_mixed = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='ephemeral')
    return pl.DataFrame({"id": [2], "source": ["python"]})
"""

mixed_downstream_sql = """
{{ config(materialized='table') }}
select * from {{ ref('sql_ephemeral_for_mixed') }}
union all
select * from {{ ref('python_ephemeral_for_mixed') }}
"""


class TestMixedSqlAndPythonEphemeralInSqlModel(PolarsTestMixin):
    """A SQL model can ref both a SQL ephemeral and a Python ephemeral simultaneously.
    Both are inlined as CTEs by dbt; the adapter evaluates them independently and
    registers each as a frame before executing the downstream SQL."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sql_ephemeral_for_mixed.sql": sql_ephemeral_for_mixed,
            "python_ephemeral_for_mixed.py": python_ephemeral_for_mixed,
            "downstream_mixed.sql": mixed_downstream_sql,
        }

    def test_mixed_ephemeral_in_sql(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter,
            "downstream_mixed",
            columns=["id", "source"],
            order_by="id",
        )
        assert rows == [(1, "sql"), (2, "python")]


base_sql_ephemeral = """
{{ config(materialized='ephemeral') }}
select 1 as id, 'alice' as name
union all
select 2 as id, 'bob' as name
"""

filtering_python_ephemeral = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='ephemeral')
    df = dbt.ref('base_sql_ephemeral')
    return df.filter(pl.col('id') > 1)
"""

downstream_after_chain = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='table')
    return dbt.ref('filtering_python_ephemeral')
"""


downstream_sql_after_chain = """
{{ config(materialized='table') }}
select * from {{ ref('filtering_python_ephemeral') }}
"""


class TestSqlModelRefsPythonEphemeralChain(PolarsTestMixin):
    """SQL downstream refs a Python ephemeral, which itself refs a SQL ephemeral.
    The SQL ephemeral is a transitive dep — it never appears directly in the
    downstream SQL, only in extra_ctes. Exercises the full SQL→Python→SQL chain."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_sql_ephemeral.sql": base_sql_ephemeral,
            "filtering_python_ephemeral.py": filtering_python_ephemeral,
            "downstream_sql_after_chain.sql": downstream_sql_after_chain,
        }

    def test_sql_model_refs_python_ephemeral_chain(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter,
            "downstream_sql_after_chain",
            columns=["id", "name"],
            order_by="id",
        )
        assert rows == [(2, "bob")]


class TestPythonEphemeralRefsSqlEphemeral(PolarsTestMixin):
    """A Python ephemeral can ref a SQL ephemeral. dbt passes the CTE frame name
    (e.g. '__dbt__cte__base_sql_ephemeral') as the ref key, and the adapter resolves
    it from the accumulated CTE frames rather than looking up a catalog table."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_sql_ephemeral.sql": base_sql_ephemeral,
            "filtering_python_ephemeral.py": filtering_python_ephemeral,
            "downstream_after_chain.py": downstream_after_chain,
        }

    def test_python_ephemeral_refs_sql_ephemeral(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter,
            "downstream_after_chain",
            columns=["id", "name"],
            order_by="id",
        )
        assert rows == [(2, "bob")]


base_python_ephemeral_chained = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='ephemeral')
    return pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
"""

chained_python_ephemeral = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='ephemeral')
    df = dbt.ref('base_python_ephemeral_chained')
    return df.filter(pl.col('id') > 1)
"""

downstream_from_chained_python = """
import polars as pl

def model(dbt, _):
    dbt.config(materialized='table')
    return dbt.ref('chained_python_ephemeral')
"""


class TestChainedPythonEphemerals(PolarsTestMixin):
    """A Python ephemeral can ref another Python ephemeral. CTEs are evaluated in
    dependency order, so each ephemeral has access to previously computed frames
    when its own model function runs."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_python_ephemeral_chained.py": base_python_ephemeral_chained,
            "chained_python_ephemeral.py": chained_python_ephemeral,
            "downstream_from_chained_python.py": downstream_from_chained_python,
        }

    def test_chained_python_ephemerals(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter,
            "downstream_from_chained_python",
            columns=["id", "name"],
            order_by="id",
        )
        assert rows == [(2, "bob"), (3, "charlie")]


class TestEphemeralPythonInSqlModel(PolarsTestMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_python.py": ephemeral_python,
            "downstream_sql.sql": downstream_sql,
        }

    def test_ephemeral_python_in_sql(self, project):
        run_dbt(["run"])
        rows = polars_read_relation(
            project.adapter,
            "downstream_sql",
            columns=["id", "name"],
            order_by="id",
        )
        assert rows == [(2, "bob"), (3, "charlie")]
