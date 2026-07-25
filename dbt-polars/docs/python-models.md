# Python models and tests

dbt-polars supports Python models and Python singular tests. Both use Polars DataFrames instead of SQL.

## Python models

A Python model is a `.py` file in your `models/` directory. The required entrypoint is `def model(dbt, session)`:

```python
# models/my_model.py
import polars as pl

def model(dbt, session):
    dbt.config(materialized="table")
    df = dbt.ref("upstream_model")   # returns a Polars LazyFrame
    return df.filter(pl.col("amount") > 0)
```

The `session` argument is unused (it exists for interface compatibility) — use `dbt.ref()` and `dbt.source()` to access upstream data.

### Incremental Python models

```python
import polars as pl

def model(dbt, session):
    dbt.config(materialized="incremental", unique_key="id")
    df = dbt.ref("source_data")
    if dbt.is_incremental:
        df = df.filter(pl.col("updated_at") > dbt.this_date)
    return df
```

### Return type

Return a `pl.DataFrame` or `pl.LazyFrame`. dbt-polars materializes it using the configured `file_format` (default: Delta Lake).

## Python singular tests

Python singular tests live in the `tests/` directory. The entrypoint is `def test(dbt, pl)`:

```python
# tests/assert_no_negative_amounts.py
def test(dbt, pl):
    df = dbt.ref("my_model")
    return df.filter(pl.col("amount") < 0)   # rows returned = test failures
```

A test passes when the returned DataFrame (or LazyFrame) is empty. Any returned rows are counted as failures.

### Boolean return

Tests can also return a boolean directly:

```python
def test(dbt, pl):
    df = dbt.ref("my_model").collect()
    return df.height > 0   # True = pass, False = fail
```

### Test configuration

Use `dbt.config()` to set test options:

```python
def test(dbt, pl):
    dbt.config(store_failures=True)
    return dbt.ref("my_model").filter(pl.col("id").is_null())
```

Supported config options: `store_failures`, `limit`, `fail_calc`, `warn_if`, `error_if`.

## Running tests

Run the full test suite:

```bash
pytest
```

The test suite uses pytest and the `--profile` option to select the catalog backend:

```bash
pytest --profile local          # local filesystem (default)
pytest --profile iceberg        # Iceberg with SQLite
pytest --profile iceberg-databricks
pytest --profile azure
pytest --profile s3
```

### Running without the default `-n 0`

The `pyproject.toml` sets `addopts = "-n 0"` for local debugging. To override it (for example, to use pytest-xdist parallelism in CI):

```bash
pytest -o addopts="" -n auto
```

### Profile-specific markers

Tests can be restricted to or excluded from specific profiles:

```python
import pytest

@pytest.mark.require_profiles("iceberg", "iceberg-databricks")
class TestIcebergOnly:
    ...

@pytest.mark.skip_profiles("local")
class TestNotLocal:
    ...
```
