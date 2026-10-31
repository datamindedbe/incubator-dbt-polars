---
slug: /contributing
---

# Contributing

## Running tests

Run the full test suite:

```bash
pytest
```

The test suite uses pytest and the `--profile` option to select the catalog backend:

```bash
pytest --profile local          # local filesystem (default)
pytest --profile iceberg        # Iceberg with SQLite
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

@pytest.mark.require_profiles("iceberg")
class TestIcebergOnly:
    ...

@pytest.mark.skip_profiles("local")
class TestNotLocal:
    ...
```
