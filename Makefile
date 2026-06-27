.PHONY: test-generic test xtest lint

test-generic:
	uv run --directory dbt-polars pytest --table-format delta -n 0

test-table-formats:
	uv run --directory dbt-polars pytest -m "table_format_specific" -n 0

test:
	uv run --directory dbt-polars pytest -n 0

xtest:
	uv run --directory dbt-polars pytest -n auto

lint:
	uv run prek
