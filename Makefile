.PHONY: test-default test-generic test xtest lint

test-generic:
	cd dbt-polars && uv run pytest --table-format delta -n 0

test-table-formats:
	cd dbt-polars && uv run pytest -m "table_format_specific" -n 0

test:
	cd dbt-polars && uv run pytest -n 0

xtest:
	cd dbt-polars && uv run pytest -n auto

lint:
	uv run pre-commit run
