.PHONY: test-generic test xtest lint

test-iceberg-databricks:
	uv run --extra iceberg --directory dbt-polars pytest --profile iceberg-databricks -n 0

test-iceberg:
	uv run --extra iceberg --directory dbt-polars pytest --profile iceberg -n 0

test:
	uv run --directory dbt-polars pytest --profile local -n 0

xtest:
	uv run --directory dbt-polars pytest --profile local -n auto

lint:
	uv run prek
