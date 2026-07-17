.PHONY: test-generic test xtest lint

test-iceberg-databricks:
	uv run --extra iceberg --directory dbt-polars pytest --profile iceberg-databricks -n 0

test-iceberg:
	uv run --extra iceberg --directory dbt-polars pytest --profile iceberg -n 0

test:
	uv run --directory dbt-polars pytest --profile local -n 0

test-csv:
	uv run --directory dbt-polars pytest --profile local --config csv -n 0

test-parquet:
	uv run --directory dbt-polars pytest --profile local --config parquet -n 0

test-ndjson:
	uv run --directory dbt-polars pytest --profile local --config ndjson -n 0

test-azure:
	uv run --extra azure --directory dbt-polars pytest --profile azure -n 0

test-s3:
	uv run --extra azure --directory dbt-polars pytest --profile s3 -n 0


xtest:
	uv run --directory dbt-polars pytest --profile local -n auto

lint:
	uv run prek
