.PHONY: test-generic test xtest lint

test-iceberg-databricks:
	uv run --extra iceberg pytest --profile iceberg-databricks -n 0

test-iceberg:
	uv run --extra iceberg pytest --profile iceberg -n 0

test:
	uv run pytest --profile local -n 0

test-csv:
	uv run pytest --profile local --config csv -n 0

test-parquet:
	uv run pytest --profile local --config parquet -n 0

test-ndjson:
	uv run pytest --profile local --config ndjson -n 0

test-azure:
	uv run --extra azure pytest --profile azure -n 0

test-s3:
	uv run --extra s3 pytest --profile s3 -n 0


xtest:
	uv run pytest --profile local -n auto

lint:
	uv run prek
