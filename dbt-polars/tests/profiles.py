import os


def local_catalog(name: str, root: str) -> dict:
    return {
        "type": "local",
        "name": name,
        "root": root,
    }


def s3_catalog(name: str, bucket: str, region: str, key_prefix: str) -> dict:
    return {
        "type": "s3",
        "name": name,
        "bucket": bucket,
        "region": region,
        "key_prefix": key_prefix,
    }


def default_target() -> dict:
    catalogs = [
        local_catalog(name="local", root="test_root/"),
        local_catalog(name="local2", root="test_root2/"),
    ]

    bucket = os.environ.get("S3_CATALOG_BUCKET")
    if bucket:
        catalogs.append(
            s3_catalog(
                name=os.environ.get("S3_CATALOG_NAME", "s3"),
                bucket=bucket,
                region=os.environ.get("S3_CATALOG_REGION", "us-east-1"),
                key_prefix=os.environ.get(
                    "S3_CATALOG_KEY_PREFIX", "dbt-polars-inttest"
                ),
            )
        )

    return {
        "type": "polars",
        "catalogs": catalogs,
    }
