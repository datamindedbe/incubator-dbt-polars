from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import ObjectIdentifierTypeDef

import boto3
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import BaseCatalog, CatalogConfig
from dbt.adapters.polars.formats import FORMAT_REGISTRY, TableFormatStr
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("polars")


@dataclass
class S3CatalogConfig(CatalogConfig):
    """Config for the S3 object-storage catalog.

    Both the boto3 client (used for listing / deletion) and the delta-rs
    engine inside polars (used for Delta Lake I/O) resolve AWS credentials
    from the standard chain: environment variables → ``~/.aws/credentials``
    → EC2/ECS instance profile.  No credentials are stored here.
    """

    name: str
    type: str

    bucket: str  # AWS S3 bucket name
    region: str  # AWS region, e.g. "eu-west-1"
    key_prefix: str | None = None  # optional path prefix inside the bucket
    table_format: TableFormatStr = "delta"  # default for new tables

    def unique_field(self) -> str:
        prefix = self.key_prefix.strip("/") if self.key_prefix else ""
        if prefix:
            return f"s3://{self.bucket}/{prefix}"
        return f"s3://{self.bucket}"

    def connection_keys(self) -> tuple[str, str, str]:
        return ("name", "bucket", "key_prefix")


class S3Catalog(BaseCatalog):
    """Delta Lake catalog backed by AWS S3 object storage.

    Schemas map to S3 key prefixes; tables are Delta Lake tables stored at
    ``s3://<bucket>/[<key_prefix>/]<schema>/<table>/``.

    Only Delta Lake format is supported.
    Iceberg support is tracked separately (see localCatalog for context).
    """

    config: S3CatalogConfig

    def __init__(self, config: S3CatalogConfig) -> None:
        super().__init__(config)
        aws_opts = {"AWS_REGION": config.region}
        # Instantiate ALL registered formats with AWS credentials.
        # This enables per-table format detection during listing so that a
        # schema may contain a mix of Delta, Parquet, and CSV tables.
        self._formats = {k: cls(aws_opts) for k, cls in FORMAT_REGISTRY.items()}
        if config.table_format not in self._formats:
            raise DbtRuntimeError(
                f"Unknown table_format {config.table_format!r}. "
                f"Valid options: {list(FORMAT_REGISTRY)}"
            )
        self._format = self._formats[config.table_format]  # default for new tables

    # ------------------------------------------------------------------
    # S3 client
    # ------------------------------------------------------------------

    @cached_property
    def _s3_client(self) -> S3Client:
        return boto3.client("s3", region_name=self.config.region)  # type: ignore[return-value]

    def _root_s3_prefix(self) -> str:
        """S3 key prefix for the catalog root (empty string or ``'prefix/'``)."""
        prefix = self.config.key_prefix.strip("/") if self.config.key_prefix else ""
        return f"{prefix}/" if prefix else ""

    def _schema_uri(self, schema: str) -> str:
        return f"s3://{self.config.bucket}/{self._root_s3_prefix()}{schema}"

    def _relation_uri(self, relation: PolarsRelation) -> str:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        if relation.identifier is None:
            raise DbtRuntimeError(f"Relation {relation} is missing an identifier")
        return f"{self._schema_uri(relation.schema)}/{relation.identifier}"

    # ------------------------------------------------------------------
    # S3-specific helpers
    # ------------------------------------------------------------------

    def _s3_prefix_from_uri(self, uri: str) -> str:
        """Strip ``s3://<bucket>/`` from a full URI to get the bare S3 key prefix."""
        return uri.removeprefix(f"s3://{self.config.bucket}/")

    def _delete_s3_prefix(self, prefix: str) -> None:
        """Delete all S3 objects whose key starts with *prefix*."""
        client = self._s3_client
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix):
            objects: list[ObjectIdentifierTypeDef] = [
                {"Key": key}
                for obj in page.get("Contents", [])
                if (key := obj.get("Key")) is not None
            ]
            if objects:
                client.delete_objects(
                    Bucket=self.config.bucket,
                    Delete={"Objects": objects},
                )

    # ------------------------------------------------------------------
    # Storage topology
    # ------------------------------------------------------------------

    def create_schema(self, relation: PolarsRelation) -> None:
        # S3 is a flat namespace — schemas are implicit key prefixes, nothing to create.
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        logger.debug(
            f"Creating schema {relation.catalog}/{relation.schema} (S3 prefix — no-op)"
        )

    def drop_schema(self, relation: PolarsRelation) -> None:
        if relation.schema is None:
            raise DbtRuntimeError(f"Relation {relation} is missing a schema")
        logger.debug(f"Dropping schema {relation.catalog}/{relation.schema}")
        prefix = self._s3_prefix_from_uri(self._schema_uri(relation.schema)) + "/"
        self._delete_s3_prefix(prefix)

    def list_schemas(self) -> list[str]:
        client = self._s3_client
        root_prefix = self._root_s3_prefix()
        paginator = client.get_paginator("list_objects_v2")
        schemas: list[str] = []
        for page in paginator.paginate(
            Bucket=self.config.bucket, Prefix=root_prefix, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []):
                if (raw := cp.get("Prefix")) is None:
                    continue
                name = raw[len(root_prefix) :].rstrip("/")
                if name:
                    schemas.append(name)
        return schemas

    def drop_relation(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"Dropping table if exists {relation.catalog}/"
            f"{relation.schema}/{relation.identifier}"
        )
        prefix = self._s3_prefix_from_uri(self._relation_uri(relation)) + "/"
        self._delete_s3_prefix(prefix)

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        if schema_relation.schema is None:
            raise DbtRuntimeError(f"Relation {schema_relation} is missing a schema")
        client = self._s3_client
        schema_prefix = (
            self._s3_prefix_from_uri(self._schema_uri(schema_relation.schema)) + "/"
        )
        paginator = client.get_paginator("list_objects_v2")
        relations: list[PolarsRelation] = []
        for page in paginator.paginate(
            Bucket=self.config.bucket, Prefix=schema_prefix, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []):
                if (raw := cp.get("Prefix")) is None:
                    continue
                table_name = raw[len(schema_prefix) :].rstrip("/")
                if not table_name:
                    continue
                relation_uri = (
                    f"{self._schema_uri(schema_relation.schema)}/{table_name}"
                )
                # Probe all registered formats to detect which one this table uses.
                detected = next(
                    (
                        f.table_format
                        for f in self._formats.values()
                        if f.is_table(relation_uri)
                    ),
                    None,
                )
                if detected is None:
                    continue  # Unrecognised prefix, skip silently
                relations.append(
                    schema_relation.create(
                        database=schema_relation.database,
                        schema=schema_relation.schema,
                        identifier=table_name,
                        type=RelationType.Table,
                        format=detected,
                        catalog=schema_relation.catalog,
                    )
                )
        return relations
