from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.polars.catalogs.baseCatalog import CatalogConfig
from dbt.adapters.polars.catalogs.formats import FILE_FORMATS
from dbt.adapters.polars.catalogs.storageCatalog import StorageCatalog
from dbt.adapters.polars.relation import PolarsRelation
from dbt_common.exceptions import DbtRuntimeError
from deltalake import DeltaTable

logger = AdapterLogger("polars")

_FILE_FORMATS = frozenset(FILE_FORMATS)


class S3CatalogConfig(CatalogConfig):
    """Profile config for an AWS S3 (or S3-compatible) catalog.

    All kwargs beyond name/type/bucket/prefix are forwarded as needed: boto3.Session
    receives the subset matching its signature (aws_access_key_id, region_name,
    profile_name, …); everything else passes through as object_store storage_options
    (aws_endpoint_url, aws_allow_http, …).
    """

    def __init__(
        self,
        name: str,
        type: str,
        bucket: str,
        prefix: str = "",
        **session_kwargs: object,
    ) -> None:
        self.name = name
        self.type = type
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.session_kwargs = session_kwargs

    def unique_field(self) -> str:
        return f"{self.bucket}/{self.prefix}"

    def connection_keys(self) -> tuple[str, ...]:
        return ("name", "bucket", "prefix")


class AWSS3Catalog(StorageCatalog):
    config: S3CatalogConfig

    def __init__(self, config: S3CatalogConfig, project_root: str) -> None:
        try:
            import boto3  # noqa: F401
        except ImportError as exc:
            raise DbtRuntimeError(
                "The s3 extra is required for AWSS3Catalog. "
                "Install it with: pip install 'dbt-polars[s3]'"
            ) from exc
        super().__init__(config, project_root)
        self._s3_client = None

    # ── URI and credential helpers ────────────────────────────────────────────

    def _object_prefix(self, *parts: str) -> str:
        components = [self.config.prefix] if self.config.prefix else []
        components.extend(p.strip("/") for p in parts if p)
        return "/".join(components)

    def _boto3_session(self):
        import inspect

        import boto3

        valid_keys = inspect.signature(boto3.Session).parameters
        kwargs = {
            k: v for k, v in self.config.session_kwargs.items() if k in valid_keys
        }
        return boto3.Session(**kwargs)

    def _get_uri(self, relation: PolarsRelation) -> str:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        path = self._object_prefix(schema, identifier)
        if relation.file_format != "delta":
            path = f"{path}.{relation.file_format}"
        return f"s3://{self.config.bucket}/{path}"

    def _get_storage_options(self, _uri: str) -> dict[str, str]:
        creds = self._boto3_session().get_credentials()
        if creds is None:
            raise DbtRuntimeError(
                "No AWS credentials found. Configure via env vars, ~/.aws/credentials, "
                "or an IAM role."
            )
        frozen = creds.get_frozen_credentials()
        opts: dict[str, str] = {
            "aws_access_key_id": frozen.access_key,
            "aws_secret_access_key": frozen.secret_key,
        }
        if frozen.token:
            opts["aws_session_token"] = frozen.token
        # region_name is a boto3 session kwarg; object_store uses aws_region
        region = self.config.session_kwargs.get("region_name")
        if region:
            opts["aws_region"] = str(region)
        # Pass through any remaining object_store-specific keys
        for key in ("aws_endpoint_url", "aws_allow_http"):
            val = self.config.session_kwargs.get(key)
            if val:
                opts[key] = str(val)
        return opts

    def _get_s3_client(self):
        if self._s3_client is not None:
            return self._s3_client
        self._s3_client = self._boto3_session().client(
            "s3",
            endpoint_url=self.config.session_kwargs.get("aws_endpoint_url"),
        )
        return self._s3_client

    def _delete_objects_with_prefix(self, prefix: str) -> None:
        s3 = self._get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                s3.delete_objects(
                    Bucket=self.config.bucket, Delete={"Objects": objects}
                )

    # ── schema management ─────────────────────────────────────────────────────

    def create_schema(self, relation: PolarsRelation) -> None:
        logger.debug(
            f"create_schema (no-op for S3): {relation.catalog}/{relation.schema}"
        )

    def drop_schema(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        prefix = self._object_prefix(schema) + "/"
        logger.debug(f"Dropping schema {relation.catalog}/{schema}")
        self._delete_objects_with_prefix(prefix)

    def list_schemas(self) -> list[str]:
        base = (self.config.prefix + "/") if self.config.prefix else ""
        s3 = self._get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        schemas: set[str] = set()
        for page in paginator.paginate(
            Bucket=self.config.bucket, Prefix=base, Delimiter="/"
        ):
            for common_prefix in page.get("CommonPrefixes", []):
                schema = common_prefix["Prefix"].rstrip("/").split("/")[-1]
                schemas.add(schema)
        return list(schemas)

    def table_exists(self, relation: PolarsRelation) -> bool:
        uri = self._get_uri(relation)
        opts = self._get_storage_options(uri)
        if relation.file_format == "delta":
            return DeltaTable.is_deltatable(uri, storage_options=opts)
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        key = f"{self._object_prefix(schema, identifier)}.{relation.file_format}"
        try:
            self._get_s3_client().head_object(Bucket=self.config.bucket, Key=key)
            return True
        except Exception:
            return False

    def drop_relation(self, relation: PolarsRelation) -> None:
        schema = relation.schema or ""
        identifier = relation.identifier or ""
        logger.debug(
            f"Dropping table if exists {relation.catalog}/{schema}/{identifier}"
        )
        if relation.file_format == "delta":
            self._delete_objects_with_prefix(
                self._object_prefix(schema, identifier) + "/"
            )
        else:
            key = f"{self._object_prefix(schema, identifier)}.{relation.file_format}"
            self._get_s3_client().delete_object(Bucket=self.config.bucket, Key=key)

    def list_relations_without_caching(
        self, schema_relation: PolarsRelation
    ) -> list[PolarsRelation]:
        schema = schema_relation.schema or ""
        prefix = self._object_prefix(schema) + "/"
        s3 = self._get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        relations: list[PolarsRelation] = []

        opts = self._get_storage_options(f"s3://{self.config.bucket}/{prefix}")

        for page in paginator.paginate(
            Bucket=self.config.bucket, Prefix=prefix, Delimiter="/"
        ):
            for common_prefix in page.get("CommonPrefixes", []):
                dir_name = common_prefix["Prefix"].rstrip("/").split("/")[-1]
                dir_uri = (
                    f"s3://{self.config.bucket}/{self._object_prefix(schema, dir_name)}"
                )
                if DeltaTable.is_deltatable(dir_uri, storage_options=opts):
                    relations.append(
                        schema_relation.create(
                            database=schema_relation.database,
                            schema=schema,
                            identifier=dir_name,
                            type=RelationType.Table,
                            catalog=schema_relation.catalog,
                            file_format="delta",
                        )
                    )

            for obj in page.get("Contents", []):
                filename = obj["Key"].split("/")[-1]
                stem, _, ext = filename.rpartition(".")
                if ext in _FILE_FORMATS:
                    relations.append(
                        schema_relation.create(
                            database=schema_relation.database,
                            schema=schema,
                            identifier=stem,
                            type=RelationType.Table,
                            catalog=schema_relation.catalog,
                            file_format=ext,
                        )
                    )

        return relations
