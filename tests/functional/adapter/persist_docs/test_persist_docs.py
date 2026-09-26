from dbt.adapters.polars.testing.data_operations import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)


class TestPersistDocs(BasePersistDocs):
    pass


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass
