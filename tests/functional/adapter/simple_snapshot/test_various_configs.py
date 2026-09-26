from dbt.adapters.polars.testing.data_operations import (
    BaseSnapshotColumnNames,
    BaseSnapshotColumnNamesFromDbtProject,
    BaseSnapshotDbtValidToCurrent,
    BaseSnapshotInvalidColumnNames,
    BaseSnapshotIsDeletedColumnName,
    BaseSnapshotMultiUniqueKey,
)


class TestSnapshotColumnNames(BaseSnapshotColumnNames):
    pass


class TestSnapshotColumnNamesFromDbtProject(BaseSnapshotColumnNamesFromDbtProject):
    pass


class TestSnapshotInvalidColumnNames(BaseSnapshotInvalidColumnNames):
    pass


class TestSnapshotDbtValidToCurrent(BaseSnapshotDbtValidToCurrent):
    pass


class TestSnapshotMultiUniqueKey(BaseSnapshotMultiUniqueKey):
    pass


class TestSnapshotIsDeletedColumnName(BaseSnapshotIsDeletedColumnName):
    pass
