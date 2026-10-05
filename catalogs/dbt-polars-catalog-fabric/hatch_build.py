import sys
from email.parser import Parser
from pathlib import Path

from hatchling.metadata.plugin.interface import MetadataHookInterface

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def core_version(root: Path) -> str:
    """The dbt-polars version, which every catalog package shares."""
    pkg_info = root / "PKG-INFO"
    if pkg_info.exists():
        return Parser().parsestr(pkg_info.read_text())["Version"]
    with open(root.parent.parent / "pyproject.toml", "rb") as file:
        return tomllib.load(file)["project"]["version"]


def core_readme(root: Path) -> str:
    """The dbt-polars README; built from an sdist it's the copy included there."""
    local = root / "README.md"
    return (local if local.exists() else root.parent.parent / "README.md").read_text()


class CustomMetadataHook(MetadataHookInterface):
    def update(self, metadata: dict) -> None:
        version = core_version(Path(self.root))
        metadata["version"] = version
        metadata["readme"] = {
            "content-type": "text/markdown",
            "text": core_readme(Path(self.root)),
        }
        metadata["dependencies"] = [
            f"dbt-polars=={version}",
            *self.config.get("dependencies", []),
        ]
