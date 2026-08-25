from pathlib import Path


def resolve_relative_path(path: str | Path, project_root: str) -> Path:
    """Resolve `path` against `project_root` if it is not already absolute."""
    path = Path(path)
    if not path.is_absolute():
        path = Path(project_root) / path
    return path.resolve()
