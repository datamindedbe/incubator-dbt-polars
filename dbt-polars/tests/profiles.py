def local_catalog(name: str, root: str) -> dict:
    return {
        "type": "local",
        "name": name,
        "root": root,
    }


def default_target() -> dict:
    return {
        "type": "polars",
        "catalogs": [local_catalog(name="local", root="test_root/")],
    }
