from typing import Any

CONFIG_PRESETS: dict[str, dict[str, Any]] = {
    "default": {"models": {}, "seeds": {}},
    "csv": {
        "models": {"+file_format": "csv"},
        "seeds": {"+file_format": "csv"},
    },
    "parquet": {
        "models": {"+file_format": "parquet"},
        "seeds": {"+file_format": "parquet"},
    },
    "ndjson": {
        "models": {"+file_format": "ndjson"},
        "seeds": {"+file_format": "ndjson"},
    },
}
