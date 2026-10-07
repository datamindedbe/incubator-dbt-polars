# dbt feature compatibility

Overview of features dbt-polars already supports and plans to support in the future.

| Status | Meaning |
|---|---|
| ✅ | Supported |
| 🟡 | Partially supported |
| 🚧 | Not supported yet, but planned |
| ❌️ | Not supported, unplanned |

## dbt core features

| Feature | Status | Remarks |
|---|---|---|
| table materialization | ✅ | |
| incremental materialization | 🟡 | ✅ append <br> ✅ merge <br> ✅ delete+insert<br/> 🚧 microbatch |
| ephemeral materialization | ✅ | |
| view materialization | ❌️ | dbt-polars has no database to hold views. Use table or ephemeral instead. |
| Snapshots | ✅ | |
| Seeds | ✅ | |
| Sources | ✅ | |
| Python models | ✅ | |
| Data tests (generic and singular SQL) | ✅ | |
| Python singular tests | ✅ | |
| Unit tests (defined in YAML) | 🚧 |  |
| persist_docs | ✅ | |
| dbt docs generate | ✅ | |
| dbt show | ✅ | |
| dbt clone | ❌️ | |
| Hooks | ❌️ | |
| Grants | ❌️ | |
| Model contracts | 🚧 | |

## dbt core extensions

dbt-polars adds two features that are not supported in dbt-core:

- Ephemeral Python models
- Python data tests

## Macros

dbt-core publishes a list of  [cross-database macros](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros) that each warehouse should implement.

| Macro group | Status | Remarks |
|---|---|---|
| Date/time | ✅ | |
| Hash | ❌️ |  |
| Type conversion | ✅ | |
| Strings | ✅ | |
| Arrays | 🟡 | Missing:<br>- array_append<br> - array_concat |
| Aggregates | 🟡 | Missing: listagg with an order or limit |
| Set operations | ✅ | |
| Other | ✅ | |

Unsupported macros raise a compilation error. Use a [Python model](python-models.md) for the same logic.
