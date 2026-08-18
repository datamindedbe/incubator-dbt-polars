# Changelog

## v0.0.2

### Breaking

- schema parameter required on catalog. Set schema="" to restore old behavior.
- New profile shape for single- vs multi-catalog setups, see [docs](docs/index.md#profile-structure).

### Bugs fixed

- Error when schema was unspecified on both the model and profile level.
- Iceberg Catalog: Error when using relative, local paths for warehouse or uri.
