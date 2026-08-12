# Changelog

## v0.0.2

### Breaking

- schema parameter required on catalog. Set schema="" to restore old behavior.

### Bugs fixed

- Error when schema was unspecified on both the model and profile level.
- Iceberg Catalog: Error when using relative, local paths for warehouse or uri.
