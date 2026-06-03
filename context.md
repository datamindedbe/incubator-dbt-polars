# dbt-polars

## Summary

This repository contains a package dbt-polars, which is an adapter for dbt which uses Polars for executing models.

## Principles

### Multi catalog support

Users should be able to configure this adapter to target multiple storage systems (catalogs). In the initial phase, the following two source systems should be supported:

- local:  write the data to the local disk
- databricks: use Databricks unity catalog as the catalog

In the design of the adapter, logic related to the catalog should be separated from logic that is adapter independent, such that it is easy to add integrations for more catalogs (e.g. Azure blobstorage, AWS S3).

### Databricks integration

The databricks integration should never use Databricks compute for evaluating models. The only interaction with Databricks unity catalog should be through the API.

Data should be stored in Azure first and then registered via the API. To get credentials to access Azure, credential vending is used through the API endpoint /api/2.0/unity-catalog/temporary-table-credentials

## Writing data

Data is written in Delta format.

## SQL models

