---
sidebar_label: "4. Documentation"
---

# Documentation

The pipeline now loads the weather data and computes daily aggregates. dbt generates documentation for it, including a lineage graph of the models. This works the same in dbt-polars as in any other dbt project.

## Generating docs

```bash
dbt docs generate
dbt docs serve
```

This creates and starts a documentation website for the dbt project. It includes a lineage graph showing how models depend on each other. For the project so far:

![Lineage graph of the project so far](../demos/images/getting-started-lineage.png)

By using `ref` we enable dbt to collect this lineage.

## Further reading

* See [About documentation](https://docs.getdbt.com/docs/build/documentation) in the dbt documentation for adding descriptions to models and columns.
