# dbt-polars

dbt-polars integrates the flexibility and performance of Polars with the data governance and lineage capabilities of dbt.

**[Try the quick start](https://datamindedbe.github.io/incubator-dbt-polars/quickstart)** and run your first dbt-polars project in a few minutes, on your own laptop.

## Python and SQL, side by side

Choose Python or SQL per model, whichever fits the transformation best. Both run on the same Polars engine and can freely reference each other. Unlike in dbt-core, Python models are first class and can do anything a SQL model can, even be ephemeral.

<table>
<tr>
<th>Python: <code>models/revenue.py</code></th>
<th>SQL: <code>models/revenue.sql</code></th>
</tr>
<tr>
<td valign="top">

```python
import polars as pl


def model(dbt, session):
    return (
        dbt.ref("orders")
        .group_by("country")
        .agg(revenue=pl.sum("amount"))
    )
```

</td>
<td valign="top">

```sql
select
    country,
    sum(amount) as revenue
from {{ ref('orders') }}
group by country
```

</td>
</tr>
</table>

## The flexibility of Python and Polars

<img src="https://raw.githubusercontent.com/datamindedbe/incubator-dbt-polars/main/docs/images/catalogs.svg" alt="dbt-polars runs on any compute and writes to any of its catalogs" width="720">

**Run anywhere.** Polars executes your models in the same process as dbt. You don't need an expensive data warehouse to run your transformations. dbt-polars runs anywhere you can run Python.

**Store data where you want.** dbt-polars connects to popular platforms through catalogs. Setting one up only takes a few lines in your profile.

| Platform | Catalog |
|---|---|
| Local filesystem | [local](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/local) |
| Azure Blob Storage / ADLS Gen2 | [azure](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/azure) |
| Databricks Unity Catalog | [databricks](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/databricks) (Delta, Iceberg) |
| Microsoft Fabric OneLake | [fabric](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/fabric) (Delta, Iceberg) |
| Snowflake Horizon Catalog | [snowflake](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/snowflake) (Iceberg) |
| Other Iceberg catalogs, such as REST or Glue | [iceberg](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/iceberg) |

**Extensible to any platform.** Implement a [custom catalog](https://datamindedbe.github.io/incubator-dbt-polars/guides/custom-catalog) to tailor dbt-polars to your platform. A custom catalog controls authentication, where data is written and how metadata is registered. The dbt-polars engine still performs the writes.

## The familiarity of dbt

If you know dbt, you already know how to use dbt-polars. Introduce dbt-polars to your Python projects to benefit from key dbt features, such as lineage, documentation and tests. See [dbt compatibility](https://datamindedbe.github.io/incubator-dbt-polars/dbt-compatibility) for the full list of supported dbt features.

<img src="https://raw.githubusercontent.com/datamindedbe/incubator-dbt-polars/main/docs/demos/images/getting-started-lineage.png" alt="Lineage graph of a dbt-polars project" width="720">

## Built for efficiency

dbt-polars builds on the full power of the Polars engine. Models run lazily, so Polars optimizes the whole query before running it. Execution and writes are streamed in batches to keep memory use low.

## Get started

Install dbt-polars and create a new project:

```bash
pip install dbt-polars
dbt init
```

The [quick start](https://datamindedbe.github.io/incubator-dbt-polars/quickstart) walks you through the rest. It runs locally and writes tables to your disk, so you don't need a cloud account. Afterwards, the [demo project](https://datamindedbe.github.io/incubator-dbt-polars/tutorial/loading-data) builds a weather data pipeline that uses many of dbt-polars' features along the way.

## Limitations

- Views are not supported, as there is no database to hold them. Use `table` or `ephemeral` instead.
- The `hash` macro is not supported. Use a Python model for hashing instead.

## Documentation

The full documentation is available at [datamindedbe.github.io/incubator-dbt-polars](https://datamindedbe.github.io/incubator-dbt-polars/).
