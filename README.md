# dbt-polars

dbt-polars integrates the flexibility and performance of Polars with the data governance and lineage capabilities of dbt.

## Why dbt-polars

<details>
<summary><b>Bring dbt to your Polars code</b>
<ul>
<li>Organize Polars code in a familiar dbt project structure.</li>
<li>Get lineage and documentation for every model out of the box.</li>
<li>Test your models with dbt tests, written in SQL or Python.</li>
</ul>
</summary>

See [dbt compatibility](https://datamindedbe.github.io/incubator-dbt-polars/dbt-compatibility) for the full list of supported features.

</details>

<details>
<summary><b>Combine SQL and Python models</b>
<ul>
<li>Freely mix SQL and Python models, even when they are ephemeral.</li>
<li>Minimize data transfer by delaying model execution until write.</li>
<li>Python models provide an escape hatch for complex SQL logic.</li>
</ul>
</summary>

SQL and Python models both compile to a Polars LazyFrame, so they can `ref()` each other freely. The same model, written both ways:

<table>
<tr>
<th>SQL: <code>models/revenue_per_country.sql</code></th>
<th>Python: <code>models/revenue_per_country.py</code></th>
</tr>
<tr>
<td valign="top">

```sql
select
    country,
    sum(amount) as revenue
from {{ ref('orders') }}
where status = 'paid'
group by country
```

</td>
<td valign="top">

```python
import polars as pl

def model(dbt, session):
    orders = dbt.ref("orders")
    paid = orders.filter(
        pl.col("status") == "paid"
    )
    return paid.group_by("country").agg(
        revenue=pl.col("amount").sum()
    )
```

</td>
</tr>
</table>

#### Python ephemeral models

Unlike most dbt adapters, dbt-polars supports ephemeral Python models thanks to Polars' lazy frames. This is how it works:

<img src="https://raw.githubusercontent.com/datamindedbe/incubator-dbt-polars/main/docs/images/lazy-execution.svg" alt="An ephemeral Python model inlined into a SQL model" width="640">

1. Polars runs inside the dbt process, so Python models need no separate compute.
2. `ref()` returns a LazyFrame: a reference to storage. No data is read yet.
3. The ephemeral Python model returns a LazyFrame, which the downstream SQL or Python model uses like any other table. Both add to one query plan.
4. On write, Polars optimizes the full plan, reads only the data it needs, and streams the result to storage.

</details>

<details>
<summary><b>Adaptable to your platform</b>
<ul>
<li>Run Polars on any compute, avoiding lock-in to a costly warehouse.</li>
<li>Write to Databricks, Snowflake, Azure Blob Storage, Fabric, ...</li>
<li>Or write a <a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/custom">custom catalog extension</a> to perfectly fit your platform.</li>
</ul>
</summary>

dbt-polars uses **catalogs** to define where data is stored. Each catalog maps to a storage backend. You can configure multiple catalogs in a single profile and reference them from your models. See the [Catalog overview](https://datamindedbe.github.io/incubator-dbt-polars/catalog-overview).

</details>

## Supported catalogs

Catalog extensions define where dbt-polars reads and stores data.

<table>
<tr><th>Target</th><th>Catalog</th></tr>
<tr><td>Local filesystem</td><td><a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/local">local</a></td></tr>
<tr><td>Azure Blob Storage / ADLS Gen2</td><td><a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/azure">azure</a></td></tr>
<tr><td>Databricks Unity Catalog</td><td><a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/databricks">databricks</a> (Delta, Iceberg)</td></tr>
<tr><td>Microsoft Fabric OneLake</td><td><a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/fabric">fabric</a> (Delta, Iceberg)</td></tr>
<tr><td>Snowflake Horizon Catalog</td><td><a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/snowflake">snowflake</a> (Iceberg)</td></tr>
<tr><td>Other Iceberg catalogs</td><td><a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/iceberg">iceberg</a> (any pyiceberg backend, e.g. SQLite, REST, Glue)</td></tr>
<tr><td>Other platforms</td><td><a href="https://datamindedbe.github.io/incubator-dbt-polars/catalogs/custom">Write your own catalog extension</a></td></tr>
</table>

To support another platform, you can write your own catalog. See [Custom catalogs](https://datamindedbe.github.io/incubator-dbt-polars/catalogs/custom).

## Limitations

- Views are not supported, as there is no database to hold them. Use `table` or `ephemeral` instead.
- The `hash` macro is not supported. Use a Python model for hashing instead.

See [dbt compatibility](https://datamindedbe.github.io/incubator-dbt-polars/dbt-compatibility) for the full overview, including supported macros.

## Getting started

The first steps are below. For the full walkthrough, see the [Getting started](https://datamindedbe.github.io/incubator-dbt-polars/demos/getting_started) guide.

Install dbt-polars and create a new dbt project:

```bash
pip install dbt-polars
dbt init
```

When prompted, choose `polars` as the database and `local` as the catalog type. This creates a profile that writes tables to your local disk:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: polars
      schema: main
      catalog_type: local
      root: ./data
```

dbt-polars does not support views, so replace the `models:` block in `dbt_project.yml` with:

```yaml
models:
  my_project:
    +materialized: table
```

Then build the example models:

```bash
dbt run
```

The [Getting started](https://datamindedbe.github.io/incubator-dbt-polars/demos/getting_started) guide continues from here with other catalogs, seeds, ephemeral models, docs, tests and incremental ingestion.

## Documentation

The full documentation is available at [datamindedbe.github.io/incubator-dbt-polars](https://datamindedbe.github.io/incubator-dbt-polars/).
