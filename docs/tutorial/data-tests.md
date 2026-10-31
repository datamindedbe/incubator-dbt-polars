---
sidebar_label: "5. Data tests"
mdx:
  format: mdx
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Data tests

[Data tests](https://docs.getdbt.com/docs/build/data-tests) improve the reliability of your data. In dbt, data tests are placed in the `tests/` folder and return the rows that break a rule. Tests succeed when no rows are returned. As with the models, dbt-polars lets you write a test in SQL or in Python.

## Writing a test

The test below fails when a daily average temperature is below -20°C or above 40°C:

<Tabs groupId="language">
<TabItem value="python" label="Python" default>

```python
# quickstart/tests/meteo_daily_temperature_range.py
import polars as pl

def test(dbt, session):
    daily: pl.LazyFrame = dbt.ref("meteo_daily")
    return daily.filter((pl.col("temperature") < -20) | (pl.col("temperature") > 40))
```

<div className="alert alert--info" role="note">A Python test can also return a boolean, where `True` means the test passed.</div>

</TabItem>
<TabItem value="sql" label="SQL">

```sql
-- quickstart/tests/meteo_daily_temperature_range.sql
select *
from {{ ref('meteo_daily') }}
where temperature < -20 or temperature > 40
```

</TabItem>
</Tabs>

Run all tests with:

```bash
dbt test
```

## Further reading

* See [Data tests](../guides/testing.md) for Python tests, boolean tests and test configurations in dbt-polars.
* See [Data tests](https://docs.getdbt.com/docs/build/data-tests) in the dbt documentation for generic and singular tests.
