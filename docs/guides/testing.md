# Data tests

dbt uses [data tests](https://docs.getdbt.com/docs/build/data-tests) to check the data in your models, such as columns that should never be empty. dbt-polars supports these tests as in dbt-core, and adds tests written in Python:

| Test type | dbt-core | dbt-polars |
|---|---|---|
| Generic tests | ✅ | ✅ |
| Singular SQL tests | ✅ | ✅ |
| Singular Python tests | ❌️ | ✅ |
| Unit tests on SQL models | ✅ | ✅ |
| Unit tests on Python models | ❌️ | ✅ |

## Python tests

### Writing a test

A Python test is a `.py` file in the tests folder. The file needs to contain a function named `test`, which is the entrypoint for dbt-polars to run. Like a SQL test, it typically returns the rows that fail the test. The test below fails when a customer has a negative revenue:

```python
# tests/no_negative_revenue.py
import polars as pl


def test(dbt, session) -> pl.LazyFrame:
    revenue: pl.LazyFrame = dbt.ref("customer_revenue")

    return revenue.filter(pl.col("revenue") < 0)
```

### Returning a boolean

Python tests can also return a boolean. `True` indicates the test passed, while `False` indicates a failure. The following test passes when paid_orders is not empty.

```python
# tests/has_paid_orders.py
def test(dbt, session) -> bool:
    paid = dbt.ref("paid_orders").limit(1).collect()

    return not paid.is_empty()
```

### Configuring a test

`dbt.config` configures a Python test, like the config block of a SQL test. It supports the [test configurations](https://docs.getdbt.com/reference/data-test-configs) `store_failures`, `limit`, `fail_calc`, `warn_if` and `error_if`:

```python
# tests/no_large_orders.py
import polars as pl


def test(dbt, session) -> pl.LazyFrame:
    dbt.config(store_failures=True, warn_if="> 0", error_if="> 10")

    orders: pl.LazyFrame = dbt.ref("orders")

    return orders.filter(pl.col("amount") > 15)
```

This test warns when there are orders above 15, and only fails when there are more than 10 of them. With `store_failures`, dbt-polars writes the failing rows to a table, in a schema ending in `_dbt_test__audit`.

:::info
A test that returns a boolean ignores `store_failures`, `limit`, `fail_calc`, `warn_if` and `error_if`, because there are no rows to count.
:::

## Unit tests

A [unit test](https://docs.getdbt.com/docs/build/unit-tests) checks the logic of a model on a few made-up input rows, instead of the real data. dbt-polars runs unit tests on both SQL and Python models.

### Writing a unit test

Take a Python model that adds VAT to the orders:

```python
# models/order_totals.py
import polars as pl


def model(dbt, session) -> pl.LazyFrame:
    orders: pl.LazyFrame = dbt.ref("orders")

    return orders.select("order_id", (pl.col("amount") * 1.21).alias("amount_incl_vat"))
```

A unit test is defined in a YAML file in the models folder. It lists the input rows under `given` and the rows the model should return under `expect`:

```yaml
# models/unit_tests.yml
unit_tests:
  - name: test_vat
    model: order_totals
    given:
      - input: ref('orders')
        rows:
          - {order_id: 1, amount: 100}
          - {order_id: 2, amount: 50}
    expect:
      rows:
        - {order_id: 1, amount_incl_vat: 121}
        - {order_id: 2, amount_incl_vat: 60.5}
```

Run it with the other tests of the model:

```bash
dbt test --select order_totals
```

dbt replaces orders with the given rows, runs the model and compares the result with the expected rows. When they differ, dbt shows the rows that don't match.

### Input tables

dbt-polars converts the given rows to the column types of the input, so the input table must already exist in storage. Columns that you leave out are empty, and dates and timestamps can be written as text, such as 2024-01-01. `dbt build` builds the inputs of a model before running its unit tests:

```bash
dbt build --select +order_totals
```

An ephemeral model has no table. Write the rows of an ephemeral input as a SQL query instead, with `format: sql`:

```yaml
given:
  - input: ref('orders')
    format: sql
    rows: |
      select 1 as order_id, 100.0 as amount
```

## Read more

* [Data tests](https://docs.getdbt.com/docs/build/data-tests): generic and singular tests in the dbt documentation.
* [Unit tests](https://docs.getdbt.com/docs/build/unit-tests): all unit test options in the dbt documentation.
* [Python models](python-models.md): how `dbt.ref` and LazyFrames work in Python.
