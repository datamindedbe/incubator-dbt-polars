def model(dbt, session):
    orders = dbt.ref("orders")  # returns pl.LazyFrame
    customers = dbt.ref("customers")  # returns pl.LazyFrame
    return orders.join(customers, left_on="customer_id", right_on="id")
