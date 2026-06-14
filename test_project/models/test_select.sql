{{ config(engine='polars') }}
select * from {{ ref('customers') }}
