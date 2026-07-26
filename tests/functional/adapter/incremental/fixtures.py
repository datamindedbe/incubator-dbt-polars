_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = dbt.type_string() %}

{% if is_incremental() %}

SELECT id,
       cast(field4 as {{string_type}}) AS field4,
       -- to validate new field and order change
       cast(field2 as {{string_type}}) as field2,
       cast(field1 as {{string_type}}) as field1
       -- to validate order change

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2

from source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as {{string_type}}) as field1
       ,field2
       ,cast(case when id <= 3 then null else field4 end as {{string_type}}) as field4

from source_data
order by id
"""
