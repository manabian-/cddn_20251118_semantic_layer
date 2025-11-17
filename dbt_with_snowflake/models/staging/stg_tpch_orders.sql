{{ config(materialized='view', tags=['staging','tpch']) }}

select
  cast(o_orderkey     as {{ dbt.type_int() }})      as order_key,
  cast(o_custkey      as {{ dbt.type_int() }})      as customer_key,
  cast(o_orderstatus  as {{ dbt.type_string() }})   as order_status,
  cast(o_totalprice   as {{ dbt.type_numeric() }})  as total_price,
  cast(o_orderdate    as date)                      as order_date,
  cast(o_orderpriority as {{ dbt.type_string() }})  as order_priority,
  cast(o_clerk        as {{ dbt.type_string() }})   as order_clerk,
  cast(o_shippriority as {{ dbt.type_int() }})      as ship_priority,
  cast(o_comment      as {{ dbt.type_string() }})   as comment
from {{ source('tpch','orders') }}