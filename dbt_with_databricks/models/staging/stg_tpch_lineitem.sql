{{ config(materialized='view', tags=['staging','tpch']) }}

select
  cast(l_orderkey     as {{ dbt.type_int() }})      as order_key,
  cast(l_partkey      as {{ dbt.type_int() }})      as part_key,
  cast(l_suppkey      as {{ dbt.type_int() }})      as supplier_key,
  cast(l_linenumber   as {{ dbt.type_int() }})      as line_number,
  cast(l_quantity     as {{ dbt.type_numeric() }})  as quantity,
  cast(l_extendedprice as {{ dbt.type_numeric() }}) as extended_price,
  cast(l_discount     as {{ dbt.type_numeric() }})  as discount,
  cast(l_tax          as {{ dbt.type_numeric() }})  as tax,
  cast(l_returnflag   as {{ dbt.type_string() }})   as return_flag,
  cast(l_linestatus   as {{ dbt.type_string() }})   as line_status,
  cast(l_shipdate     as date)                      as ship_date,
  cast(l_commitdate   as date)                      as commit_date,
  cast(l_receiptdate  as date)                      as receipt_date,
  cast(l_shipinstruct as {{ dbt.type_string() }})   as ship_instruct,
  cast(l_shipmode     as {{ dbt.type_string() }})   as ship_mode,
  cast(l_comment      as {{ dbt.type_string() }})   as comment
from {{ source('tpch','lineitem') }}