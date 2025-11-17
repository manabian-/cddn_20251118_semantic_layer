{{ config(materialized="view", tags=["staging", "tpch"]) }}

select
    cast(c_custkey as {{ dbt.type_int() }}) as customer_key,
    cast(c_name as {{ dbt.type_string() }}) as name,
    cast(c_address as {{ dbt.type_string() }}) as address,
    cast(c_nationkey as {{ dbt.type_int() }}) as nation_key,
    cast(c_phone as {{ dbt.type_string() }}) as phone,
    cast(c_acctbal as {{ dbt.type_numeric() }}) as acct_bal,
    cast(c_mktsegment as {{ dbt.type_string() }}) as mktsegment,
    cast(c_comment as {{ dbt.type_string() }}) as comment
from {{ source("tpch", "customer") }}
