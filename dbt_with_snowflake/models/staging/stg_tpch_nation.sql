{{ config(materialized='view', tags=['staging','tpch']) }}

select
  cast(n_nationkey as {{ dbt.type_int() }})    as nation_key,
  cast(n_name      as {{ dbt.type_string() }}) as nation_name,
  cast(n_regionkey as {{ dbt.type_int() }})    as region_key,
  cast(n_comment   as {{ dbt.type_string() }}) as comment
from {{ source('tpch','nation') }}