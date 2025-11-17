{{ config(materialized='view', tags=['marts','tpch']) }}

select
  c.customer_key,
  c.name         as customer_name,
  c.address      as customer_address,
  c.phone        as customer_phone,
  c.acct_bal     as customer_acct_bal,
  c.mktsegment   as customer_segment,
  c.comment      as customer_comment,

  -- dbt MetricFlow の 2 hop 仕様への対応
  -- lineitem -> Order -> customer-> nation を
  -- lineitem -> Order -> customer + nation へ変更
  c.nation_key,
  n.nation_name

from {{ ref('stg_tpch_customer') }} c
left join {{ ref('stg_tpch_nation') }} n
  on c.nation_key = n.nation_key
