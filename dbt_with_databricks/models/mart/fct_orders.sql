{{ config(materialized='view', tags=['marts','tpch']) }}

with src as (
  select * from {{ ref('stg_tpch_orders') }}
)
select
  -- 事実（メジャー候補）
  total_price,

  -- 粒度キー & 外部キー
  order_key,
  customer_key,

  -- 時間
  order_date,

  -- ディメンション（退化含む）
  order_status,
  order_priority,
  order_clerk,
  ship_priority,
  comment,

  -- ===== 互換エイリアス（Semantic Layer の既存メトリック用）=====
  order_key       as o_orderkey,
  customer_key    as o_custkey,
  order_status    as o_orderstatus,
  total_price     as o_totalprice,
  order_date      as o_orderdate,
  order_priority  as o_orderpriority,
  order_clerk     as o_clerk,
  ship_priority   as o_shippriority,
  comment         as o_comment
from src
