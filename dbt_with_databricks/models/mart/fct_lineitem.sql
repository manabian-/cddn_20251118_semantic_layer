{{ config(materialized="view", tags=["marts", "tpch"]) }}

with li as (select * from {{ ref("stg_tpch_lineitem") }})
select
    -- 粒度キー
    {{ dbt_utils.generate_surrogate_key(["order_key", "line_number"]) }}
    as order_item_key,

    -- 外部キー
    order_key,
    part_key,
    supplier_key,

    -- 明細属性 / 時間
    line_number,
    ship_date,
    commit_date,
    receipt_date,
    return_flag,
    line_status,
    ship_instruct,
    ship_mode,
    comment,

    -- 価格・数量
    quantity,
    extended_price,
    discount,
    tax,

    -- 典型メトリック派生列（使い回し用）
    extended_price * (1 - discount) as gross_revenue,
    extended_price * discount as discount_amount,
    (extended_price * (1 - discount)) * (1 + tax) as net_revenue,
    extended_price * tax as tax_amount,

    -- 便利用フラグ
    case when receipt_date > commit_date then true else false end as is_delayed,
    case when return_flag = 'R' then true else false end as is_returned,

    -- ===== 互換エイリアス（Semantic Layer の既存メトリック用）=====
    order_key as l_orderkey,
    part_key as l_partkey,
    supplier_key as l_suppkey,
    line_number as l_linenumber,
    quantity as l_quantity,
    extended_price as l_extendedprice,
    discount as l_discount,
    tax as l_tax,
    return_flag as l_returnflag,
    line_status as l_linestatus,
    ship_date as l_shipdate,
    commit_date as l_commitdate,
    receipt_date as l_receiptdate,
    ship_instruct as l_shipinstruct,
    ship_mode as l_shipmode,
    comment as l_comment
from li
