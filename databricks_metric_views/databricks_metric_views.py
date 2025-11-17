# Databricks notebook source
# MAGIC %md
# MAGIC ## データベースとスキーマを作成

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS cddn_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cddn_demo.cddn_demo_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## デフォルトのデータベースとスキーマを変更

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG cddn_demo;
# MAGIC USE SCHEMA cddn_demo_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Metric Views を定義

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW TPCH_LINEITEM_MV
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC COMMENT 'Metric view version of TPCH_LINEITEM_SV (lineitem + orders + customer + nation)'
# MAGIC AS $$
# MAGIC version: 1.1
# MAGIC
# MAGIC # line_items をファクトにしたスノーフレーク・スキーマ
# MAGIC source: samples.tpch.lineitem
# MAGIC
# MAGIC joins:
# MAGIC   - name: orders
# MAGIC     source: samples.tpch.orders
# MAGIC     on: source.l_orderkey = orders.o_orderkey
# MAGIC     joins:
# MAGIC       - name: customer
# MAGIC         source: samples.tpch.customer
# MAGIC         on: orders.o_custkey = customer.c_custkey
# MAGIC         joins:
# MAGIC           - name: nation
# MAGIC             source: samples.tpch.nation
# MAGIC             on: customer.c_nationkey = nation.n_nationkey
# MAGIC
# MAGIC dimensions:
# MAGIC   # 日付系
# MAGIC   - name: ship_date
# MAGIC     expr: l_shipdate
# MAGIC   - name: ship_month
# MAGIC     expr: DATE_TRUNC('month', l_shipdate)
# MAGIC
# MAGIC   - name: order_date
# MAGIC     expr: orders.o_orderdate
# MAGIC   - name: order_month
# MAGIC     expr: DATE_TRUNC('month', orders.o_orderdate)
# MAGIC
# MAGIC   # orders -> customer という結合であるため expr が独特
# MAGIC   # customer / nation
# MAGIC   - name: customer_name
# MAGIC     expr: orders.customer.c_name
# MAGIC
# MAGIC   - name: nation_key
# MAGIC     expr: orders.customer.nation.n_nationkey
# MAGIC
# MAGIC   - name: nation_name
# MAGIC     expr: orders.customer.nation.n_name
# MAGIC
# MAGIC measures:
# MAGIC   # ----- 1. SIMPLE（総収益） -----
# MAGIC   - name: total_revenue
# MAGIC     expr: SUM(l_extendedprice * (1 - l_discount))
# MAGIC
# MAGIC   # ----- 2. RATIO（返品率） -----
# MAGIC   - name: total_lines
# MAGIC     expr: COUNT(1)
# MAGIC
# MAGIC   - name: returned_lines
# MAGIC     expr: SUM(CASE WHEN l_returnflag = 'R' THEN 1 ELSE 0 END)
# MAGIC
# MAGIC   - name: return_rate
# MAGIC     expr: MEASURE(returned_lines) / NULLIF(MEASURE(total_lines), 0)
# MAGIC     # Metric View は別 measure を MEASURE() で参照して比率を作る :contentReference[oaicite:3]{index=3}
# MAGIC
# MAGIC   # ----- 3. DERIVED（前日の総収益） -----
# MAGIC   - name: total_revenue_prev_day
# MAGIC     expr: SUM(l_extendedprice * (1 - l_discount))
# MAGIC     window:
# MAGIC       - order: ship_date
# MAGIC         range: trailing 1 day
# MAGIC         semiadditive: last
# MAGIC     # window measure で「前日分」を表現 :contentReference[oaicite:4]{index=4}
# MAGIC
# MAGIC   # ----- 4. CUMULATIVE（月内累計総収益） -----
# MAGIC   - name: revenue_mtd
# MAGIC     expr: SUM(l_extendedprice * (1 - l_discount))
# MAGIC     window:
# MAGIC       - order: ship_date
# MAGIC         range: cumulative
# MAGIC         semiadditive: last
# MAGIC       - order: ship_month
# MAGIC         range: current
# MAGIC         semiadditive: last
# MAGIC     # Period-to-date のパターンを YTD サンプルにならって MTD 化 :contentReference[oaicite:5]{index=5}
# MAGIC
# MAGIC   # ----- 5. CONVERSION（注文→出荷90日以内達成率） -----
# MAGIC   # 90 日以内に 1 回でも出荷された注文数
# MAGIC   - name: orders_shipped_90d
# MAGIC     expr: COUNT(
# MAGIC             DISTINCT CASE
# MAGIC               WHEN l_shipdate >= orders.o_orderdate
# MAGIC                AND l_shipdate < date_add(orders.o_orderdate, 91)
# MAGIC               THEN l_orderkey
# MAGIC             END
# MAGIC           )
# MAGIC
# MAGIC   # 発注数（注文粒度に DISTINCT）
# MAGIC   - name: orders_placed
# MAGIC     expr: COUNT(DISTINCT l_orderkey)
# MAGIC
# MAGIC   # コンバージョン率（メトリクス同士の式）
# MAGIC   - name: order_to_ship_conversion_rate_90d
# MAGIC     expr: MEASURE(orders_shipped_90d) / NULLIF(MEASURE(orders_placed), 0)
# MAGIC $$;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 定義を確認
# MAGIC DESCRIBE TABLE EXTENDED TPCH_LINEITEM_MV;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED TPCH_LINEITEM_MV AS JSON;

# COMMAND ----------

# 上記のビュー名を Ctrl を押しながら選択すると左部にてそのビューのオブジェクトが表示される
# ディメンションとメジャーの確認が可能

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Metric Views からデータを取得

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. SIMPLE（総収益）

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 自然言語クエリ例： 出荷日が1993年1月の国ごとの総収益を教えてください
# MAGIC SELECT
# MAGIC     nation_name,
# MAGIC     measure(total_revenue) AS total_revenue
# MAGIC FROM
# MAGIC     TPCH_LINEITEM_MV
# MAGIC WHERE
# MAGIC     ship_date >= DATE '1993-01-01'
# MAGIC     AND ship_date < DATE '1993-02-01'
# MAGIC GROUP BY
# MAGIC     nation_name
# MAGIC ORDER BY
# MAGIC     nation_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. RATIO（返品率）

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 自然言語クエリ例： 出荷日が1993年1月の日別返品率を教えてください
# MAGIC SELECT
# MAGIC     ship_date,
# MAGIC     measure(return_rate) AS return_rate,
# MAGIC     measure(returned_lines) AS returned_lines,
# MAGIC     measure(total_lines) AS total_lines
# MAGIC FROM
# MAGIC     TPCH_LINEITEM_MV
# MAGIC WHERE
# MAGIC     ship_date >= DATE '1993-01-01'
# MAGIC     AND ship_date < DATE '1993-02-01'
# MAGIC GROUP BY
# MAGIC     ship_date
# MAGIC ORDER BY
# MAGIC     ship_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. DERIVED（前日の総収益）

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 自然言語クエリ例： 出荷日が1993年2月の日別収益と前日収益を教えてください
# MAGIC SELECT
# MAGIC     ship_date,
# MAGIC     measure(total_revenue) AS total_revenue,
# MAGIC     measure(total_revenue_prev_day) AS total_revenue_prev_day
# MAGIC FROM
# MAGIC     TPCH_LINEITEM_MV
# MAGIC WHERE
# MAGIC     ship_date >= DATE '1993-02-01'
# MAGIC     AND ship_date < DATE '1993-03-01'
# MAGIC GROUP BY
# MAGIC     ship_date
# MAGIC ORDER BY
# MAGIC     ship_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 国ごとに取得
# MAGIC SELECT
# MAGIC     nation_name,
# MAGIC     measure(total_revenue) AS total_revenue,
# MAGIC     measure(total_revenue_prev_day) AS total_revenue_prev_day
# MAGIC FROM
# MAGIC     TPCH_LINEITEM_MV
# MAGIC WHERE
# MAGIC     ship_date >= DATE '1993-02-01'
# MAGIC     AND ship_date < DATE '1993-03-01'
# MAGIC GROUP BY
# MAGIC     nation_name
# MAGIC ORDER BY
# MAGIC     nation_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. CUMULATIVE（月内累計総収益）

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 自然言語クエリ例： 出荷日が1993年2月の日別累計収益を教えてください
# MAGIC SELECT
# MAGIC     ship_date,
# MAGIC     ship_month,
# MAGIC     measure(total_revenue) AS total_revenue,
# MAGIC     measure(revenue_mtd) AS revenue_mtd
# MAGIC FROM
# MAGIC     TPCH_LINEITEM_MV
# MAGIC WHERE
# MAGIC     ship_date >= DATE '1993-02-01'
# MAGIC     AND ship_date < DATE '1993-03-01'
# MAGIC GROUP BY
# MAGIC     ship_date,
# MAGIC     ship_month
# MAGIC ORDER BY
# MAGIC     ship_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. CONVERSION（注文→出荷90日以内達成率）

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 自然言語クエリ例： 受注日が1993年2月のコンバージョン率を教えてください
# MAGIC SELECT
# MAGIC     order_month,
# MAGIC     measure(order_to_ship_conversion_rate_90d) AS conv_rate_90d
# MAGIC FROM
# MAGIC     TPCH_LINEITEM_MV
# MAGIC WHERE
# MAGIC     order_month = DATE '1993-02-01'
# MAGIC GROUP BY
# MAGIC     order_month
# MAGIC ORDER BY
# MAGIC     order_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- あっているか確認
# MAGIC WITH od AS (
# MAGIC     SELECT
# MAGIC         o_orderkey,
# MAGIC         o_orderdate
# MAGIC     FROM
# MAGIC         samples.tpch.orders
# MAGIC     WHERE
# MAGIC         o_orderdate >= DATE '1993-02-01'
# MAGIC         AND o_orderdate <  DATE '1993-03-01'
# MAGIC
# MAGIC ),
# MAGIC converted AS (
# MAGIC     SELECT DISTINCT
# MAGIC         od.o_orderkey
# MAGIC     FROM
# MAGIC         od
# MAGIC             JOIN samples.tpch.lineitem li
# MAGIC                 ON li.l_orderkey = od.o_orderkey
# MAGIC     WHERE
# MAGIC         li.l_shipdate >= od.o_orderdate
# MAGIC         AND li.l_shipdate < date_add(od.o_orderdate, 91)
# MAGIC )
# MAGIC SELECT
# MAGIC     count(*) AS base_orders,
# MAGIC     (
# MAGIC         SELECT
# MAGIC             count(*)
# MAGIC         FROM
# MAGIC             converted
# MAGIC     ) AS converted_orders,
# MAGIC     (
# MAGIC         SELECT
# MAGIC             count(*)
# MAGIC         FROM
# MAGIC             converted
# MAGIC     )
# MAGIC     / count(*) AS conversion_rate_pct
# MAGIC FROM
# MAGIC     od;

# COMMAND ----------

# end
