## クエリ
### 1. SIMPLE（総収益）-----

```cli
dbt sl query --metrics total_revenue --group-by customer__nation_name --order-by customer__nation_name --where "{{ TimeDimension('metric_time','day') }} >= '1993-01-01' AND {{ TimeDimension('metric_time','day') }} < '1993-02-01'"
```

### 2. RATIO（返品率） 

```cli
dbt sl query --metrics return_rate,returned_lines,total_lines_metric --group-by metric_time__day --where "{{ TimeDimension('metric_time','day') }} >= '1993-01-01' AND {{ TimeDimension('metric_time','day') }} < '1993-02-01'" --order-by metric_time
```

### 3. DERIVED（前日の総収益）

# ToDo このまま 2/1 がデータがなく値をとれないかも。
# `--start-time` と `--end-time` を dbt platfrom で指定できないことが原因かも

```cli
dbt sl query --metrics total_revenue,total_revenue_prev_day --group-by metric_time__day --order-by metric_time --where "{{ TimeDimension('metric_time','day') }} >= '1993-02-01' AND {{ TimeDimension('metric_time','day') }} < '1993-03-01'" --order-by metric_time
```

### 4. CUMULATIVE（月内累計総収益） 

```cli
dbt sl query --metrics total_revenue,revenue_mtd --group-by metric_time__day --order-by metric_time --where "{{ TimeDimension('metric_time','day') }} >= '1993-02-01' AND {{ TimeDimension('metric_time','day') }} < '1993-03-01'" --order-by metric_time
```

### 5. CONVERSION（注文→出荷90日以内達成率） 

```cli
dbt sl query --metrics order_to_ship_conversion_rate_90d --group-by metric_time__month --order-by metric_time --where "{{ TimeDimension('metric_time','month') }} = '1993-02-01'"
```
