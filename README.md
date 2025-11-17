## 概要

本レポジトリは、 Code-Driven データ分析ナイト #2 セマンティックレイヤーにおける発表資料の一部です。 dbt Platform での MetricFlow、 Snowflake Semantic Layer、 Databricks Metric View をほぼ同じロジックで実装しています。

https://code-based-presentation.connpass.com/event/374363/preview/

## ディレクトリ構成

- `dbt_with_snowflake` : dbt MetricFlow を Snowflake を用いて実装した例
- `snowflake_semantic_views` : Snowflake Semantic Layer を用いて実装した例
- `dbt_with_databricks` : dbt MetricFlow を Databricks を用いて実装した例
- `databricks_metric_views` : Databricks Metric View を用いて実装した例

## 使用データとメトリクス

### ソースのデータ

以下の公開サンプルデータを使用します。各サービス上で該当データにアクセスできることを前提としています。

- `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`（Snowflake）
- `samples.tpch`（Databricks）

### メトリックスの種類

dbt MetricFlow の[ドキュメント](https://docs.getdbt.com/docs/build/metrics-overview#default-granularity-for-metrics)にて紹介されている Metrics Type を参考に、以下の5種類のメトリックス（メジャー）をそれぞれのサービスで実装しています。

| #    | メトリックス種類 | 概要                                                         | 実装メトリック                  |
| ---- | -------------- | ------------------------------------------------------------ | ----------------------- |
| 1    | simple         | 1つの measure をそのまま指すシンプルなメトリクス。           | 総収益                  |
| 2    | ratio          | 比率を計算するメトリクス。                                   | 返品率                  |
| 3    | derived        | 既存のメトリクス同士を式で組み合わせて計算するメトリクス     | 前日の総収益            |
| 4    | cumulative     | 指定した期間で累積集計するメトリクス。                       | 月内累計総収益          |
| 5    | conversion     | 「基準イベント」と「コンバージョンイベント」が、同じエンティティ上で一定期間内に発生したかを追跡するメトリクス。 | 注文→出荷90日以内達成率 |

注: 「注文→出荷90日以内達成率」は、注文発生から90日以内に出荷された注文の割合を指します。