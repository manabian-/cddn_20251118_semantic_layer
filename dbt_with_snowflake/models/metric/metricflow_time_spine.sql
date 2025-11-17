{{
   config(
       materialized = 'table',
   )
}}
with days as (
   {{
       dbt_utils.date_spine(
           'day',
           "to_date('1992-01-01','yyyy-mm-dd')",
           "to_date('1998-12-31','yyyy-mm-dd')"
       )
   }}
),
final as (
   select cast(date_day as date) as date_day
   from days
)
select * from final