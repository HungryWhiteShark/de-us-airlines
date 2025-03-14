
{{
    config(tags = ['dbt_assets'])
}}
with cte as (
    select 
        airline, cancellation_description, total_cancelled, quarter, rank() over(partition by quarter order by total desc) top
    from 
        {{ ref ('total_cancelled_flights_by_quarter') }}
)
select 
    airline, cancellation_description, quarter, total_cancelled
from
    cte
where
    top <= 5;

