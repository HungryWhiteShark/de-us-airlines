
{{
    config(tags = ['dbt_assets'])
}}

with cte as 
(
    select 
        airline, cancellation_reason, count(cancellation_reason) as reasons, date_part('month', flight_date) as MONTH
    from 
        {{source('gold', 'flights') }}
    where 
        cancellation_reason is not null
    group by 
        airline, cancellation_reason, MONTH
)
SELECT airline, cancellation_description, sum(reasons) as total_cancelled, CASE 
    when MONTH BETWEEN 1 AND 3 THEN 'Q1'
    WHEN MONTH BETWEEN 4 AND 6 THEN 'Q2'
    WHEN MONTH  BETWEEN 7 AND 9 THEN 'Q3'
    WHEN MONTH BETWEEN 10 AND 12 THEN 'Q4' END AS QUARTER
FROM 
    cte
join 
    {{ source('gold', 'cancellation_codes') }} using (cancellation_reason) 
group by 
    airline, cancellation_description, quarter

