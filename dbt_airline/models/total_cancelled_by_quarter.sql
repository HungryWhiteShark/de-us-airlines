
{{
    config(tags =['dbt_assets'])
}}

with cte as 
(
    SELECT AIRLINE, CANCELLED, CASE 
    WHEN MONTH BETWEEN 1 AND 3 THEN 'Q1'
    WHEN MONTH BETWEEN 4 AND 6 THEN 'Q2'
    WHEN MONTH BETWEEN 7 AND 9 THEN 'Q3'
    WHEN MONTH BETWEEN 10 AND 12 THEN 'Q4' END AS QUARTER
    FROM {{ source('gold', 'flights') }}
)
SELECT al.AIRLINE, cte.QUARTER, sum(cte.CANCELLED)
FROM cte
join airlines al on al.IATA_CODE = cte.airline
GROUP BY cte.AIRLINE, cte.QUARTER;




