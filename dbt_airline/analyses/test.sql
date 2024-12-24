--1.
SELECT C1.AIRLINE, C1.QUARTER, sum(C1.CANCELLED)
FROM 
(
    SELECT AIRLINE, CANCELLED, CASE 
    WHEN MONTH BETWEEN 1 AND 3 THEN 'Q1'
    WHEN MONTH BETWEEN 4 AND 6 THEN 'Q2'
    WHEN MONTH BETWEEN 7 AND 9 THEN 'Q3'
    WHEN MONTH BETWEEN 10 AND 12 THEN 'Q4' END AS QUARTER
    FROM flights 
    WHERE YEAR = 2015
) AS C1
GROUP BY C1.AIRLINE, C1.QUARTER;




--2.
with delay_cte as (
    SELECT origin_airport, sum(AIR_SYSTEM_DELAY) as AIR_SYSTEM_DELAY, sum(SECURITY_DELAY) as SECURITY_DELAY, sum(AIRLINE_DELAY) as AIRLINE_DELAY , 
    sum(LATE_AIRCRAFT_DELAY) as LATE_AIRCRAFT_DELAY, sum(WEATHER_DELAY) as WEATHER_DELAY, sum(distance) as distance, count(*) as n_flights
    FROM flights
    WHERE year = 2015
    group by origin_airport
)

SELECT 
    D.AIR_SYSTEM_DELAY + D.SECURITY_DELAY + D.AIRLINE_DELAY + D.LATE_AIRCRAFT_DELAY + D.WEATHER_DELAY as total_delay,
    D.DISTANCE, A.STATE, D.n_flights

FROM delay_cte D
join airports A on A.IATA_CODE = D.origin_airport
group by D.origin_airport;


--3.

with cte as (
SELECT F.day_of_week, sum(F.n_flights) AS n_flights, F.airport_code, A.STATE
    FROM (
        SELECT count(*) AS n_flights ,day_of_week, origin_airport AS airport_code
        FROM flights
        GROUP BY day_of_week,  origin_airport

        union all

        SELECT count(*) AS n_flights, day_of_week, destination_airport AS airport_code
        FROM flights
        GROUP BY day_of_week,  destination_airport
    ) AS F
    join airports AS A ON  A.IATA_CODE = F.airport_code
    GROUP BY F.day_of_week, F.airport_code, A.STATE
    order by  A.STATE, F.airport_code

), cte2 as (
select MAX(n_flights) as active, state
from cte
--where day_of_week = 7
GROUP by state)

select cte.*
FROM cte
join cte2 on cte.n_flights = cte2.active and cte.state = cte2.state
--and day_of_week = 7
order by cte.state, cte.airport_code;



with cte as (
SELECT F.day_of_week, sum(F.n_flights) AS n_flights, F.airport_code, A.STATE
    FROM (
        SELECT count(*) AS n_flights ,day_of_week, origin_airport AS airport_code
        FROM flights
        GROUP BY day_of_week,  origin_airport

        union all

        SELECT count(*) AS n_flights, day_of_week, destination_airport AS airport_code
        FROM flights
        GROUP BY day_of_week,  destination_airport
    ) AS F
    join airports AS A ON  A.IATA_CODE = F.airport_code
    GROUP BY F.day_of_week, F.airport_code, A.STATE
    order by  A.STATE, F.airport_code

), cte2 as (
select MAX(n_flights) as active, state
from cte
GROUP by state)

select cte.*
FROM cte
join cte2 on cte.n_flights = cte2.active and cte.state = cte2.state
order by cte.state, cte.airport_code;











