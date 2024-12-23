
USE DATABASE postgres;
CREATE SCHEMA gold;


set search_path to gold;


CREATE TABLE IF NOT EXISTS gold.airlines(
    iata_code varchar (3) not null,
    airline varchar(30),
    sys_date date,
    primary key (iata_code)
);


CREATE TABLE IF NOT EXISTS gold.airports(
    iata_code varchar(3) not null, 
    airport varchar(100),
    city varchar(50),
    state varchar(60),
    latitude float(16),
    longitude float(16),
    sys_date date,
    primary key (iata_code)
);



CREATE TABLE IF NOT EXISTS gold.cancellation_codes (
    cancellation_reason varchar(2),
    cancellation_description varchar(20),
    sys_date date,
    primary key (cancellation_reason)
);


CREATE TABLE IF NOT EXISTS gold.flights(
    FLIGHT_DATE date,
    DAY_OF_WEEK int,
    AIRLINE varchar(3),
    FLIGHT_NUMBER int,
    TAIL_NUMBER varchar(7),
    ORIGIN_AIRPORT varchar(4),
    DESTINATION_AIRPORT varchar(4),
    SCHEDULED_DEPARTURE varchar(4),
    DEPARTURE_TIME varchar(4),
    TAXI_OUT int,
    WHEELS_OFF varchar(4),
    SCHEDULED_TIME int,
    ELAPSED_TIME int,
    AIR_TIME int,
    DISTANCE int,
    WHEELS_ON varchar(4),
    TAXI_IN int,
    SCHEDULED_ARRIVAL varchar(4),
    ARRIVAL_TIME varchar(4),
    ARRIVAL_DELAY float(4),
    DIVERTED int,
    CANCELLED int,
    CANCELLATION_REASON varchar(2),
    sys_date date,
    primary key(FLIGHT_DATE, AIRLINE, FLIGHT_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT)
);


