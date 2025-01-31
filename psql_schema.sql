
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
    DAY_OF_WEEK int8,
    AIRLINE text,
    FLIGHT_NUMBER int8,
    TAIL_NUMBER text,
    ORIGIN_AIRPORT text,
    DESTINATION_AIRPORT text,
    SCHEDULED_DEPARTURE timestamptz,
    DEPARTURE_TIME timestamptz,
    TAXI_OUT int8,
    WHEELS_OFF timestamptz,
    SCHEDULED_TIME int8,
    ELAPSED_TIME int8,
    AIR_TIME int8,
    DISTANCE int8,
    WHEELS_ON timestamptz,
    TAXI_IN int8,
    SCHEDULED_ARRIVAL timestamptz,
    ARRIVAL_TIME timestamptz,
    ARRIVAL_DELAY float,
    DIVERTED int8,
    CANCELLED int8,
    CANCELLATION_REASON text,
    sys_date date,
    primary key(FLIGHT_DATE, AIRLINE, FLIGHT_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT)
);


