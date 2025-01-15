DROP TABLE IF EXISTS airlines;
CREATE TABLE airlines (
    iata_code varchar (3),
    airline varchar(30),
    primary key (iata_code)
);


DROP TABLE IF EXISTS airports;
CREATE TABLE airports (
    iata_code varchar(3),
    airport varchar(100),
    city varchar(50),
    state varchar(60),
    country varchar(50),
    latitude float(16),
    longitude float(16),
    primary key (iata_code)
);


DROP TABLE IF EXISTS cancellation_codes;
CREATE TABLE cancellation_codes (
    cancellation_reason varchar(2),
    cancellation_description varchar(20),
    primary key (cancellation_reason)
);


DROP TABLE IF EXISTS flights;
CREATE TABLE flights (
    YEAR int,
    MONTH int,
    DAY int,
    DAY_OF_WEEK int,
    AIRLINE varchar(3),
    FLIGHT_NUMBER int,
    TAIL_NUMBER varchar(7),
    ORIGIN_AIRPORT varchar(4),
    DESTINATION_AIRPORT varchar(4),
    SCHEDULED_DEPARTURE varchar(4),
    DEPARTURE_TIME varchar(4),
    DEPARTURE_DELAY float(4),
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
    AIR_SYSTEM_DELAY int,
    SECURITY_DELAY int,
    AIRLINE_DELAY int,
    LATE_AIRCRAFT_DELAY int,
    WEATHER_DELAY int,
    primary key (year, month, day, airline, FLIGHT_NUMBER)
);
