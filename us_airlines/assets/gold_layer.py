from dagster import multi_asset, Output, AssetIn, AssetOut
import pandas as pd


LAYER = 'gold'
COMPUTE_KIND = 'Postgres'



@multi_asset(
    ins = {
        'dim_airport': AssetIn(
            key_prefix=['silver', 'carriers']
        )

    },
    outs={
        'airports_us_airlines': AssetOut(
            io_manager_key='psql_io_manager',
            key_prefix=['gold'],
            metadata={
                'primary_keys': ['iata_code'],
                'columns': ['iata_code', 'airport', 'city', 'state', 'latitude', 'longitude', 'sys_date']
            }
            
        )
    },  
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
    description='Airport Table'
)
def airports_us_airlines(context, dim_airport):
    return Output(
        dim_airport,
        metadata={
            'table': 'Airports',
            'records': len(dim_airport)
        }
    )


@multi_asset(
    ins = {
        'dim_airline': AssetIn(
            key_prefix=['silver', 'carriers']
        )
    },
    
    outs={
        'airlines_us_airlines': AssetOut(
            io_manager_key='psql_io_manager',
            key_prefix=['gold'],
            metadata={
                'primary_keys': ['iata_code'],
                'columns': ['iata_code', 'airline', 'sys_date']
            }
            
        )
    },  
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
    description='Airline Table'
)
def airlines_us_airlines(context, dim_airline):
    return Output(
        dim_airline,
        metadata={
            'table': 'Airlines',
            'records': len(dim_airline)
        }
    )



@multi_asset(
    ins = {
        'dim_cancellationcode': AssetIn(
            key_prefix=['silver', 'carriers']
        )
    },
    outs={
        'cancellation_codes_us_airlines': AssetOut(
            io_manager_key='psql_io_manager',
            key_prefix=['gold'],
            metadata={
                'primary_keys': ['cancellation_reason'],
                'columns': ['cancellation_reason', 'cancellation_description', 'sys_date']
            }
            
        )
    },  
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
    description='Cancellation Codes'
)
def cancellation_codes_us_airlines(context, dim_cancellationcode):
    
    return Output(
        dim_cancellationcode,
        metadata={
            'table': 'gold_cancellationcode',
            'records': len(dim_cancellationcode)
        }
    )



@multi_asset(
    ins = {
        'fact_flight': AssetIn(
            key_prefix=['silver', 'carriers']
        )
    },
    
    outs={
        'flights_us_airlines': AssetOut(
            io_manager_key='psql_io_manager',
            key_prefix=['gold'],
            metadata={
                'primary_keys': ['flight_date', 'airline', 'flight_number', 'origin_airport', 'destination_airport'],
                'columns': ['flight_date', 'day_of_week', 'airline', 'flight_number', 'tail_number',
                            'origin_airport', 'destination_airport','scheduled_departure', 'departure_time', 'taxi_out', 'wheels_off',
                            'scheduled_time', 'elapsed_time', 'air_time', 'distance', 'wheels_on', 'taxi_in', 'scheduled_arrival',
                            'arrival_time', 'arrival_delay', 'diverted', 'cancelled', 'cancellation_reason', 'sys_date']
            }
            
        )
    },  
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
    description='Flights'
    
)
def flights_us_airlines(context, fact_flight):
    
    return Output(
        fact_flight,
        metadata={
            'table': 'gold_flights',
            'records': len(fact_flight)
        }
    )