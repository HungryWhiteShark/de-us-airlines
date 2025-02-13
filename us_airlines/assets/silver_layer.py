from dagster import asset, Output, AssetIn
import datetime
import pandas as pd

LAYER = 'silver'
GROUP_NAME = 'silver'
SCHEMA = 'carriers'
COMPUTE_KIND = 'MinIO'



@asset(
    ins = {
        'bronze_airport_dataset': AssetIn(
            key_prefix=['bronze', 'carriers']
        )
    },
    io_manager_key='minio_io_manager',
    key_prefix=[LAYER, SCHEMA],
    group_name=GROUP_NAME,
    compute_kind=COMPUTE_KIND,
    description='Silver Airport Dimension Table'
)
def dim_airport(context, bronze_airport_dataset):
    
    df = bronze_airport_dataset.drop_duplicates(subset=['IATA_CODE'])
    
    df['AIRPORT'] = df['AIRPORT'].str.strip()
    df['CITY'] = df['CITY'].str.strip()
    df['sys_date'] = datetime.date.today()
    
    pd.to_numeric(df.iloc[:, 5], downcast='float')
    
    pd.to_numeric(df.iloc[:, 6], downcast='float')
    
    df = df.convert_dtypes()
    
    new_cols_name = {df.columns[i]: str(df.columns[i]).lower() for i in range(len(df.columns))}
    
    df.rename(new_cols_name, inplace=True, axis=1)
    
    return Output(
        df,
        metadata={
            'table': 'dim_airport',
            'records': len(df)
        }
    )



@asset(
    ins = {
        'bronze_airline_dataset': AssetIn(
            key_prefix=['bronze', 'carriers']
        )
    },
    io_manager_key='minio_io_manager',
    key_prefix=[LAYER, SCHEMA],
    group_name=GROUP_NAME,
    compute_kind=COMPUTE_KIND,
    description='Airline Dimension table'
)
def dim_airline(context, bronze_airline_dataset):
    
    df = bronze_airline_dataset.drop_duplicates(subset=['IATA_CODE'])
    df['AIRLINE'] = df['AIRLINE'].str.strip()
    df['sys_date'] = datetime.date.today()
    
    df = df.convert_dtypes()
    
    new_cols_name = {df.columns[i]: str(df.columns[i]).lower() for i in range(len(df.columns))}
    df.rename(new_cols_name, inplace=True, axis=1)
    
    return Output(
        df,
        metadata={
            'table': 'dim_airline',
            'records': len(df)
        }
    )


@asset(
    ins = {
        'bronze_cancelcode_dataset': AssetIn(
            key_prefix=['bronze', 'carriers']
        )
    },
    io_manager_key='minio_io_manager',
    key_prefix=[LAYER, SCHEMA],
    group_name=GROUP_NAME,
    compute_kind=COMPUTE_KIND,
    description='Cancellation codes Dimension table'
)
def dim_cancellationcode(context, bronze_cancelcode_dataset):
    
    df = bronze_cancelcode_dataset.drop_duplicates(subset=['CANCELLATION_REASON'])
    df['CANCELLATION_DESCRIPTION'] = df['CANCELLATION_DESCRIPTION'].str.strip()
    df['sys_date'] = datetime.date.today()
    
    df = df.convert_dtypes()
    
    df.rename(columns=lambda x: str(x).lower(), inplace=True)
    
    return Output(
        df,
        metadata={
            'table': 'dim_cancellationcode',
            'records': len(df)
        }
    )
    


@asset(
    ins = {
        'bronze_flight_dataset': AssetIn(
            key_prefix=['bronze', 'carriers']
        )
    },
    io_manager_key='minio_io_manager',
    key_prefix=[LAYER, SCHEMA],
    group_name=GROUP_NAME,
    compute_kind=COMPUTE_KIND,
    description='Flight fact table'
)
def fact_flight(context, bronze_flight_dataset):  
    df = bronze_flight_dataset.drop_duplicates(subset=['YEAR', 'MONTH', 'DAY', 'AIRLINE', 'FLIGHT_NUMBER', 
                                                       'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'])
      
    df['SCHEDULED_DEPARTURE'] = df['SCHEDULED_DEPARTURE'].astype(str).apply(lambda x: x[:2]+':'+x[2:] if x != 'None' else None)
    df['DEPARTURE_TIME'] = df['DEPARTURE_TIME'].astype(str).apply(lambda x: x[:2]+':'+x[2:] if x != 'None' else None)
    df['WHEELS_OFF'] = df['WHEELS_OFF'].astype(str).apply(lambda x: x[:2]+':'+x[2:] if x != 'None' else None )
    df['WHEELS_ON'] = df['WHEELS_ON'].astype(str).apply(lambda x: x[:2]+':'+x[2:]if x != 'None' else None )
    df['SCHEDULED_ARRIVAL'] = df['SCHEDULED_ARRIVAL'].astype(str).apply(lambda x: x[:2]+':'+x[2:] if x != 'None' else None )
    df['ARRIVAL_TIME'] = df['ARRIVAL_TIME'].astype(str).apply(lambda x: x[:2]+':'+x[2:] if x != 'None' else None )
    
    df['FLIGHT_DATE'] = df['YEAR'].astype(str) + '-' + df['MONTH'].astype(str) + '-' + df['DAY'].astype(str)
    
    df['FLIGHT_DATE'] = df['FLIGHT_DATE'].astype('datetime64[ns]')
    
    df.drop(columns=['DEPARTURE_DELAY', 'AIR_SYSTEM_DELAY', 'SECURITY_DELAY', 'AIRLINE_DELAY', 'LATE_AIRCRAFT_DELAY', 
                     'WEATHER_DELAY', 'YEAR', 'MONTH', 'DAY'], inplace=True)

    df['sys_date'] = datetime.date.today()
    
    df['SCHEDULED_DEPARTURE'] = df['SCHEDULED_DEPARTURE'].astype('datetime64[ns]')               
    df['DEPARTURE_TIME'] = df['DEPARTURE_TIME'].str.replace('24:', '00:').astype('datetime64[ns]')
    df['WHEELS_OFF'] = df['WHEELS_OFF'].str.replace('24:', '00:').astype('datetime64[ns]')
    df['WHEELS_ON'] = df['WHEELS_ON'].str.replace('24:', '00:').astype('datetime64[ns]')
    df['SCHEDULED_ARRIVAL'] = df['SCHEDULED_ARRIVAL'].str.replace('24:', '00:').astype('datetime64[ns]')
    df['ARRIVAL_TIME'] = df['ARRIVAL_TIME'].str.replace('24:', '00:').astype('datetime64[ns]')
    
    df.rename(columns=lambda x: str(x).lower(), inplace=True)
    
    return Output(
        df,
        metadata={
            'table': 'fact_flight',
            'records': len(df)
        }
    )


