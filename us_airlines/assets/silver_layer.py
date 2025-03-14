from dagster import asset, Output, AssetIn, MonthlyPartitionsDefinition, IdentityPartitionMapping
import datetime
import pandas as pd
import numpy as np
import json


LAYER = 'silver'
GROUP_NAME = 'silver'
SCHEMA = 'carriers'
COMPUTE_KIND = 'MinIO'

airport_names = 'airline_dataset/alter_airport_name.json'



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
    df.rename(columns=lambda x: str(x).lower(), inplace=True)
    
    # new_cols_name = {df.columns[i]: str(df.columns[i]).lower() for i in range(len(df.columns))}
    # df.rename(new_cols_name, inplace=True, axis=1)
    
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
    
    df.rename(columns=lambda x: str(x).lower(), inplace=True)
    
    # new_cols_name = {df.columns[i]: str(df.columns[i]).lower() for i in range(len(df.columns))}
    # df.rename(new_cols_name, inplace=True, axis=1)
    
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
            key_prefix=['bronze', 'carriers'],
            partition_mapping=IdentityPartitionMapping()
        )
    },
    partitions_def=MonthlyPartitionsDefinition(start_date='2015-1-1', end_date='2016-1-1'),
    io_manager_key='minio_io_manager',
    key_prefix=[LAYER, SCHEMA],
    group_name=GROUP_NAME,
    compute_kind=COMPUTE_KIND,
    description='Flight fact table'
)
def fact_flight(context, bronze_flight_dataset) -> Output[pd.DataFrame]:
    month = context.asset_partition_key_for_output()

    month = str(month).split('-')[1]
    df = bronze_flight_dataset.loc[bronze_flight_dataset['MONTH'] == int(month)]
    
    df = df.drop_duplicates(subset=['YEAR', 'MONTH', 'DAY', 'AIRLINE', 'FLIGHT_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'])
      
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



@asset(
    ins = {
        'bronze_origin_airport_code': AssetIn(
            key_prefix=['bronze', 'carriers']
        ),
        'fact_flight': AssetIn(
            key_prefix=['silver', 'carriers']
        )
    },
    io_manager_key='minio_io_manager',
    key_prefix=[LAYER, SCHEMA],
    group_name=GROUP_NAME,
    compute_kind=COMPUTE_KIND,
    description='Flight fact table'
)
def dim_origin_airport_code(context, bronze_origin_airport_code, fact_flight) -> Output[pd.DataFrame]:
    bronze_origin_airport_code = bronze_origin_airport_code.astype({'Code':'int32'})
    
    df_origin = fact_flight['origin_airport']
    df_destination = fact_flight['destination_airport']

    df_origin.rename('airport', inplace=True)
    df_destination.rename('airport', inplace=True)
    
    
    df = pd.concat([df_origin, df_destination]).drop_duplicates()
    
    print(df.head())
    
    df = df[df.str[0] == '1']
    df = df.astype({'airport': 'int32'})
    
    df_merge = df.to_frame().merge(bronze_origin_airport_code, how='left', left_on='airport', right_on='Code')
    
    df_merge['slice'] = df_merge['Description'].str.find(':')

    df_merge['airport_name'] = np.where(df_merge['slice'] == -1, df_merge['Description'], 
                                        df_merge.apply(lambda x: x['Description'][x['slice'] + 2:], axis=1))


    df_merge['slice'] = df_merge['Description'].str.find(',')

    df_merge = df_merge.drop(labels=['slice', 'Description', 'Code'], axis=1)
    df_merge['airport_name'] = df_merge['airport_name'].str.replace('.', '-')
    df_merge['airport_name'] = df_merge['airport_name'].str.replace('/', '-')            
    df_merge['airport_name'] = df_merge['airport_name'].str.replace('–', '-')
    df_merge['airport_name'] = df_merge['airport_name'].str.replace(' ', '-')
    df_merge['airport_name'] = df_merge['airport_name'].str.replace('---', '-')
    df_merge['airport_name'] = df_merge['airport_name'].str.replace('--', '-')
    
    
    with open(airport_names, 'r') as file:
        new_name = json.load(file)
    
    
    df_merge['new_airport_name'] = df_merge['airport_name'].map(new_name)
    df_merge['new_airport_name'] = df_merge['new_airport_name'].fillna(df_merge['airport_name'])

    df_merge = df_merge.drop(labels=['airport_name'], axis=1)

    return Output(
        df_merge,metadata={
            'table': 'dim_origin_airport_code',
            'records': len(df_merge)
        }
    )




@asset(
    ins = {
        'bronze_name_airport_dataset': AssetIn(
            key_prefix=['bronze', 'carriers']
        ),
        'dim_origin_airport_code': AssetIn(
            key_prefix=['silver', 'carriers']
        )
    },
    io_manager_key='minio_io_manager',
    key_prefix=[LAYER, SCHEMA],
    group_name=GROUP_NAME,
    compute_kind=COMPUTE_KIND,
    description=''
)
def dim_origin_airport_name(context, bronze_name_airport_dataset, dim_origin_airport_code) -> Output[pd.DataFrame]:
    df = bronze_name_airport_dataset[['City served', 'IATA', 'Airport name']].drop_duplicates()
    df.dropna(subset=['City served'], ignore_index=True, inplace=True)
    
    df['IATA'] = df['IATA'].replace('', np.nan)
    df.dropna(subset='IATA', inplace=True)
    
    df['slice_index'] = df['Airport name'].str.find(' (')

    df['airport_name'] = np.where(df['slice_index'] == -1, df['Airport name'], df.apply(lambda x: x['Airport name'][0: x['slice_index']], axis=1))

    df['slice_index'] = df['airport_name'].str.find(' [')
    df['airport_name'] = np.where(df['slice_index'] == -1, df['airport_name'], df.apply(lambda x: x['airport_name'][0: x['slice_index']], axis=1))

    df['desc_reduce'] = df['airport_name'].str.find(' Airport')

    df['airport_name'] = np.where(df['desc_reduce'] == -1, df['airport_name'], df.apply(lambda x: x['airport_name'][0: x['desc_reduce']], axis=1))


    df = df.drop(labels=['Airport name', 'slice_index', 'desc_reduce'], axis=1)

    df['airport_name'] = df['airport_name'].str.replace('.', '-')
    df['airport_name'] = df['airport_name'].str.replace('/', '-')
    df['airport_name'] = df['airport_name'].str.replace('–', '-')
    df['airport_name'] = df['airport_name'].str.replace(' ', '-')
    df['airport_name'] = df['airport_name'].str.replace('---', '-')
    df['airport_name'] = df['airport_name'].str.replace('--', '-')
    
    df = dim_origin_airport_code.merge(df, how='left', left_on=['new_airport_name'], right_on=['airport_name'])
    
    df = df.drop(labels=['airport_name', 'City served', 'new_airport_name'], axis=1)

    return Output(
        df, metadata={
            'table': 'dim_origin_airport_name',
            'records': len(df)
        }
    )

