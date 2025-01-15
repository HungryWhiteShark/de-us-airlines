from dagster import asset, Output, MonthlyPartitionsDefinition

import pandas as pd
import datetime

@asset(
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'carriers'],
    compute_kind='MySQL',
    group_name='ExtractLayer',
    description='Store airlines dataset as parquet in MinIO'
)
def bronze_airline_dataset(context) -> Output[pd.DataFrame]:
    sql_query = 'SELECT * FROM airlines'
    
    pd_data = context.resources.mysql_io_manager.extract_data(sql_query)
    
    return Output(
        pd_data, metadata={
            'table': 'airlines',
            'records': len(pd_data)
        }
    )



@asset(
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'carriers'],
    compute_kind='MySQL',
    group_name='ExtractLayer',
    description='Store airports dataset as parquet in MinIO'
)
def bronze_airport_dataset(context) -> Output[pd.DataFrame]:
    sql_query = 'SELECT * FROM airports'
    
    pd_data = context.resources.mysql_io_manager.extract_data(sql_query)
    
    return Output(
        pd_data, metadata={
            'table': 'airports',
            'records': len(pd_data)
        }
    )
    
    
    
@asset(
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'carriers'],
    compute_kind='MySQL',
    group_name='ExtractLayer',
    description='Store cancellation codes dataset as parquet in MinIO'
)
def bronze_cancelcode_dataset(context) -> Output[pd.DataFrame]:
    sql_query = 'SELECT * FROM cancellation_codes'
    
    pd_data = context.resources.mysql_io_manager.extract_data(sql_query)
    
    return Output(
        pd_data, metadata={
            'table': 'cancellation_codes',
            'records': len(pd_data)
        }
    )
    


@asset(  
    io_manager_key='minio_io_manager',
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze', 'carriers'],
    compute_kind='MySQL',
    group_name='ExtractLayer',
    description='Store flights dataset as parquet in MinIO',
    partitions_def=MonthlyPartitionsDefinition(start_date='2015-1-1', end_date='2016-1-1')
)
def bronze_flight_dataset(context) -> Output[pd.DataFrame]:
    
    sql_query = f"SELECT * FROM flights "
    
    month = context.asset_partition_key_for_output()

    month = str(month).split('-')[1]
    month = month[1] if month[0] == '0' else month
    
    sql_query += f" WHERE MONTH = '{month}' "
    
    pd_data = context.resources.mysql_io_manager.extract_data(sql_query)
    
    
    return Output(
        pd_data, metadata={
            'table': 'flights',
            'records': len(pd_data)
        }
    )

