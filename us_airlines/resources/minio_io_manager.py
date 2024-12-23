import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import IOManager, OutputContext, InputContext
from minio import Minio



@contextmanager
def connect_minio(config):
    client = Minio(endpoint=config.get('endpoint_url') + ':' + config.get('port'),
                   access_key=config.get('minio_access_key'),
                   secret_key=config.get('minio_secret_key'), secure=False)
    
    try:
        yield client
        
    except Exception:
        raise
    
    
    
class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        
        
    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path

        key = f'{layer}/{schema}/{table}'
        
        tmp_file_path = f'/tmp/{layer}/{schema}/'
        
        os.makedirs(tmp_file_path, exist_ok=True)
        
        tmp_file_path = f"{tmp_file_path}{table}.parquet"
        
        
        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            partition_str = start.strftime('%Y-%m-%d')
            return os.path.join(key, f'{partition_str}.parquet'), tmp_file_path
        
        else:
            return f'{key}.parquet', tmp_file_path
    


    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        
        table = pa.Table.from_pandas(obj)
        
        pq.write_table(table, tmp_file_path)


        try:
            bucket_name = self._config.get('bucket')
            
            with connect_minio(self._config) as client:
                bucket_found = client.bucket_exists(bucket_name)
                if not bucket_found:
                    client.make_bucket(bucket_name)
                    
                client.fput_object(
                    bucket_name, key_name, tmp_file_path, content_type='application/vnd.apache.parquet'            
                )
            
            os.remove(tmp_file_path)
            
        except Exception:
            raise
        
        

    def load_input(self, context: InputContext) -> pd.DataFrame:
        bucket_name = self._config.get('bucket')
        key_name, tmp_file_path = self._get_path(context)
        
        try:
            with connect_minio(self._config) as client:
                client.fget_object(bucket_name, key_name, tmp_file_path)
                                                                                        
            pd_data = pd.read_parquet(tmp_file_path)               
            return pd_data
        
        except Exception:
            raise


