from contextlib import contextmanager
from datetime import datetime

from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

import pandas as pd
import sqlalchemy as sqlal



@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise
    
    
    
class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        
    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass    
    
    

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        
        table = table.replace('_us_airlines', '')
        
        tmp_table = f'{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}'
        
        
        with connect_psql(self._config) as conn:
            primary_keys =  (context.metadata or {}).get('primary_keys', [])
            ls_columns = (context.metadata or {}).get('columns', [])
            
            with conn.connect() as cursor:
                cursor.execute(
                    sqlal.text(f'CREATE TEMP TABLE IF NOT EXISTS {tmp_table} (LIKE {schema}.{table})')
                    
                )
                obj[ls_columns].to_sql(
                    name=tmp_table, con=conn, schema=schema, if_exists='replace', index=False, chunksize=10000, method='multi'
                )    
            
            with conn.connect() as cursor:
                result = cursor.execute(
                    sqlal.text(f'SELECT COUNT(*) FROM {schema}.{tmp_table};')
                )
                
                
                for row in result:
                    print(f'Temp table records: {row}')
                    cols = ','.join(x for x in ls_columns)
                    if len(primary_keys) > 0:
                        
                        conditions = ' AND '.join([
                            f""" {schema}.{table}."{key}" = {tmp_table}."{key}" """ for key in primary_keys
                        ])
                        
                    
                        command = f"""
                            BEGIN TRANSACTION; 
                            DELETE FROM {schema}.{table} USING {schema}.{tmp_table} WHERE {conditions};
                            INSERT INTO {schema}.{table}  SELECT * FROM {schema}.{tmp_table};
                            END TRANSACTION;
                            """
                    else:
                        command = f"""
                            BEGIN TRANSACTION; 
                            TRUNCATE TABLE {schema}.{table} ;
                            INSERT INTO {schema}.{table} SELECT {cols} FROM {schema}.{tmp_table};
                            END TRANSACTION;
                            """
                    
                    cursor.execute(sqlal.text(command))
                    cursor.execute(sqlal.text(f'DROP TABLE IF EXISTS {schema}.{tmp_table};'))
    
    
            


