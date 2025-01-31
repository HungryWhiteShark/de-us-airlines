from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine




@contextmanager
def connect_mysql(config):
    host = 'localhost'
    conn_info = (f"mysql+pymysql://{config['user']}:{config['password']}"+ f"@{host}:{config['port']}"+ f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)

    try:
        yield db_conn
    except Exception:
        raise


class MySQLIOManager(IOManager):
    
    def __init__(self, config):
        self._config = config
        
        
    def handle_output(self, context: OutputContext, obj: pd.DataFrame, table_name):
            
        with connect_mysql(self._config) as conn:
            with conn.connect() as cursor:               
                obj.to_sql(
                    name=table_name,
                    con=conn, if_exists='replace', index=False, chunksize=10000
                )

    
    def load_input(self, context: InputContext, file_path, dtype=None) -> pd.DataFrame:
        df = pd.read_csv(file_path, header=0, engine='c', dtype=dtype, na_values=['None'])
        return df
        
        
    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            return pd_data



