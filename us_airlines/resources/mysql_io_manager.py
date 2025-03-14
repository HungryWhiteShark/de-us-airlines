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
        self.chunksize = 10000
        
        
    def handle_output(self, context: OutputContext, obj, table_name):
        with connect_mysql(self._config) as conn:
            with conn.connect() as cursor:               
                obj.to_sql(name=table_name, if_exists='append', con=conn, index=False)

    
    def load_input(self, context: InputContext, file_path, dtype=None):
        chunk = pd.read_csv(file_path, header=0, engine='c', dtype=dtype, na_values=['None'], chunksize=self.chunksize)
        for c in chunk:
            yield c
        
        
    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            return pd_data

