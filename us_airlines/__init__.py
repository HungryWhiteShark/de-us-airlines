
from dagster import Definitions, load_assets_from_modules
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources import dbt_resource
from dotenv import load_dotenv

load_dotenv()


from .assets import insert_data, bronze_layer, silver_layer, gold_layer, dbt
import os


all_assets = load_assets_from_modules(modules=[insert_data, bronze_layer, silver_layer, gold_layer])
dbt_assets = load_assets_from_modules(modules=[dbt])



MYSQL_CONFIG = {
    'host': 'localhost',
    'port': int(os.getenv('MYSQL_PORT')),
    'database': os.getenv('MYSQL_DATABASE'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD')
}


MINIO_CONFIG = {
    'endpoint_url': 'localhost',
    'port': '9000',
    'bucket': os.getenv('BUCKET'),
    'minio_access_key': os.getenv('MINIO_ACCESS_KEY'),
    'minio_secret_key': os.getenv('MINIO_SECRET_KEY')
}



PSQL_CONFIG= {
    'host': 'localhost',
    'port': '5432',
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'database': os.getenv('POSTGRES_DB')
}



defs = Definitions(
    assets=[*all_assets, *dbt_assets],
    resources={
        'mysql_io_manager': MySQLIOManager(MYSQL_CONFIG),
        'minio_io_manager': MinIOIOManager(MINIO_CONFIG),
        'psql_io_manager': PostgreSQLIOManager(PSQL_CONFIG),
        'dbt': dbt_resource
    }
)
