
from dagster import Definitions, load_assets_from_modules, EnvVar
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources import dbt_resource

from .assets import insert_data, bronze_layer, silver_layer, gold_layer, dbt


all_assets = load_assets_from_modules(modules=[insert_data, bronze_layer, silver_layer, gold_layer])
dbt_assets = load_assets_from_modules(modules=[dbt])



MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'database': EnvVar('MYSQL_DATABASE').get_value(),
    'user': EnvVar('MYSQL_USER').get_value(),
    'password': EnvVar('MYSQL_PASSWORD').get_value()
}


MINIO_CONFIG = {
    'endpoint_url': 'localhost',
    'port': '9000',
    'bucket': EnvVar('BUCKET').get_value(),
    'minio_access_key': EnvVar('MINIO_ACCESS_KEY').get_value(),
    'minio_secret_key': EnvVar('MINIO_SECRET_KEY').get_value()
}



PSQL_CONFIG= {
    'host': 'localhost',
    'port': 5432,
    'user': EnvVar('POSTGRES_USER').get_value(),
    'password': EnvVar('POSTGRES_PASSWORD').get_value(),
    'database': EnvVar('POSTGRES_DB').get_value()
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
