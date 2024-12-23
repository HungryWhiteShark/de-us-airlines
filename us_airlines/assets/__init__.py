from dagster import load_assets_from_modules

from . import insert_data, bronze_layer, silver_layer, gold_layer


all_assets = load_assets_from_modules([insert_data, bronze_layer, silver_layer, gold_layer])



