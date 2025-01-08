from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator, build_dbt_asset_selection
from dagster import AssetKey, AssetExecutionContext
from ..project import dbt_project

SCHEMA = 'gold'

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props['resource_type']
        name = dbt_resource_props['name']
        
        if resource_type == 'source':
            return AssetKey(f'{name}_us_airlines').with_prefix(SCHEMA)
        else:
            return super().get_asset_key(dbt_resource_props)
        

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_airline(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(['build'], context=context).stream()

