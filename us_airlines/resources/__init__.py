from ..project import dbt_project
from dagster_dbt import DbtCliResource

dbt_resource = DbtCliResource(
    project_dir=dbt_project
)





