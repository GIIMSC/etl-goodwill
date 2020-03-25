# Define repository of pipelines
# It returns a RepositoryDefinition object
from dagster import RepositoryDefinition
from .pipelines import pipelines


def define_goodwill_repo():
    return RepositoryDefinition(
        name='etl_goodwill',
        pipeline_dict=pipelines
    )