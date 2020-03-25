from dagster import ModeDefinition, PresetDefinition, pipeline
from dagster.utils import file_relative_path

from .resources import googleapi_resource, spreadsheet_resource
from .solids import (add_two, extractor, get_sheet_as_list, print_hello,
                     return_google_sheet_id, send_number, transformer,
                     create_spreadsheet_id, return_all_sheets)


# Define etl pipelines composed of solids
@pipeline
def hello_pipeline():
    print_hello()


@pipeline
def adder_pipeline():
    num = send_number()
    add_two(num)


@pipeline
def simple_extract_pipeline():
    # The way that we pass in values can be via another solid
    transformer(extractor())


local_mode = ModeDefinition(
    name="local",
    resource_defs={"googleapi_resource": googleapi_resource}
)

@pipeline(
    mode_defs=[local_mode],
    preset_defs=[PresetDefinition.from_files(
        name="local",
        mode="local",
        environment_files=[
            file_relative_path(__file__, 'environments/extract_local.yaml')
        ]
    )]
)
def extract_pipeline():
    transformer(get_sheet_as_list(return_google_sheet_id()))


@pipeline(
    mode_defs=[local_mode],
    preset_defs=[PresetDefinition.from_files(
        name="local",
        mode="local",
        environment_files=[
            file_relative_path(__file__, 'environments/extract_local.yaml')
        ]
    )]
)
def extract_all_sheets():
    SPREADSHEET_IDS = {
        "Goodwill_Southern_Arizona": "xxxxxxxxx"
    }
    for key, item in SPREADSHEET_IDS.items():
        spread_sheet_name_solid = create_spreadsheet_id(key, item)
        transformer(get_sheet_as_list(spread_sheet_name_solid()))



all_files_local_mode = ModeDefinition(
    name="all_files_local",
    resource_defs={
        "googleapi_resource": googleapi_resource, 
        "spreadsheet_resource": spreadsheet_resource
    }
)

@pipeline(
    mode_defs=[all_files_local_mode],
    preset_defs=[PresetDefinition.from_files(
        name="all_files_local",
        mode="all_files_local",
        environment_files=[
            file_relative_path(__file__, 'environments/extract_all_files_local.yaml')
        ]
    )]
)
def extract_all_sheets_from_resource():
    return_all_sheets()

pipelines = {
    'hello_pipeline': lambda : hello_pipeline,
    'adder_pipeline': lambda : adder_pipeline,
    'simple_extract_pipeline': lambda : simple_extract_pipeline,
    'extract_pipeline': lambda : extract_pipeline,
    'extract_all_sheets': lambda: extract_all_sheets,
    'extract_all_sheets_from_resource': lambda : extract_all_sheets_from_resource
}
