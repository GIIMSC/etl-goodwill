from dagster import( 
    solid,
    String,
    Int,
    List,
    Field,
    Dict,
    Array,
    InputDefinition,
    OutputDefinition,
    Output,
    composite_solid
)
from pandas import DataFrame

from etl.extractor import Extractor

from config.config import GOOGLE_DRIVE_CREDENTIALS

@solid
def print_hello(context) -> String:
    print("Hello")
    return "Hello"


@solid
def add_two(context, number: Int) -> Int:
    return number + 2


@solid
def send_number(context) -> Int:
    return 5


@solid
def extractor(context) -> List:
    results = Extractor(
        google_account_info=GOOGLE_DRIVE_CREDENTIALS, 
        spreadsheet_id="1ag6AgfK4ki3GuyMnXomCXK80fOR8antHb1ZN38zIjIY"
    ).get_sheet_as_list()

    context.log.info("!!!!!!!!!")
    context.log.info(f"{len(results)}")

    return results

@solid
def transformer(context, extracted_list: List) -> Int:
    context.log.info("!!!!!!!!!")
    context.log.info(f"{len(extracted_list)}")
    return len(extracted_list)


# N.B., Dagster has a Google module, which might be able to use in the future.
@solid(
    input_defs=[InputDefinition("spreadsheet_id", dagster_type=String)], # If you use `input_defs` then you do not need to specify the arg type in the function params
    output_defs=[OutputDefinition(List)],
    required_resource_keys={"googleapi_resource"}
)
def get_sheet_as_list(context, spreadsheet_id):
        '''
        This function returns the Google sheet as a list of lists.

        Note! `sheet_as_list_of_lists` contains a collection of lists 
        with inconsistent lengths, e.g. there are 41 headers, but some rows have 40 or fewer values.
        This occurs because the service does not read empty cells *at the end of rows*.
        '''
        discovery_resource = context.resources.googleapi_resource

        results = (
            discovery_resource.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range="1:1000000")
            .execute()
        )

        return results.get("values", [])

def create_spreadsheet_id(sheet_ref_name: String, spreadsheet_id: String):
    @solid(
        name=f"spreadsheet_solid_{sheet_ref_name}"
    )
    def return_google_sheet_id(context) -> String:
        return spreadsheet_id
    return return_google_sheet_id


def get_spreadsheet_id_solid(sheet_ref_name: String, spreadsheet_id: String):
    @solid(
        name=f"spreadsheet_solid_{sheet_ref_name}"
    )
    def return_google_sheet_id(context) -> String:
        return spreadsheet_id
    return return_google_sheet_id

@solid
def return_google_sheet_id(context) -> String:
    return "xxxxxxxx"



@composite_solid(
    output_defs=[
        OutputDefinition(
            dagster_type=Dict, 
            name="sheet_dict"
        )
    ],
    config={
        'sheets': Field(
            Array(

                {
                    "id": Field(String),
                    "value": Field(String)
                }

            )
        )
    },
    # required_resource_keys={"spreadsheet_resource"}
)
def return_all_sheets(context) -> String:
    spreadsheets = context.solid_config['sheets']
    for sheet in spreadsheets:
        spread_sheet_name_solid = create_spreadsheet_id(sheet["id"], sheet["value"])
        transformer(get_sheet_as_list(spread_sheet_name_solid()))
