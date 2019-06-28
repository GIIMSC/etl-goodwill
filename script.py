import pandas as pd
from typing import Dict, List

from googleapiclient import discovery
from googleapiclient.discovery import Resource
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

from config import GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_ID 


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/documents.readonly",
]


def build_resource(account_info: Dict):
    """
    This function builds a googleapiclient Resource, i.e.,
    an object that can interact with an api, such as Google sheets ("sheets").

    https://github.com/googleapis/google-api-python-client/blob/5c11b0a1b2658b26fe41b13ebd2e9e7b53c1ab01/googleapiclient/discovery.py#L170
    """
    credentials = Credentials.from_service_account_info(account_info, scopes=SCOPES)

    return discovery.build(
        'sheets',
        'v4',
        credentials=credentials,
        cache_discovery=False,
    )

def load_sheet(sheets_service, spreadsheet_id, range, has_header_row=True):
    '''
    This function retrieves the Google sheet as a list of lists.
    It then transforms that into a more amenable data type: a list of dictionaries, wherein the
    headers are the keys.

    Note! `sheet_as_list_of_lists` contains a collection of lists 
    with inconsistent lengths, e.g. there are 41 headers, but some rows have 40 or fewer values.
    This occurs because the service does not read empty cells *at the end of rows*.
    '''
    results = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range)
        .execute()
    )

    sheet_as_list_of_lists = results.get("values", [])

    headers = sheet_as_list_of_lists[0]
    dataframe_obj = pd.DataFrame(sheet_as_list_of_lists, columns=headers) 
    sheet_as_list_of_dicts = dataframe_obj.to_dict('records')

    return sheet_as_list_of_dicts[1:]


service = build_resource(GOOGLE_DRIVE_CREDENTIALS)

sheet = load_sheet(service, SPREADSHEET_ID, "1:100000")



