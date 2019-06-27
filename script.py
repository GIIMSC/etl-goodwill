import pandas as pd
from typing import Dict, List

from googleapiclient import discovery
from googleapiclient.discovery import Resource
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

from config import GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_ID 

SheetInfo = Dict[str, str]

SheetTitle = str
SheetTitles = List[SheetTitle]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/documents.readonly",
]

SHEET_RANGE_SPLIT_CHAR = "!"


def get_google_service(account_info: Dict, api: str, api_version: str) -> Resource:
    """Returns a Google API Resource for a given account authorization, api type, and
    version."""
    credentials: Credentials = Credentials.from_service_account_info(
        account_info, scopes=SCOPES
    )
    return discovery.build(
        api,
        api_version,
        credentials=credentials,
        cache_discovery=False,  # Silence caching warning with Google API client
    )


def get_google_sheets_service(account_info: Dict) -> Resource:
    """Returns a Google Sheets API Resource."""
    return get_google_service(account_info, "sheets", "v4")


def load_sheet_as_dataframe(sheets_service, spreadsheet_id, range, has_header_row=True):
    # Try getting the sheet - if that is not possible, return an empty dataframe
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range)
        .execute()
    )

    values = result.get("values", [])

    if not values:
        return None
    else:
        sheet_df = pd.DataFrame.from_records(values)
        if has_header_row:
            sheet_df.columns = sheet_df.iloc[0].str.strip()
            sheet_df = sheet_df.reindex(sheet_df.index.drop(0))
        return sheet_df


service = get_google_sheets_service(GOOGLE_DRIVE_CREDENTIALS)

raw_df = load_sheet_as_dataframe(service, SPREADSHEET_ID, "1:100000")

print(raw_df)