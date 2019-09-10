from typing import Dict

from google.oauth2.service_account import Credentials
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError


class Extractor:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/documents.readonly",
    ]

    def __init__(self, google_account_info: Dict, spreadsheet_id):
        self.google_account_info = google_account_info
        self.spreadsheet_id = spreadsheet_id
    
    def _build_googleapi_resource(self):
        """
        This function builds a googleapiclient Resource, i.e.,
        an object that can interact with an api, such as Google sheets ("sheets").

        https://github.com/googleapis/google-api-python-client/blob/5c11b0a1b2658b26fe41b13ebd2e9e7b53c1ab01/googleapiclient/discovery.py#L170
        """
        credentials = Credentials.from_service_account_info(self.google_account_info, scopes=self.scopes)

        return discovery.build(
            'sheets',
            'v4',
            credentials=credentials,
            cache_discovery=False,
        )

    def get_sheet_as_list(self):
        '''
        This function, at first, retrieves the Google sheet as a list of lists.
        It then transforms the sheet data into something more amenable: a Pandas dataframe.

        Note! `sheet_as_list_of_lists` contains a collection of lists 
        with inconsistent lengths, e.g. there are 41 headers, but some rows have 40 or fewer values.
        This occurs because the service does not read empty cells *at the end of rows*.
        '''
        discovery_resource = self._build_googleapi_resource()

        # Googleapiclient.discovery.Resource – an object
        # values() – returns a list of all values in a dictionary
        results = (
            discovery_resource.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range="1:1000000")
            .execute()
        )

        return results.get("values", [])
