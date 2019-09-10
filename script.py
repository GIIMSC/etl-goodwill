import pandas as pd
from typing import Dict, List
import numpy as np

from googleapiclient import discovery
from googleapiclient.discovery import Resource
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

from config import GOOGLE_DRIVE_CREDENTIALS, SPREADSHEET_ID 


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/documents.readonly",
]

HEADER_MAPPINGS = {
    "Organization Name": "ProgramProvider",
    "Organization URL": "ProviderUrl",
    "Organization Address": "ProviderAddress",
    "Program Name": "ProgramName",
    "Program Category": "ProgramCategory", 
    "Population(s) Targeted": "PopulationTargeted",
    "Goal/Outcome": "Goal",
    "Time Investment": "TimeInvestment",
    "Mission Impact Program ID": "MissionImpactProgramId",
    "Program Status": "ProgramStatus",
    "CIP Code": "CIP",
    "Program Address (if different from organization address)": "ProgramAddress",
    "URL of Program": "ProgramUrl",
    "Contact phone number for program": "ContactPhone",
    "Program description": "ProgramDescription",
    "Should this program be available in Google Pathways?": "PathwaysEnabled",
    "Cost of program, in dollars": "ProgramFees",
    "Duration / Time to complete": "ProgramLength",
    "Total Units": "TotalUnits",
    "Unit Cost (not required if total cost is given)": "UnitCost",
    "Format": "Format",
    "Timing": "Timing",
    "Books, materials, supplies cost (in dollars)": "CostOfBooksAndSupplies",
    "Start date(s)": "StartDate",
    "End date(s)": "EndDate",
    "Credential level earned": "CredentialLevelEarned",
    "Accreditation body name": "AccreditationBodyName",
    "What certification (exam), license, or certificate (if any) does this program prepare you for or give you?": "CredentialEarned",
    "What occupations/jobs does the training prepare you for?": "RelatedOccupations",
    "Apprenticeship or Paid Training Available": "IsPaid",
    "If yes, average hourly wage paid to student": "AverageWagePaid",
    "Incentives": "Incentives",
    "Salary post-graduation": "PostGradSalary",
    "Eligibile groups": "EligibleGroups",
    "Maximum yearly household income to be eligible": "MaxIncomeEligibility",
    "HS diploma required?": "IsDiplomaRequired",
    "Other prerequisites": "Prerequisites",
    "Anything else to add about the program?": "Miscellaneous",
}

def build_googleapi_resource(account_info: Dict):
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

def get_sheet_as_dataframe(sheets_service, spreadsheet_id, range, has_header_row=True):
    '''
    This function, at first, retrieves the Google sheet as a list of lists.
    It then transforms the sheet data into something more amenable: a Pandas dataframe.

    Note! `sheet_as_list_of_lists` contains a collection of lists 
    with inconsistent lengths, e.g. there are 41 headers, but some rows have 40 or fewer values.
    This occurs because the service does not read empty cells *at the end of rows*.
    '''
    
    # Googleapiclient.discovery.Resource – an object
    # values() – returns a list of all values in a dictionary
    results = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range)
        .execute()
    )

    sheet_as_list_of_lists = results.get("values", [])
    headers = sheet_as_list_of_lists[0]
    dataframe_obj = pd.DataFrame(sheet_as_list_of_lists, columns=headers) 
    
    # Replace all empty strings with NaN: to avoid sqlalchemy "invalid input syntax for integer" Error.
    df = dataframe_obj.replace('', np.nan)

    return df

def convert_headers(dataframe_obj):
    # Note! We can likely remove this step, if the Google sheet/script can be altered to match the schema.
    return dataframe_obj.rename(index=str, columns=HEADER_MAPPINGS)



# EXTRACT
service = build_googleapi_resource(GOOGLE_DRIVE_CREDENTIALS)
sheet = get_sheet_as_dataframe(service, SPREADSHEET_ID, "1:100000")



# TRANSFORM
df = convert_headers(sheet)
df.drop(df.index[0], inplace=True)



# LOAD
# 1. Engine to connect to the database
from sqlalchemy import create_engine

POSTGRES_USER = 'test_user'
POSTGRES_PASSWORD = 'test_password'
# Connect to the psql container, rather than localhost. 
# POSTGRES_HOSTNAME = 'data-resource-api_postgres_1'
POSTGRES_HOSTNAME = 'localhost'
POSTGRES_PORT = '5433' 
POSTGRES_DATABASE = 'data_resource_dev'
SQLALCHEMY_DATABASE_URI = 'postgresql://{}:{}@{}:{}/{}'.format(
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOSTNAME,
    POSTGRES_PORT,
    POSTGRES_DATABASE
)

engine = create_engine(SQLALCHEMY_DATABASE_URI)

# 2. Get all columns
connection = engine.connect()
query = '''
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = 'programs';
'''
results = connection.execute(query)
table_columns = [desc[0] for desc in results.fetchall()]



# Remove dates not current
from datetime import datetime

# TODO: 
# Add Timestamp to Data Resource API Schema
# Query Data Resource API for latest date.
# Replace "now" with that value...

query = '''
    SELECT "Timestamp"
    FROM programs
    ORDER BY "Timestamp"
    LIMIT 1
'''
results = connection.execute(query)

import pdb; pdb.set_trace()
now = datetime.now()
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m/%d/%Y %H:%M:%S')
df = df[df['Timestamp'] >= now]




# df1 = df[df.columns.intersection(table_columns)]

# df1.to_sql('programs', 
#     con=engine, 
#     if_exists='append', 
#     index=False)

# # TODO:
# # 1. Only import recently updated data
# # 2. Adjust the schema to properly reflect the titles, i.e., `title` field in the schema should be the same at the column name in the Google sheet. Do we want to change the column headers to be the same at the table fields?
# # 3. Refactor
# # 4. Tests: write some tests + test with more data...
