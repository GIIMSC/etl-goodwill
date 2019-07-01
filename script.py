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

HEADER_MAPPINGS = {
    "Organization Name": "ProgramProvider",
    "Organization URL": "ProviderUrl",
    "Address": "ProviderAddress",
    "Program Name": "ProgramName",
    "Program Category": "ProgramCategory", 
    "Population(s) Targeted": "PopulationTargeted",
    "Goal/Outcome": "Goal",
    "Time Investment": "TimeInvestment",
    "Mission Impact Program ID (used to link to mission impact outcomes - not shown externally in Pathways)": "MissionImpactProgramId",
    "Program status (open, discontinued, deferred, etc.)": "ProgramStatus",
    "CIP Code": "CIP",
    "Program Location (if different from organization address)": "ProgramAddress",
    "URL of Program": "ProgramUrl",
    "Contact phone number for program": "ContactPhone",
    "Program description": "ProgramDescription",
    "Should this program be available in Google Pathways": "PathwaysEnabled",
    "Cost of program, in dollars": "ProgramFees",
    "Time to complete in weeks or semesters or years": "ProgramLength",
    "Total Units": "TotalUnits",
    "Unit Cost (not required if total cost is given)": "UnitCost",
    "How the program is offered": "Format",
    "When the program is offered": "Timing",
    "Books, materials, supplies cost": "CostOfBooksAndSupplies",
    "Start date(s)": "StartDate",
    "End date(s)": "EndDate",
    "Does the program end with a degree, certification, or certificate?": "CredentialLevelEarned",
    "Accreditation body name": "AccreditationBodyName",
    "What certification (exam), license, or certificate (if any) does this program prepare you for or give you?": "CredentialEarned",
    "What occupations/jobs does the training prepare you for?": "RelatedOccupations",
    "Is apprenticeship or paid training available? Incentives (such as covering transportation costs or a small stipend) do not count as paid": "IsPaid",
    "Average hourly wage paid to student, if program is paid": "AverageWagePaid",
    "Incentives (i.e. transportation, child support, stipend)": "Incentives",
    "Salary post-graduation": "PostGradSalary",
    "Eligibile groups, (i.e. Veteran, Senior, Youth). If null, all groups eligible.": "EligibleGroups",
    "Maximum yearly household income to be eligible": "MaxIncomeEligibility",
    "HS diploma required?": "IsDiplomaRequired",
    "Other prerequisites (please list)": "Prerequisites",
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

def get_sheet_as_list_of_dicts(sheets_service, spreadsheet_id, range, has_header_row=True):
    '''
    This function, at first, retrieves the Google sheet as a list of lists.
    It then transforms the sheet data into something more amenable: a list of dictionaries, wherein the
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

    return dataframe_obj
    # sheet_as_list_of_dicts = dataframe_obj.to_dict('records')

    # return sheet_as_list_of_dicts[1:]

def convert_headers(dataframe_obj):
    return dataframe_obj.rename(index=str, columns=HEADER_MAPPINGS)


service = build_googleapi_resource(GOOGLE_DRIVE_CREDENTIALS)

sheet = get_sheet_as_list_of_dicts(service, SPREADSHEET_ID, "1:100000")

df = convert_headers(sheet)
df.drop(df.index[0], inplace=True)

print(df)


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

# con.execute('TRUNCATE my_table RESTART IDENTITY;')
# https://stackoverflow.com/questions/41740985/how-do-i-insert-a-pandas-dataframe-to-an-existing-postgresql-table
# df.to_sql('programs', con=engine, if_exists='replace')


connection = engine.connect()
query = '''
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = 'programs';
'''
results = connection.execute(query)
table_columns = [desc[0] for desc in results.fetchall()]

df1 = df[df.columns.intersection(table_columns)]

df1.to_sql('programs', con=engine, if_exists='append', index=False)





